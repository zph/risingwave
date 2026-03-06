// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::{SplitId, SplitMetaData};

/// Per-chunk state for parallel snapshot recovery (OPLOG-050).
///
/// Each chunk tracks its `_id` range and resume position. The vector of
/// chunk states is persisted in `MongodbOplogOffset::snapshot_chunks` at
/// each checkpoint so that crash recovery can skip completed chunks and
/// resume incomplete ones from their last processed `_id`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SnapshotChunkState {
    /// Index of this chunk in the boundary list.
    pub chunk_idx: usize,
    /// Lower bound `_id` (BSON ExtJSON). `None` = start of collection.
    pub min_id: Option<String>,
    /// Upper bound `_id` (BSON ExtJSON). `None` = end of collection.
    pub max_id: Option<String>,
    /// Last successfully processed `_id` within this chunk (resume point).
    pub last_id: Option<String>,
    /// Whether this chunk has been fully scanned.
    pub done: bool,
}

/// Persisted offset state for the mongo-oplog source.
///
/// During the snapshot phase, tracks the last `_id` scanned (OPLOG-034).
/// After the snapshot, tracks the oplog timestamp for CDC resumption (OPLOG-014/015).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MongodbOplogOffset {
    /// Whether the initial snapshot scan has completed (OPLOG-033).
    pub snapshot_done: bool,
    /// Last `_id` yielded during snapshot, serialized as BSON Extended JSON string.
    /// Used for crash recovery resumption (OPLOG-034).
    pub snapshot_last_id: Option<String>,
    /// Oplog timestamp seconds component for CDC resumption (OPLOG-014).
    pub oplog_ts_secs: u32,
    /// Oplog timestamp ordinal component for CDC resumption (OPLOG-014).
    pub oplog_ts_ord: u32,
    /// Per-chunk state for parallel snapshot (OPLOG-050).
    /// Present only during chunked snapshot; cleared on phase transition to CDC.
    /// Backward-compatible: old offsets without this field deserialize as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_chunks: Option<Vec<SnapshotChunkState>>,
}

impl MongodbOplogOffset {
    /// Initial state: no snapshot, no offset.
    pub fn empty() -> Self {
        Self {
            snapshot_done: false,
            snapshot_last_id: None,
            oplog_ts_secs: 0,
            oplog_ts_ord: 0,
            snapshot_chunks: None,
        }
    }

    /// Encode to a JSON string for storage in the split offset field.
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).expect("MongodbOplogOffset serialization should not fail")
    }

    /// Decode from a JSON string stored in the split offset field.
    pub fn from_json_str(s: &str) -> ConnectorResult<Self> {
        serde_json::from_str(s).map_err(Into::into)
    }
}

/// Split metadata for the mongo-oplog source.
///
/// Single split per source (one oplog per replica set). The split_id is
/// always "0" for v1 (OPLOG-038).
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct MongodbOplogSplit {
    pub split_id: SplitId,
    /// Direct replica set URI for this split's shard (OPLOG-041).
    /// Populated by auto-discovery; None in single-RS mode.
    pub replica_set_uri: Option<String>,
    /// JSON-encoded `MongodbOplogOffset`, or `None` for a fresh start.
    pub start_offset: Option<String>,
}

impl SplitMetaData for MongodbOplogSplit {
    fn id(&self) -> SplitId {
        self.split_id.clone()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.start_offset = Some(last_seen_offset);
        Ok(())
    }
}

impl MongodbOplogSplit {
    /// Create a new split with no offset (fresh start).
    pub fn new() -> Self {
        Self {
            split_id: "0".into(),
            replica_set_uri: None,
            start_offset: None,
        }
    }

    /// Parse the offset from the stored JSON string.
    pub fn offset(&self) -> ConnectorResult<Option<MongodbOplogOffset>> {
        match &self.start_offset {
            None => Ok(None),
            Some(s) => MongodbOplogOffset::from_json_str(s).map(Some),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_serialization_roundtrip() {
        let offset = MongodbOplogOffset {
            snapshot_done: false,
            snapshot_last_id: Some("{\"$oid\": \"65bc9fb6c485f419a7a877fe\"}".to_owned()),
            oplog_ts_secs: 0,
            oplog_ts_ord: 0,
            snapshot_chunks: None,
        };

        let json = offset.to_json_string();
        let restored = MongodbOplogOffset::from_json_str(&json).unwrap();
        assert_eq!(offset, restored);
    }

    #[test]
    fn test_offset_after_snapshot() {
        let offset = MongodbOplogOffset {
            snapshot_done: true,
            snapshot_last_id: None,
            oplog_ts_secs: 1706968217,
            oplog_ts_ord: 1,
            snapshot_chunks: None,
        };

        let json = offset.to_json_string();
        let restored = MongodbOplogOffset::from_json_str(&json).unwrap();
        assert_eq!(offset, restored);
        assert!(restored.snapshot_done);
        assert_eq!(restored.oplog_ts_secs, 1706968217);
    }

    #[test]
    fn test_split_encode_restore_roundtrip() {
        let split = MongodbOplogSplit {
            split_id: "0".into(),
            replica_set_uri: None,
            start_offset: Some(
                MongodbOplogOffset {
                    snapshot_done: true,
                    snapshot_last_id: None,
                    oplog_ts_secs: 100,
                    oplog_ts_ord: 2,
                    snapshot_chunks: None,
                }
                .to_json_string(),
            ),
        };

        let json = split.encode_to_json();
        let restored = MongodbOplogSplit::restore_from_json(json).unwrap();
        assert_eq!(split, restored);
    }

    #[test]
    fn test_split_new_has_no_offset() {
        let split = MongodbOplogSplit::new();
        assert_eq!(split.split_id.as_ref(), "0");
        assert!(split.start_offset.is_none());
        assert!(split.offset().unwrap().is_none());
    }

    // ── OPLOG-050: SnapshotChunkState serde ─────────────────────────

    #[test]
    fn test_snapshot_chunk_state_serde_roundtrip() {
        let chunk = SnapshotChunkState {
            chunk_idx: 2,
            min_id: Some("{\"$oid\": \"aaa\"}".to_owned()),
            max_id: Some("{\"$oid\": \"bbb\"}".to_owned()),
            last_id: Some("{\"$oid\": \"abc\"}".to_owned()),
            done: false,
        };
        let json = serde_json::to_string(&chunk).unwrap();
        let restored: SnapshotChunkState = serde_json::from_str(&json).unwrap();
        assert_eq!(chunk, restored);
    }

    #[test]
    fn test_offset_with_chunks_serde_roundtrip() {
        let offset = MongodbOplogOffset {
            snapshot_done: false,
            snapshot_last_id: None,
            oplog_ts_secs: 1000,
            oplog_ts_ord: 1,
            snapshot_chunks: Some(vec![
                SnapshotChunkState {
                    chunk_idx: 0,
                    min_id: None,
                    max_id: Some("{\"$oid\": \"mid\"}".to_owned()),
                    last_id: Some("{\"$oid\": \"cur\"}".to_owned()),
                    done: false,
                },
                SnapshotChunkState {
                    chunk_idx: 1,
                    min_id: Some("{\"$oid\": \"mid\"}".to_owned()),
                    max_id: None,
                    last_id: None,
                    done: true,
                },
            ]),
        };
        let json = offset.to_json_string();
        let restored = MongodbOplogOffset::from_json_str(&json).unwrap();
        assert_eq!(offset, restored);
    }

    #[test]
    fn test_offset_backward_compat_no_chunks_field() {
        // Old JSON without snapshot_chunks field should deserialize as None
        let json = r#"{"snapshot_done":false,"snapshot_last_id":null,"oplog_ts_secs":100,"oplog_ts_ord":1}"#;
        let offset = MongodbOplogOffset::from_json_str(json).unwrap();
        assert!(offset.snapshot_chunks.is_none());
        assert_eq!(offset.oplog_ts_secs, 100);
    }

    #[test]
    fn test_offset_chunks_skipped_when_none() {
        // When snapshot_chunks is None, it should not appear in JSON
        let offset = MongodbOplogOffset {
            snapshot_done: true,
            snapshot_last_id: None,
            oplog_ts_secs: 500,
            oplog_ts_ord: 2,
            snapshot_chunks: None,
        };
        let json = offset.to_json_string();
        assert!(
            !json.contains("snapshot_chunks"),
            "None snapshot_chunks should be skipped in JSON: {}",
            json
        );
    }

    #[test]
    fn test_split_update_offset() {
        let mut split = MongodbOplogSplit::new();
        let offset = MongodbOplogOffset {
            snapshot_done: true,
            snapshot_last_id: None,
            oplog_ts_secs: 500,
            oplog_ts_ord: 3,
            snapshot_chunks: None,
        };
        split.update_offset(offset.to_json_string()).unwrap();

        let parsed = split.offset().unwrap().unwrap();
        assert_eq!(parsed.oplog_ts_secs, 500);
        assert_eq!(parsed.oplog_ts_ord, 3);
    }
}
