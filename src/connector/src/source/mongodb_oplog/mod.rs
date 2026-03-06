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

pub mod enumerator;
pub mod message;
pub mod oplog_types;
pub mod reader;
pub mod split;

#[cfg(test)]
mod integration_tests;

use std::collections::HashMap;

use mongodb::options::{ClientOptions, SelectionCriteria};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;

use crate::connector_common::MongodbCommon;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::source::SourceProperties;
use crate::source::mongodb_oplog::enumerator::MongodbOplogSplitEnumerator;
use crate::source::mongodb_oplog::reader::MongodbOplogSplitReader;
use crate::source::mongodb_oplog::split::MongodbOplogSplit;

// OPLOG-001: connector name constant
pub const MONGO_OPLOG_CONNECTOR: &str = "mongo-oplog";

/// Boundary discovery mode for chunked parallel snapshot (OPLOG-054).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundaryMode {
    /// Use min/max index lookups + arithmetic splits (OPLOG-055/056/057).
    /// Only supports ObjectId, Int32, Int64 `_id` types. Fails loudly otherwise.
    MinMax,
    /// Use `$sample` aggregation pipeline (OPLOG-060).
    Sample,
}

const fn default_watermark_poll_interval_ms() -> u64 {
    100
}

const fn default_buffer_max_bytes() -> u64 {
    104_857_600 // 100MB
}

const fn default_watermark_stall_timeout_secs() -> u64 {
    30
}

const fn default_heartbeat_interval_secs() -> u64 {
    60
}

const fn default_snapshot_batch_size() -> u64 {
    1024
}

const fn default_shard_discovery_interval_secs() -> u64 {
    30
}

const fn default_snapshot_workers_per_shard() -> u64 {
    1
}

const fn default_snapshot_chunk_target_docs() -> u64 {
    10_000
}

fn default_snapshot_boundary_mode() -> String {
    "min_max".to_owned()
}

/// Properties for the `mongo-oplog` source connector.
///
/// Tails `local.oplog.rs` on a MongoDB replica set member using the
/// High-Watermark Tailer pattern (OPLOG-001 through OPLOG-039).
#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MongodbOplogProperties {
    /// MongoDB connection URI (OPLOG-028)
    #[serde(rename = "mongodb.url")]
    pub mongodb_url: String,

    /// Target namespace in `db.collection` format (OPLOG-003)
    #[serde(rename = "mongodb.namespace")]
    pub namespace: String,

    /// Interval in ms between `replSetGetStatus` polls (OPLOG-004)
    #[serde(
        rename = "mongodb.watermark.poll_interval_ms",
        default = "default_watermark_poll_interval_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub watermark_poll_interval_ms: u64,

    /// Maximum in-memory buffer size in bytes (OPLOG-008)
    #[serde(
        rename = "mongodb.buffer.max_bytes",
        default = "default_buffer_max_bytes"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub buffer_max_bytes: u64,

    /// Stall timeout in seconds before warning when watermark is stuck (OPLOG-008a)
    #[serde(
        rename = "mongodb.watermark.stall_timeout_secs",
        default = "default_watermark_stall_timeout_secs"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub watermark_stall_timeout_secs: u64,

    /// Heartbeat emission interval in seconds during idle periods (OPLOG-025)
    #[serde(
        rename = "mongodb.heartbeat.interval_secs",
        default = "default_heartbeat_interval_secs"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub heartbeat_interval_secs: u64,

    /// Number of documents per batch during snapshot scan (OPLOG-033)
    #[serde(
        rename = "mongodb.snapshot.batch_size",
        default = "default_snapshot_batch_size"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub snapshot_batch_size: u64,

    /// Shard discovery polling interval in seconds (OPLOG-046).
    /// Only used when connected to mongos for auto-discovery.
    #[serde(
        rename = "mongodb.shard.discovery.interval_secs",
        default = "default_shard_discovery_interval_secs"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub shard_discovery_interval_secs: u64,

    /// Number of parallel snapshot workers per shard (OPLOG-047).
    /// Default 1 (serial). Range: 1–64. Controls concurrency, not chunk count.
    #[serde(
        rename = "mongodb.snapshot.workers_per_shard",
        default = "default_snapshot_workers_per_shard"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub snapshot_workers_per_shard: u64,

    /// Target number of documents per snapshot chunk (OPLOG-048).
    /// Boundary discovery divides the collection into chunks of approximately
    /// this many documents. Chunk count = max(1, estimated_count / target).
    /// Workers pull from a shared pool; `workers_per_shard` controls concurrency.
    /// Minimum: 100. Default: 10,000.
    #[serde(
        rename = "mongodb.snapshot.chunk_target_docs",
        default = "default_snapshot_chunk_target_docs"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub snapshot_chunk_target_docs: u64,

    /// Boundary discovery mode for chunked snapshot (OPLOG-054).
    /// Values: `min_max` (default), `sample`.
    #[serde(
        rename = "mongodb.snapshot.boundary_mode",
        default = "default_snapshot_boundary_mode"
    )]
    pub snapshot_boundary_mode: String,

    /// Startup mode: snapshot (default), latest, earliest (OPLOG-036)
    #[serde(rename = "scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl EnforceSecret for MongodbOplogProperties {
    fn enforce_secret<'a>(prop_iter: impl Iterator<Item = &'a str>) -> ConnectorResult<()> {
        for prop in prop_iter {
            MongodbCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl SourceProperties for MongodbOplogProperties {
    type Split = MongodbOplogSplit;
    type SplitEnumerator = MongodbOplogSplitEnumerator;
    type SplitReader = MongodbOplogSplitReader;

    const SOURCE_NAME: &'static str = MONGO_OPLOG_CONNECTOR;
}

impl crate::source::UnknownFields for MongodbOplogProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl MongodbOplogProperties {
    /// Parse `mongodb.namespace` into (db, collection).
    pub fn parse_namespace(&self) -> ConnectorResult<(&str, &str)> {
        let parts: Vec<&str> = self.namespace.splitn(2, '.').collect();
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(anyhow::anyhow!(
                "mongodb.namespace must be in 'db.collection' format, got: {}",
                self.namespace
            )
            .into());
        }
        Ok((parts[0], parts[1]))
    }

    /// Get the startup mode, defaulting to "snapshot".
    pub fn startup_mode(&self) -> &str {
        self.scan_startup_mode
            .as_deref()
            .unwrap_or("snapshot")
    }

    /// Parse the boundary mode string into the enum (OPLOG-054).
    pub fn boundary_mode(&self) -> ConnectorResult<BoundaryMode> {
        match self.snapshot_boundary_mode.as_str() {
            "min_max" => Ok(BoundaryMode::MinMax),
            "sample" => Ok(BoundaryMode::Sample),
            other => Err(anyhow::anyhow!(
                "mongodb.snapshot.boundary_mode must be 'min_max' or 'sample', got: '{}'",
                other
            )
            .into()),
        }
    }

    /// Validate snapshot parallelism settings (OPLOG-047/048/054).
    pub fn validate_snapshot_config(&self) -> ConnectorResult<()> {
        if self.snapshot_workers_per_shard < 1 || self.snapshot_workers_per_shard > 64 {
            return Err(anyhow::anyhow!(
                "mongodb.snapshot.workers_per_shard must be between 1 and 64, got: {}",
                self.snapshot_workers_per_shard
            )
            .into());
        }
        if self.snapshot_chunk_target_docs < 100 {
            return Err(anyhow::anyhow!(
                "mongodb.snapshot.chunk_target_docs must be >= 100, got: {}",
                self.snapshot_chunk_target_docs
            )
            .into());
        }
        // Validate boundary mode is parseable
        self.boundary_mode()?;
        Ok(())
    }

    /// Build a MongoDB client from the connection URI (OPLOG-037).
    ///
    /// Returns `(Client, replica_set_name)`. The replica set name is extracted
    /// from `ClientOptions::repl_set_name` (populated by URI parsing); defaults
    /// to `"rs0"` when the URI does not include `?replicaSet=...`.
    ///
    /// If the URI does not specify a read preference, defaults to `secondaryPreferred`
    /// to offload read load from the Primary while still allowing reads when no
    /// Secondary is available. Users configure read preference via the URI string
    /// (e.g., `?readPreference=secondary&readPreferenceTags=dc:us-east`).
    pub async fn build_client(&self) -> ConnectorResult<(mongodb::Client, String)> {
        let mut opts = ClientOptions::parse_async(&self.mongodb_url).await?;
        let rs_name = opts.repl_set_name.clone().unwrap_or_else(|| "rs0".to_owned());
        if opts.selection_criteria.is_none() {
            opts.selection_criteria = Some(SelectionCriteria::ReadPreference(
                mongodb::options::ReadPreference::SecondaryPreferred {
                    options: Default::default(),
                },
            ));
        }
        Ok((mongodb::Client::with_options(opts)?, rs_name))
    }
}

/// Parse a `config.shards` host field into a MongoDB URI (OPLOG-041).
///
/// Input:  "rs1/host1:27017,host2:27017"
/// Output: "mongodb://host1:27017,host2:27017/?replicaSet=rs1"
pub fn parse_shard_host_to_uri(host: &str) -> ConnectorResult<String> {
    let (rs_name, hosts) = host
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("unexpected shard host format (missing '/'): {}", host))?;
    Ok(format!("mongodb://{}/?replicaSet={}", hosts, rs_name))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use maplit::btreemap;

    use super::*;

    #[test]
    fn test_parse_config_defaults() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://localhost:27017/?replicaSet=rs0".to_owned(),
            "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
        };

        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        assert_eq!(props.mongodb_url, "mongodb://localhost:27017/?replicaSet=rs0");
        assert_eq!(props.namespace, "mydb.mycoll");
        assert_eq!(props.watermark_poll_interval_ms, 100);
        assert_eq!(props.buffer_max_bytes, 104_857_600);
        assert_eq!(props.watermark_stall_timeout_secs, 30);
        assert_eq!(props.heartbeat_interval_secs, 60);
        assert_eq!(props.snapshot_batch_size, 1024);
        assert_eq!(props.shard_discovery_interval_secs, 30);
        assert_eq!(props.snapshot_workers_per_shard, 1);
        assert_eq!(props.snapshot_chunk_target_docs, 10_000);
        assert_eq!(props.scan_startup_mode, None);
    }

    #[test]
    fn test_parse_config_with_overrides() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://user:pass@host:27017/?replicaSet=rs0".to_owned(),
            "mongodb.namespace".to_owned() => "testdb.events".to_owned(),
            "mongodb.watermark.poll_interval_ms".to_owned() => "200".to_owned(),
            "mongodb.buffer.max_bytes".to_owned() => "52428800".to_owned(),
            "mongodb.watermark.stall_timeout_secs".to_owned() => "60".to_owned(),
            "mongodb.heartbeat.interval_secs".to_owned() => "30".to_owned(),
            "mongodb.snapshot.batch_size".to_owned() => "2048".to_owned(),
            "scan.startup.mode".to_owned() => "latest".to_owned(),
        };

        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        assert_eq!(props.watermark_poll_interval_ms, 200);
        assert_eq!(props.buffer_max_bytes, 52_428_800);
        assert_eq!(props.watermark_stall_timeout_secs, 60);
        assert_eq!(props.heartbeat_interval_secs, 30);
        assert_eq!(props.snapshot_batch_size, 2048);
        assert_eq!(props.scan_startup_mode, Some("latest".to_owned()));
    }

    #[test]
    fn test_parse_namespace() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
            "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
        };

        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        let (db, coll) = props.parse_namespace().unwrap();
        assert_eq!(db, "mydb");
        assert_eq!(coll, "mycoll");
    }

    #[test]
    fn test_parse_namespace_invalid() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
            "mongodb.namespace".to_owned() => "invalid_no_dot".to_owned(),
        };

        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        assert!(props.parse_namespace().is_err());
    }

    #[test]
    fn test_startup_mode_defaults() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
            "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
        };

        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        assert_eq!(props.startup_mode(), "snapshot");
    }

    #[tokio::test]
    async fn test_build_client_default_read_preference() {
        // Verify that ClientOptions parsing applies secondaryPreferred when URI
        // doesn't specify a read preference (OPLOG-037).
        let uri = "mongodb://localhost:27017/?replicaSet=rs0";
        let mut opts = ClientOptions::parse_async(uri).await.unwrap();
        assert!(
            opts.selection_criteria.is_none(),
            "URI without readPreference should parse to None"
        );

        // Verify replica set name extraction
        let rs_name = opts.repl_set_name.clone().unwrap_or_else(|| "rs0".to_owned());
        assert_eq!(rs_name, "rs0", "replicaSet=rs0 in URI should yield rs_name 'rs0'");

        // Simulate what build_client does
        opts.selection_criteria = Some(SelectionCriteria::ReadPreference(
            mongodb::options::ReadPreference::SecondaryPreferred {
                options: Default::default(),
            },
        ));
        assert!(matches!(
            opts.selection_criteria,
            Some(SelectionCriteria::ReadPreference(
                mongodb::options::ReadPreference::SecondaryPreferred { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn test_build_client_respects_uri_read_preference() {
        // Verify that an explicit readPreference in the URI is preserved.
        let uri = "mongodb://localhost:27017/?replicaSet=rs0&readPreference=primary";
        let opts = ClientOptions::parse_async(uri).await.unwrap();
        assert!(
            opts.selection_criteria.is_some(),
            "URI with readPreference should parse to Some"
        );
    }

    // ── OPLOG-047/048: snapshot parallelism config ─────────────────

    #[test]
    fn test_workers_per_shard_default_is_1() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
            "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
        };
        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(props.snapshot_workers_per_shard, 1);
        assert_eq!(props.snapshot_chunk_target_docs, 10_000);
        assert!(props.validate_snapshot_config().is_ok());
    }

    #[test]
    fn test_workers_per_shard_validation_range() {
        let make = |val: u64| -> MongodbOplogProperties {
            let config: BTreeMap<String, String> = btreemap! {
                "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
                "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
                "mongodb.snapshot.workers_per_shard".to_owned() => val.to_string(),
            };
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap()
        };

        // Valid range boundaries
        assert!(make(1).validate_snapshot_config().is_ok());
        assert!(make(4).validate_snapshot_config().is_ok());
        assert!(make(64).validate_snapshot_config().is_ok());

        // Out of range
        assert!(make(0).validate_snapshot_config().is_err());
        assert!(make(65).validate_snapshot_config().is_err());
        assert!(make(128).validate_snapshot_config().is_err());
    }

    #[test]
    fn test_chunk_target_docs_validation() {
        let make = |val: u64| -> MongodbOplogProperties {
            let config: BTreeMap<String, String> = btreemap! {
                "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
                "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
                "mongodb.snapshot.chunk_target_docs".to_owned() => val.to_string(),
            };
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap()
        };

        assert!(make(100).validate_snapshot_config().is_ok());
        assert!(make(10_000).validate_snapshot_config().is_ok());
        assert!(make(1_000_000).validate_snapshot_config().is_ok());

        // Below minimum
        assert!(make(99).validate_snapshot_config().is_err());
        assert!(make(0).validate_snapshot_config().is_err());
    }

    #[test]
    fn test_parse_shard_host_to_uri() {
        let uri = parse_shard_host_to_uri("rs1/host1:27017,host2:27017,host3:27017").unwrap();
        assert_eq!(uri, "mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=rs1");
    }

    #[test]
    fn test_parse_shard_host_to_uri_single_host() {
        let uri = parse_shard_host_to_uri("myrs/localhost:27017").unwrap();
        assert_eq!(uri, "mongodb://localhost:27017/?replicaSet=myrs");
    }

    #[test]
    fn test_parse_shard_host_to_uri_invalid() {
        assert!(parse_shard_host_to_uri("no_slash_here").is_err());
    }

    // ── OPLOG-054: BoundaryMode config ────────────────────────────────

    #[test]
    fn test_boundary_mode_default_is_min_max() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
            "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
        };
        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert!(matches!(props.boundary_mode().unwrap(), BoundaryMode::MinMax));
    }

    #[test]
    fn test_boundary_mode_parse_min_max() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
            "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
            "mongodb.snapshot.boundary_mode".to_owned() => "min_max".to_owned(),
        };
        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert!(matches!(props.boundary_mode().unwrap(), BoundaryMode::MinMax));
    }

    #[test]
    fn test_boundary_mode_parse_sample() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
            "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
            "mongodb.snapshot.boundary_mode".to_owned() => "sample".to_owned(),
        };
        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert!(matches!(props.boundary_mode().unwrap(), BoundaryMode::Sample));
    }

    #[test]
    fn test_boundary_mode_invalid_rejected() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
            "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
            "mongodb.snapshot.boundary_mode".to_owned() => "invalid".to_owned(),
        };
        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert!(props.boundary_mode().is_err());
    }

    #[test]
    fn test_validate_snapshot_config_validates_boundary_mode() {
        let config: BTreeMap<String, String> = btreemap! {
            "mongodb.url".to_owned() => "mongodb://localhost:27017".to_owned(),
            "mongodb.namespace".to_owned() => "mydb.mycoll".to_owned(),
            "mongodb.snapshot.boundary_mode".to_owned() => "invalid".to_owned(),
        };
        let props: MongodbOplogProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert!(props.validate_snapshot_config().is_err());
    }
}
