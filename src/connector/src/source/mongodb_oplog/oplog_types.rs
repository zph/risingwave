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

/// Oplog operation type constants (OPLOG-011).
pub const OP_INSERT: &str = "i";
pub const OP_UPDATE: &str = "u";
pub const OP_DELETE: &str = "d";
pub const OP_NOOP: &str = "n";
pub const OP_COMMAND: &str = "c";

/// Map oplog operation to Debezium operation code (OPLOG-011).
///
/// Returns `None` for noop and command operations which are skipped.
pub fn oplog_op_to_debezium_op(op: &str) -> Option<&'static str> {
    match op {
        OP_INSERT => Some("c"), // create
        OP_UPDATE => Some("u"), // update
        OP_DELETE => Some("d"), // delete
        OP_NOOP | OP_COMMAND => None,
        _ => None,
    }
}

/// Offset representation for oplog positions, ordered by (timestamp_secs, ordinal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OplogOffset {
    pub timestamp_secs: u32,
    pub ordinal: u32,
}

impl PartialOrd for OplogOffset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OplogOffset {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp_secs
            .cmp(&other.timestamp_secs)
            .then(self.ordinal.cmp(&other.ordinal))
    }
}

/// Encode an oplog timestamp as a single u64 for atomic storage (OPLOG-004).
///
/// Format: `(timestamp_secs << 32) | ordinal`
pub fn encode_watermark(timestamp_secs: u32, ordinal: u32) -> u64 {
    ((timestamp_secs as u64) << 32) | (ordinal as u64)
}

/// Decode a u64 watermark back into (timestamp_secs, ordinal).
pub fn decode_watermark(watermark: u64) -> (u32, u32) {
    let timestamp_secs = (watermark >> 32) as u32;
    let ordinal = watermark as u32;
    (timestamp_secs, ordinal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oplog_op_mapping() {
        // OPLOG-011: i→c, u→u, d→d
        assert_eq!(oplog_op_to_debezium_op("i"), Some("c"));
        assert_eq!(oplog_op_to_debezium_op("u"), Some("u"));
        assert_eq!(oplog_op_to_debezium_op("d"), Some("d"));
        assert_eq!(oplog_op_to_debezium_op("n"), None);
        assert_eq!(oplog_op_to_debezium_op("c"), None);
        assert_eq!(oplog_op_to_debezium_op("unknown"), None);
    }

    #[test]
    fn test_oplog_offset_ordering() {
        let a = OplogOffset {
            timestamp_secs: 100,
            ordinal: 1,
        };
        let b = OplogOffset {
            timestamp_secs: 100,
            ordinal: 2,
        };
        let c = OplogOffset {
            timestamp_secs: 101,
            ordinal: 0,
        };

        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn test_watermark_encode_decode_roundtrip() {
        let ts = 1706968217u32;
        let ord = 42u32;
        let encoded = encode_watermark(ts, ord);
        let (decoded_ts, decoded_ord) = decode_watermark(encoded);
        assert_eq!(decoded_ts, ts);
        assert_eq!(decoded_ord, ord);
    }

    #[test]
    fn test_watermark_ordering() {
        // Encoded watermarks should preserve ordering
        let w1 = encode_watermark(100, 1);
        let w2 = encode_watermark(100, 2);
        let w3 = encode_watermark(101, 0);
        assert!(w1 < w2);
        assert!(w2 < w3);
    }
}
