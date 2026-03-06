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

use mongodb::bson::{Bson, Document};

use crate::error::ConnectorResult;
use crate::source::base::SourceMessage;
use crate::source::mongodb_oplog::oplog_types::oplog_op_to_debezium_op;
use crate::source::{SourceMeta, SplitId};

/// Convert an oplog entry to Debezium-compatible key and value JSON bytes (OPLOG-009/010/026).
///
/// **Key format** (OPLOG-010):
/// ```json
/// {"schema":null,"payload":{"id":"<bson_extended_json_id>"}}
/// ```
///
/// **Value format** (OPLOG-026):
/// ```json
/// {"schema":null,"payload":{"before":null,"after":"<bson_extended_json_doc>","source":{...},"op":"c","ts_ms":123}}
/// ```
///
/// For updates (OPLOG-032), `post_image` must be provided (the full document after read-back).
/// For deletes, `after` is null. For inserts, `after` comes from `entry["o"]`.
pub fn oplog_entry_to_source_message(
    entry: &Document,
    post_image: Option<&Document>,
    rs_name: &str,
    db_name: &str,
    coll_name: &str,
    split_id: SplitId,
    offset: String,
) -> ConnectorResult<Option<SourceMessage>> {
    let op = entry
        .get_str("op")
        .map_err(|e| anyhow::anyhow!("oplog entry missing 'op' field: {}", e))?;

    let debezium_op = match oplog_op_to_debezium_op(op) {
        Some(op) => op,
        None => return Ok(None), // skip noop/command
    };

    // Extract _id from the oplog entry
    let id_bson = extract_id_from_oplog(entry, op)?;
    let id_extended_json = bson_to_canonical_extjson(&id_bson);

    // Build key (OPLOG-010)
    let key_json = serde_json::json!({
        "schema": null,
        "payload": {
            "id": id_extended_json
        }
    });

    // Extract timestamp (OPLOG-012)
    let ts = entry
        .get_timestamp("ts")
        .map_err(|e| anyhow::anyhow!("oplog entry missing 'ts' field: {}", e))?;
    let ts_ms = (ts.time as i64) * 1000;

    // Build after field
    let after_json: Option<String> = match op {
        "i" => {
            // Insert: full document is in entry["o"]
            let doc = entry
                .get_document("o")
                .map_err(|e| anyhow::anyhow!("insert oplog entry missing 'o' field: {}", e))?;
            Some(document_to_canonical_extjson(doc))
        }
        "u" => {
            // Update (OPLOG-032): use post_image from read-back
            match post_image {
                Some(doc) => Some(document_to_canonical_extjson(doc)),
                None => {
                    // If no post-image available, the document may have been deleted
                    // between the update and our read-back. Treat as a best-effort scenario.
                    tracing::warn!("No post-image for update on _id={}, document may have been deleted", id_extended_json);
                    None
                }
            }
        }
        "d" => None, // Delete: after is null
        _ => return Ok(None),
    };

    // Build source metadata block (OPLOG-012)
    let source = serde_json::json!({
        "version": "mongo-oplog-1.0",
        "connector": "mongodb",
        "name": "RW_MONGO_OPLOG",
        "ts_ms": ts_ms,
        "db": db_name,
        "rs": rs_name,
        "collection": coll_name,
        "ord": ts.increment,
    });

    // Build value envelope (OPLOG-026)
    let value_json = serde_json::json!({
        "schema": null,
        "payload": {
            "before": null,
            "after": after_json,
            "source": source,
            "op": debezium_op,
            "ts_ms": ts_ms,
        }
    });

    let key_bytes = serde_json::to_vec(&key_json)?;
    let value_bytes = serde_json::to_vec(&value_json)?;

    Ok(Some(SourceMessage {
        key: Some(key_bytes),
        payload: Some(value_bytes),
        offset,
        split_id,
        meta: SourceMeta::Empty,
    }))
}

/// Extract `_id` from an oplog entry based on operation type.
fn extract_id_from_oplog(entry: &Document, op: &str) -> ConnectorResult<Bson> {
    match op {
        "i" => {
            // Insert: _id is in entry["o"]["_id"]
            let o = entry
                .get_document("o")
                .map_err(|e| anyhow::anyhow!("insert entry missing 'o': {}", e))?;
            o.get("_id")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("insert document missing '_id'").into())
        }
        "u" => {
            // Update: _id is in entry["o2"]["_id"]
            let o2 = entry
                .get_document("o2")
                .map_err(|e| anyhow::anyhow!("update entry missing 'o2': {}", e))?;
            o2.get("_id")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("update entry missing 'o2._id'").into())
        }
        "d" => {
            // Delete: _id is in entry["o"]["_id"]
            let o = entry
                .get_document("o")
                .map_err(|e| anyhow::anyhow!("delete entry missing 'o': {}", e))?;
            o.get("_id")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("delete document missing '_id'").into())
        }
        _ => Err(anyhow::anyhow!("unexpected op type: {}", op).into()),
    }
}

/// Convert a BSON value to canonical Extended JSON v2 string representation (OPLOG-027).
fn bson_to_canonical_extjson(bson: &Bson) -> String {
    bson.clone().into_canonical_extjson().to_string()
}

/// Convert a BSON Document to canonical Extended JSON v2 string (OPLOG-027).
fn document_to_canonical_extjson(doc: &Document) -> String {
    Bson::Document(doc.clone())
        .into_canonical_extjson()
        .to_string()
}

/// Convert a document to a Debezium `"c"` (create) SourceMessage for snapshot rows (OPLOG-033).
pub fn snapshot_doc_to_source_message(
    doc: &Document,
    rs_name: &str,
    db_name: &str,
    coll_name: &str,
    split_id: SplitId,
    offset: String,
) -> ConnectorResult<SourceMessage> {
    let id_bson = doc
        .get("_id")
        .ok_or_else(|| anyhow::anyhow!("snapshot document missing '_id'"))?;
    let id_extended_json = bson_to_canonical_extjson(id_bson);

    let key_json = serde_json::json!({
        "schema": null,
        "payload": {
            "id": id_extended_json
        }
    });

    let after = document_to_canonical_extjson(doc);

    let source = serde_json::json!({
        "version": "mongo-oplog-1.0",
        "connector": "mongodb",
        "name": "RW_MONGO_OPLOG",
        "ts_ms": 0,
        "snapshot": "true",
        "db": db_name,
        "rs": rs_name,
        "collection": coll_name,
        "ord": 0,
    });

    let value_json = serde_json::json!({
        "schema": null,
        "payload": {
            "before": null,
            "after": after,
            "source": source,
            "op": "r", // read/snapshot
            "ts_ms": 0,
        }
    });

    let key_bytes = serde_json::to_vec(&key_json)?;
    let value_bytes = serde_json::to_vec(&value_json)?;

    Ok(SourceMessage {
        key: Some(key_bytes),
        payload: Some(value_bytes),
        offset,
        split_id,
        meta: SourceMeta::Empty,
    })
}

#[cfg(test)]
mod tests {
    use mongodb::bson::{doc, oid::ObjectId, Timestamp};

    use super::*;

    fn make_insert_entry(id: &str, field_val: &str) -> Document {
        doc! {
            "ts": Timestamp { time: 1706968217, increment: 1 },
            "op": "i",
            "ns": "mydb.mycoll",
            "o": {
                "_id": ObjectId::parse_str(id).unwrap(),
                "name": field_val,
            }
        }
    }

    fn make_update_entry(id: &str) -> Document {
        doc! {
            "ts": Timestamp { time: 1706968218, increment: 2 },
            "op": "u",
            "ns": "mydb.mycoll",
            "o": { "$set": { "name": "updated" } },
            "o2": { "_id": ObjectId::parse_str(id).unwrap() }
        }
    }

    fn make_delete_entry(id: &str) -> Document {
        doc! {
            "ts": Timestamp { time: 1706968219, increment: 3 },
            "op": "d",
            "ns": "mydb.mycoll",
            "o": { "_id": ObjectId::parse_str(id).unwrap() }
        }
    }

    #[test]
    fn test_insert_to_debezium() {
        let oid = "65bc9fb6c485f419a7a877fe";
        let entry = make_insert_entry(oid, "alice");
        let msg = oplog_entry_to_source_message(
            &entry,
            None,
            "rs0",
            "mydb",
            "mycoll",
            "0".into(),
            "offset1".to_owned(),
        )
        .unwrap()
        .unwrap();

        // Verify key contains _id
        let key: serde_json::Value = serde_json::from_slice(msg.key.as_ref().unwrap()).unwrap();
        assert!(key["payload"]["id"].as_str().unwrap().contains(oid));

        // Verify value has op="c" and after is a string (OPLOG-026)
        let val: serde_json::Value =
            serde_json::from_slice(msg.payload.as_ref().unwrap()).unwrap();
        assert_eq!(val["payload"]["op"], "c");
        assert!(val["payload"]["after"].is_string());
        let after_str = val["payload"]["after"].as_str().unwrap();
        assert!(after_str.contains("alice"));

        // Verify source metadata (OPLOG-012)
        assert_eq!(val["payload"]["source"]["db"], "mydb");
        assert_eq!(val["payload"]["source"]["collection"], "mycoll");
        assert_eq!(val["payload"]["source"]["rs"], "rs0");
    }

    #[test]
    fn test_update_with_post_image() {
        let oid = "65bc9fb6c485f419a7a877fe";
        let entry = make_update_entry(oid);
        let post_image = doc! {
            "_id": ObjectId::parse_str(oid).unwrap(),
            "name": "updated_alice",
        };

        let msg = oplog_entry_to_source_message(
            &entry,
            Some(&post_image),
            "rs0",
            "mydb",
            "mycoll",
            "0".into(),
            "offset2".to_owned(),
        )
        .unwrap()
        .unwrap();

        let val: serde_json::Value =
            serde_json::from_slice(msg.payload.as_ref().unwrap()).unwrap();
        assert_eq!(val["payload"]["op"], "u");
        let after_str = val["payload"]["after"].as_str().unwrap();
        assert!(after_str.contains("updated_alice"));
    }

    #[test]
    fn test_delete_has_null_after() {
        let oid = "65bc9fb6c485f419a7a877fe";
        let entry = make_delete_entry(oid);

        let msg = oplog_entry_to_source_message(
            &entry,
            None,
            "rs0",
            "mydb",
            "mycoll",
            "0".into(),
            "offset3".to_owned(),
        )
        .unwrap()
        .unwrap();

        let val: serde_json::Value =
            serde_json::from_slice(msg.payload.as_ref().unwrap()).unwrap();
        assert_eq!(val["payload"]["op"], "d");
        assert!(val["payload"]["after"].is_null());
    }

    #[test]
    fn test_noop_is_skipped() {
        let entry = doc! {
            "ts": Timestamp { time: 100, increment: 1 },
            "op": "n",
            "ns": "",
            "o": {}
        };

        let result = oplog_entry_to_source_message(
            &entry, None, "rs0", "mydb", "mycoll", "0".into(), "x".to_owned(),
        )
        .unwrap();

        assert!(result.is_none());
    }

    /// OPLOG-027: Verify all listed BSON types serialize correctly through
    /// the Debezium message path (canonical Extended JSON v2).
    #[test]
    fn test_all_bson_types_in_insert() {
        use mongodb::bson::{self, Binary, Bson, Decimal128};

        let all_types_doc = doc! {
            "_id": ObjectId::parse_str("65bc9fb6c485f419a7a877fe").unwrap(),
            "string_field": "hello",
            "int32_field": 42_i32,
            "int64_field": 9_876_543_210_i64,
            "double_field": 3.14_f64,
            "decimal128_field": Decimal128::from_bytes([0u8; 16]),
            "bool_true": true,
            "bool_false": false,
            "date_field": bson::DateTime::from_millis(1706968217000),
            "timestamp_field": Timestamp { time: 1706968217, increment: 1 },
            "binary_field": Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0xDE, 0xAD, 0xBE, 0xEF] },
            "array_field": [1_i32, 2_i32, 3_i32],
            "nested_doc": { "inner": "value", "count": 99_i32 },
            "null_field": Bson::Null,
        };

        let entry = doc! {
            "ts": Timestamp { time: 1706968217, increment: 1 },
            "op": "i",
            "ns": "mydb.mycoll",
            "o": all_types_doc,
        };

        let msg = oplog_entry_to_source_message(
            &entry, None, "rs0", "mydb", "mycoll", "0".into(), "offset".to_owned(),
        )
        .unwrap()
        .unwrap();

        let val: serde_json::Value =
            serde_json::from_slice(msg.payload.as_ref().unwrap()).unwrap();
        let after_str = val["payload"]["after"].as_str().unwrap();
        let after: serde_json::Value =
            serde_json::from_str(after_str).expect("after should be valid JSON");

        // ObjectId → {"$oid": "..."}
        assert!(
            after["_id"]["$oid"].is_string(),
            "ObjectId should serialize to $oid, got: {}",
            after["_id"]
        );

        // String → plain string
        assert_eq!(after["string_field"], "hello");

        // Int32 → {"$numberInt": "42"}
        assert_eq!(
            after["int32_field"]["$numberInt"].as_str().unwrap(),
            "42"
        );

        // Int64 → {"$numberLong": "9876543210"}
        assert_eq!(
            after["int64_field"]["$numberLong"].as_str().unwrap(),
            "9876543210"
        );

        // Double → {"$numberDouble": "3.14"}
        assert!(
            after["double_field"]["$numberDouble"].is_string(),
            "Double should serialize to $numberDouble"
        );

        // Decimal128 → {"$numberDecimal": "..."}
        assert!(
            after["decimal128_field"]["$numberDecimal"].is_string(),
            "Decimal128 should serialize to $numberDecimal"
        );

        // Boolean → plain bool
        assert_eq!(after["bool_true"], true);
        assert_eq!(after["bool_false"], false);

        // DateTime → {"$date": {"$numberLong": "..."}}
        assert!(
            after["date_field"]["$date"].is_object(),
            "DateTime should serialize to $date, got: {}",
            after["date_field"]
        );

        // Timestamp → {"$timestamp": {"t": ..., "i": ...}}
        assert!(
            after["timestamp_field"]["$timestamp"].is_object(),
            "Timestamp should serialize to $timestamp"
        );

        // Binary → {"$binary": {"base64": "...", "subType": "00"}}
        assert!(
            after["binary_field"]["$binary"].is_object(),
            "Binary should serialize to $binary"
        );

        // Array → JSON array
        assert!(after["array_field"].is_array());
        assert_eq!(after["array_field"].as_array().unwrap().len(), 3);

        // Embedded Document → nested JSON object
        assert!(after["nested_doc"].is_object());
        assert_eq!(after["nested_doc"]["inner"], "value");
        assert_eq!(
            after["nested_doc"]["count"]["$numberInt"].as_str().unwrap(),
            "99"
        );

        // Null → JSON null
        assert!(after["null_field"].is_null());
    }

    #[test]
    fn test_snapshot_doc_to_source_message() {
        let doc = doc! {
            "_id": ObjectId::parse_str("65bc9fb6c485f419a7a877fe").unwrap(),
            "name": "snapshot_test",
            "value": 42,
        };

        let msg = snapshot_doc_to_source_message(
            &doc,
            "rs0",
            "mydb",
            "mycoll",
            "0".into(),
            "snap_offset".to_owned(),
        )
        .unwrap();

        let val: serde_json::Value =
            serde_json::from_slice(msg.payload.as_ref().unwrap()).unwrap();
        assert_eq!(val["payload"]["op"], "r");
        assert!(val["payload"]["after"].is_string());
        let after_str = val["payload"]["after"].as_str().unwrap();
        assert!(after_str.contains("snapshot_test"));
        assert_eq!(val["payload"]["source"]["snapshot"], "true");
    }
}
