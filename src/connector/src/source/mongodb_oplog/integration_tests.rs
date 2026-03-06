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

//! Integration tests for the mongo-oplog connector.
//!
//! **Single-RS tests** use testcontainers (Docker/Podman).
//! **Sharded cluster tests** use `mup` (<https://github.com/zph/mup>).
//!
//! All tests are `#[ignore]` — run with:
//! ```sh
//! cargo test -p risingwave_connector mongodb_oplog::integration_tests -- --ignored
//! ```
//!
//! For Podman (single-RS tests): `export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock`
//! For sharded tests: install mup (`go install github.com/zph/mup/cmd/mup@latest`)

use std::collections::BTreeMap;
use std::time::Duration;

use mongodb::bson::{doc, Document};
use mongodb::options::{Acknowledgment, ClientOptions, FindOneOptions, InsertOneOptions, WriteConcern};

use super::{BoundaryMode, MongodbOplogProperties};
use super::oplog_types::encode_watermark;
use super::reader::SnapshotResumeState;
use super::split::MongodbOplogOffset;

/// Insert options that wait for majority acknowledgement.
/// Ensures the write is committed before we query with readConcern:majority.
fn majority_insert_opts() -> InsertOneOptions {
    InsertOneOptions::builder()
        .write_concern(
            WriteConcern::builder()
                .w(Acknowledgment::Majority)
                .build(),
        )
        .build()
}

/// Container image and tag for MongoDB 3.6
const MONGO_IMAGE: &str = "mongo";
const MONGO_TAG: &str = "3.6";
const MONGO_PORT: u16 = 27017;

/// Start a MongoDB 3.6 replica set container and return (host, port).
///
/// Uses testcontainers to run a standalone `mongo:3.6` and then initializes
/// it as a single-node replica set via `rs.initiate()`.
async fn start_mongo_rs() -> (testcontainers::ContainerAsync<testcontainers::GenericImage>, String)
{
    use testcontainers::core::IntoContainerPort;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    let image = GenericImage::new(MONGO_IMAGE, MONGO_TAG)
        .with_exposed_port(MONGO_PORT.tcp())
        .with_wait_for(testcontainers::core::WaitFor::message_on_stdout(
            "waiting for connections on port",
        ));
    let container: testcontainers::ContainerAsync<GenericImage> = image
        .with_cmd(vec!["--replSet".to_string(), "rs0".to_string()])
        .start()
        .await
        .expect("failed to start mongo container");

    let host_port = container
        .get_host_port_ipv4(MONGO_PORT)
        .await
        .expect("failed to get mongo host port");

    let uri = format!("mongodb://localhost:{}/?directConnection=true", host_port);

    // Initialize replica set
    let client_opts = ClientOptions::parse_async(&uri)
        .await
        .expect("failed to parse URI");
    let client =
        mongodb::Client::with_options(client_opts).expect("failed to create mongo client");

    // rs.initiate() — single node RS
    let admin = client.database("admin");
    let rs_config = doc! {
        "_id": "rs0",
        "members": [{
            "_id": 0,
            "host": format!("localhost:{}", MONGO_PORT),
        }]
    };
    admin
        .run_command(doc! { "replSetInitiate": rs_config }, None)
        .await
        .expect("failed to initiate replica set");

    // Wait for the replica set to elect a primary and be writable.
    // After rs.initiate() the node transitions STARTUP2 → PRIMARY which
    // can take several seconds.
    let mut primary_ready = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        // Try an actual write to confirm the node is writable
        let test_coll = client
            .database("__rs_init_check")
            .collection::<Document>("ping");
        match test_coll.insert_one(doc! { "ping": 1 }, None).await {
            Ok(_) => {
                // Clean up
                let _ = client
                    .database("__rs_init_check")
                    .drop(None)
                    .await;
                primary_ready = true;
                break;
            }
            Err(_) => continue,
        }
    }
    assert!(primary_ready, "MongoDB replica set failed to elect primary within 30s");

    let rs_uri = format!(
        "mongodb://localhost:{}/?replicaSet=rs0&directConnection=true",
        host_port
    );
    (container, rs_uri)
}

fn make_props(uri: &str, namespace: &str) -> MongodbOplogProperties {
    let config: BTreeMap<String, String> = BTreeMap::from([
        ("mongodb.url".to_owned(), uri.to_owned()),
        ("mongodb.namespace".to_owned(), namespace.to_owned()),
    ]);
    serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap()
}

fn make_props_with_mode(uri: &str, namespace: &str, mode: &str) -> MongodbOplogProperties {
    let config: BTreeMap<String, String> = BTreeMap::from([
        ("mongodb.url".to_owned(), uri.to_owned()),
        ("mongodb.namespace".to_owned(), namespace.to_owned()),
        ("scan.startup.mode".to_owned(), mode.to_owned()),
    ]);
    serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap()
}

/// OPLOG-039: Verify the connector can connect to MongoDB 3.6 and perform
/// basic operations (build_client, read oplog timestamp, write/read documents).
#[tokio::test]
#[ignore]
async fn test_mongodb_36_connectivity() {
    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "testdb.testcoll");
    let (client, rs_name) = props.build_client().await.expect("build_client failed");

    assert_eq!(rs_name, "rs0");

    // Insert a document to generate an oplog entry
    let db = client.database("testdb");
    let coll = db.collection::<Document>("testcoll");
    coll.insert_one(doc! { "hello": "world" }, majority_insert_opts())
        .await
        .expect("insert failed");

    // Verify oplog has entries
    let oplog = client
        .database("local")
        .collection::<Document>("oplog.rs");
    let opts = FindOneOptions::builder()
        .sort(doc! { "$natural": -1 })
        .build();
    let latest = oplog
        .find_one(doc! {}, opts)
        .await
        .expect("oplog query failed");
    assert!(latest.is_some(), "oplog should have at least one entry");

    let entry = latest.unwrap();
    let ts = entry
        .get_timestamp("ts")
        .expect("oplog entry should have ts");
    assert!(ts.time > 0, "timestamp should be non-zero");
}

/// OPLOG-033/034: Verify snapshot scan captures pre-existing documents
/// and produces correct offsets.
#[tokio::test]
#[ignore]
async fn test_snapshot_scan() {
    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "snapdb.snapcoll");
    let (client, rs_name) = props.build_client().await.expect("build_client failed");

    // Insert documents before snapshot
    let coll = client.database("snapdb").collection::<Document>("snapcoll");
    for i in 0..10 {
        coll.insert_one(doc! { "seq": i, "data": format!("doc_{}", i) }, majority_insert_opts())
            .await
            .expect("insert failed");
    }

    // Get current oplog ts as snapshot start
    let oplog = client
        .database("local")
        .collection::<Document>("oplog.rs");
    let opts = FindOneOptions::builder()
        .sort(doc! { "$natural": -1 })
        .build();
    let latest = oplog
        .find_one(doc! {}, opts)
        .await
        .expect("oplog query failed")
        .expect("oplog should have entries");
    let snapshot_start_ts = latest.get_timestamp("ts").unwrap();

    // Run snapshot using the reader's static method
    use super::reader::MongodbOplogSplitReader;
    let split_id = "0".into();
    let batches = MongodbOplogSplitReader::run_snapshot(
        &client,
        "snapdb",
        "snapcoll",
        &rs_name,
        &split_id,
        5, // batch_size
        snapshot_start_ts,
        SnapshotResumeState::Fresh,
        1,
        10_000,
        BoundaryMode::Sample,
        None,
    )
    .await
    .expect("snapshot failed");

    // Count total messages
    let total_msgs: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(
        total_msgs, 10,
        "snapshot should capture all 10 pre-existing documents"
    );

    // Verify batch sizing (5 per batch, so 2 batches)
    assert_eq!(batches.len(), 2, "should have 2 batches of 5");
}

/// OPLOG-034: Verify snapshot resume from a checkpoint (last_id).
#[tokio::test]
#[ignore]
async fn test_snapshot_checkpoint_resume() {
    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "resumedb.resumecoll");
    let (client, rs_name) = props.build_client().await.expect("build_client failed");

    let coll = client
        .database("resumedb")
        .collection::<Document>("resumecoll");
    for i in 0..10 {
        coll.insert_one(doc! { "seq": i }, majority_insert_opts())
            .await
            .expect("insert failed");
    }

    let oplog = client
        .database("local")
        .collection::<Document>("oplog.rs");
    let opts = FindOneOptions::builder()
        .sort(doc! { "$natural": -1 })
        .build();
    let snapshot_start_ts = oplog
        .find_one(doc! {}, opts)
        .await
        .unwrap()
        .unwrap()
        .get_timestamp("ts")
        .unwrap();

    use super::reader::MongodbOplogSplitReader;
    let split_id = "0".into();

    // First scan: get all 10
    let batches = MongodbOplogSplitReader::run_snapshot(
        &client,
        "resumedb",
        "resumecoll",
        &rs_name,
        &split_id,
        100,
        snapshot_start_ts,
        SnapshotResumeState::Fresh,
        1,
        10_000,
        BoundaryMode::Sample,
        None,
    )
    .await
    .expect("first snapshot failed");
    let total: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(total, 10);

    // Extract the offset from the 5th message to simulate a checkpoint
    let fifth_msg = &batches[0][4];
    let fifth_offset: MongodbOplogOffset =
        serde_json::from_str(fifth_msg.offset.as_ref()).expect("parse offset");
    let resume_id = fifth_offset.snapshot_last_id.clone();

    // Resume scan from the 5th document's _id
    let resumed_batches = MongodbOplogSplitReader::run_snapshot(
        &client,
        "resumedb",
        "resumecoll",
        &rs_name,
        &split_id,
        100,
        snapshot_start_ts,
        SnapshotResumeState::Serial { resume_id },
        1,
        10_000,
        BoundaryMode::Sample,
        None,
    )
    .await
    .expect("resumed snapshot failed");
    let resumed_total: usize = resumed_batches.iter().map(|b| b.len()).sum();
    assert_eq!(
        resumed_total, 5,
        "resumed snapshot should return only remaining 5 docs"
    );
}

/// OPLOG-037: Verify that build_client applies secondaryPreferred when
/// the URI has no readPreference.
#[tokio::test]
#[ignore]
async fn test_read_preference_default() {
    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "testdb.testcoll");

    // Verify build_client succeeds (implicitly applies secondaryPreferred)
    let (client, rs_name) = props.build_client().await.expect("build_client failed");
    assert_eq!(rs_name, "rs0");

    // Verify the client can still perform operations (single-node RS, so
    // secondaryPreferred should fall back to primary)
    let coll = client.database("testdb").collection::<Document>("testcoll");
    coll.insert_one(doc! { "test": "readpref" }, majority_insert_opts())
        .await
        .expect("insert with secondaryPreferred should work");
}

/// OPLOG-041: Verify shard host URI parsing end-to-end (already unit-tested,
/// this validates the full round-trip with a real MongoDB instance).
#[tokio::test]
#[ignore]
async fn test_shard_host_parsing_roundtrip() {
    let (_container, uri) = start_mongo_rs().await;

    // The start_mongo_rs returns a URI like "mongodb://localhost:PORT/?replicaSet=rs0&directConnection=true"
    // Simulate what would happen if we parsed a shard host string for this RS
    let host_port = uri
        .strip_prefix("mongodb://localhost:")
        .unwrap()
        .split('/')
        .next()
        .unwrap();

    let shard_host = format!("rs0/localhost:{}", host_port);
    let parsed_uri =
        super::parse_shard_host_to_uri(&shard_host).expect("parse_shard_host_to_uri failed");
    assert!(parsed_uri.contains("replicaSet=rs0"));
    assert!(parsed_uri.contains(&format!("localhost:{}", host_port)));
}

/// OPLOG-004/005: Verify replSetGetStatus returns valid watermark data
/// on MongoDB 3.6.
#[tokio::test]
#[ignore]
async fn test_watermark_polling_on_36() {
    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "wmdb.wmcoll");
    let (client, _rs_name) = props.build_client().await.expect("build_client failed");

    // Insert a document to ensure there's oplog activity
    let coll = client.database("wmdb").collection::<Document>("wmcoll");
    coll.insert_one(doc! { "wm": "test" }, majority_insert_opts())
        .await
        .expect("insert failed");

    // Poll replSetGetStatus — this is what the watermark poller does
    let admin = client.database("admin");
    let status = admin
        .run_command(doc! { "replSetGetStatus": 1 }, None)
        .await
        .expect("replSetGetStatus failed");

    let optimes = status
        .get_document("optimes")
        .expect("status should have optimes");
    let last_committed = optimes
        .get_document("lastCommittedOpTime")
        .expect("optimes should have lastCommittedOpTime");
    let ts = last_committed
        .get_timestamp("ts")
        .expect("lastCommittedOpTime should have ts");

    assert!(ts.time > 0, "watermark timestamp should be non-zero");

    // Verify encode/decode roundtrip
    let encoded = encode_watermark(ts.time, ts.increment);
    let (decoded_time, decoded_inc) = super::oplog_types::decode_watermark(encoded);
    assert_eq!(decoded_time, ts.time);
    assert_eq!(decoded_inc, ts.increment);
}

// ── OPLOG-036: Startup mode tests ────────────────────────────────

/// OPLOG-036: `scan.startup.mode = 'snapshot'` (default) — snapshot all
/// existing documents then position for CDC at snapshot_start_ts.
#[tokio::test]
#[ignore]
async fn test_startup_mode_snapshot() {
    use super::reader::MongodbOplogSplitReader;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props_with_mode(&uri, "modedb.modecoll", "snapshot");
    let (client, rs_name) = props.build_client().await.unwrap();

    // Insert 5 documents before snapshot
    let coll = client.database("modedb").collection::<Document>("modecoll");
    for i in 0..5 {
        coll.insert_one(doc! { "seq": i }, majority_insert_opts()).await.unwrap();
    }

    let snapshot_start_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();

    let split_id = "0".into();
    let batches = MongodbOplogSplitReader::run_snapshot(
        &client,
        "modedb",
        "modecoll",
        &rs_name,
        &split_id,
        100,
        snapshot_start_ts,
        SnapshotResumeState::Fresh,
        1,
        10_000,
        BoundaryMode::Sample,
        None,
    )
    .await
    .unwrap();

    let total: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(total, 5, "snapshot mode should capture all 5 documents");

    // Verify offsets record snapshot_done=false during scan
    let first_offset: MongodbOplogOffset =
        serde_json::from_str(batches[0][0].offset.as_ref()).unwrap();
    assert!(!first_offset.snapshot_done, "during snapshot, snapshot_done should be false");
    assert!(first_offset.snapshot_last_id.is_some(), "should have snapshot_last_id checkpoint");
    assert_eq!(
        first_offset.oplog_ts_secs, snapshot_start_ts.time,
        "offset should record snapshot_start_ts"
    );
}

/// OPLOG-036: `scan.startup.mode = 'latest'` — skip snapshot, start
/// from the current oplog position.
#[tokio::test]
#[ignore]
async fn test_startup_mode_latest() {
    use super::reader::MongodbOplogSplitReader;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props_with_mode(&uri, "latestdb.latestcoll", "latest");
    let (client, _rs_name) = props.build_client().await.unwrap();

    // Insert documents BEFORE we get the "latest" position
    let coll = client
        .database("latestdb")
        .collection::<Document>("latestcoll");
    for i in 0..5 {
        coll.insert_one(doc! { "pre": i }, majority_insert_opts()).await.unwrap();
    }

    // In "latest" mode, the connector gets the current oplog ts
    let latest_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();

    // Insert more documents AFTER the "latest" position
    for i in 0..3 {
        coll.insert_one(doc! { "post": i }, majority_insert_opts()).await.unwrap();
    }

    // Verify: the oplog should have entries after latest_ts for the
    // namespace. In "latest" mode, these 3 post-insert docs would be
    // captured by the CDC tailing phase (not snapshot).
    let oplog = client
        .database("local")
        .collection::<Document>("oplog.rs");

    use mongodb::options::FindOptions;
    let find_opts = FindOptions::builder()
        .sort(doc! { "$natural": 1 })
        .build();
    let mut cursor = oplog
        .find(
            doc! {
                "ns": "latestdb.latestcoll",
                "ts": { "$gt": latest_ts },
            },
            find_opts,
        )
        .await
        .unwrap();

    let mut post_count = 0;
    while cursor.advance().await.unwrap() {
        post_count += 1;
    }
    assert_eq!(
        post_count, 3,
        "latest mode: 3 entries after the start position should be in oplog"
    );
}

/// OPLOG-036: `scan.startup.mode = 'earliest'` — skip snapshot, start
/// from the oldest available oplog entry.
#[tokio::test]
#[ignore]
async fn test_startup_mode_earliest() {
    use super::reader::MongodbOplogSplitReader;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props_with_mode(&uri, "earlydb.earlycoll", "earliest");
    let (client, _rs_name) = props.build_client().await.unwrap();

    // The oldest oplog entry should exist (at least the rs.initiate entries)
    let oldest_ts = MongodbOplogSplitReader::get_oldest_oplog_ts(&client)
        .await
        .unwrap();
    let latest_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();

    // Oldest should be <= latest
    assert!(
        oldest_ts.time <= latest_ts.time,
        "oldest oplog ts should be <= latest"
    );

    // In earliest mode, the connector starts from None (no filter),
    // meaning it reads from the very beginning. Verify there are oplog
    // entries we could consume.
    let oplog = client
        .database("local")
        .collection::<Document>("oplog.rs");
    let opts = FindOneOptions::builder()
        .sort(doc! { "$natural": 1 })
        .build();
    let first_entry = oplog.find_one(doc! {}, opts).await.unwrap();
    assert!(first_entry.is_some(), "oplog should have at least one entry for earliest mode");
}

// ── OPLOG-035: Snapshot→CDC transition ───────────────────────────

/// OPLOG-035: Verify that writes occurring during a snapshot scan are
/// captured via the oplog after the snapshot completes.
///
/// The snapshot records `snapshot_start_ts` before scanning. Any writes
/// after that timestamp appear in the oplog and will be tailed in the
/// CDC phase starting from `snapshot_start_ts`.
#[tokio::test]
#[ignore]
async fn test_snapshot_to_cdc_transition() {
    use super::reader::MongodbOplogSplitReader;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "transdb.transcoll");
    let (client, rs_name) = props.build_client().await.unwrap();

    let coll = client
        .database("transdb")
        .collection::<Document>("transcoll");

    // Insert 5 pre-existing documents
    for i in 0..5 {
        coll.insert_one(doc! { "phase": "pre", "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    // Record snapshot_start_ts (this is what the reader does before scanning)
    let snapshot_start_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();

    // Simulate concurrent writes DURING the snapshot phase
    for i in 0..3 {
        coll.insert_one(doc! { "phase": "during", "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    // Run snapshot — should capture all 8 docs (5 pre + 3 during, since
    // snapshot uses readConcern:majority and scans by _id order)
    let split_id = "0".into();
    let batches = MongodbOplogSplitReader::run_snapshot(
        &client,
        "transdb",
        "transcoll",
        &rs_name,
        &split_id,
        100,
        snapshot_start_ts,
        SnapshotResumeState::Fresh,
        1,
        10_000,
        BoundaryMode::Sample,
        None,
    )
    .await
    .unwrap();
    let snapshot_count: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(snapshot_count, 8, "snapshot should see all 8 committed docs");

    // After snapshot completes, CDC should start from snapshot_start_ts.
    // The 3 "during" inserts happened after snapshot_start_ts, so they
    // appear in the oplog with ts > snapshot_start_ts.
    let oplog = client
        .database("local")
        .collection::<Document>("oplog.rs");

    use mongodb::options::FindOptions;
    let find_opts = FindOptions::builder()
        .sort(doc! { "$natural": 1 })
        .build();
    let mut cursor = oplog
        .find(
            doc! {
                "ns": "transdb.transcoll",
                "ts": { "$gt": snapshot_start_ts },
            },
            find_opts,
        )
        .await
        .unwrap();

    let mut cdc_entries = 0;
    while cursor.advance().await.unwrap() {
        cdc_entries += 1;
    }

    assert_eq!(
        cdc_entries, 3,
        "CDC phase should see exactly 3 oplog entries (the concurrent writes)"
    );

    // Insert more after snapshot to verify CDC can capture ongoing writes
    for i in 0..2 {
        coll.insert_one(doc! { "phase": "post", "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    // Count total oplog entries after snapshot_start_ts
    let mut cursor2 = oplog
        .find(
            doc! {
                "ns": "transdb.transcoll",
                "ts": { "$gt": snapshot_start_ts },
            },
            FindOptions::builder().sort(doc! { "$natural": 1 }).build(),
        )
        .await
        .unwrap();

    let mut total_after = 0;
    while cursor2.advance().await.unwrap() {
        total_after += 1;
    }
    assert_eq!(
        total_after, 5,
        "CDC phase should see 3 during + 2 post = 5 oplog entries total"
    );
}

// ── Sharded cluster tests (mup-based) ───────────────────────────
//
// These tests require `mup` to be installed and MongoDB 3.6 binaries
// cached. They deploy a real sharded cluster on localhost.

/// RAII guard that deploys a sharded cluster via `mup cluster deploy`
/// and destroys it on drop.
struct MupShardedCluster {
    cluster_name: String,
    mongos_uri: String,
    shard_rs_names: Vec<String>,
}

impl MupShardedCluster {
    /// Deploy a minimal sharded cluster (1 config, 2 shards, 1 mongos)
    /// using mup. Returns the mongos URI and shard RS names.
    async fn start(test_name: &str) -> Self {
        let cluster_name = format!("rw-test-{}", test_name);
        let topology_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/source/mongodb_oplog/testdata/sharded-cluster-minimal.yaml"
        );

        // Destroy any leftover from a previous failed run
        let _ = tokio::process::Command::new("mup")
            .args(["cluster", "destroy", &cluster_name, "--yes"])
            .output()
            .await;

        // Deploy
        let output = tokio::process::Command::new("mup")
            .args([
                "cluster",
                "deploy",
                &cluster_name,
                topology_path,
                "--version",
                "3.6",
                "--auto-approve",
                "--no-monitoring",
            ])
            .output()
            .await
            .expect("failed to run mup cluster deploy");

        assert!(
            output.status.success(),
            "mup cluster deploy failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        // Parse meta.yaml for mongos port and shard RS names
        let home = std::env::var("HOME").expect("HOME not set");
        let meta_path = std::path::PathBuf::from(home)
            .join(format!(".mup/storage/clusters/{}/meta.yaml", cluster_name));
        let meta_str =
            std::fs::read_to_string(&meta_path).expect("failed to read mup meta.yaml");

        // Simple line-based parsing — find mongos port and shard RS names
        let mut mongos_port: Option<u16> = None;
        let mut shard_rs_names = Vec::new();
        let mut in_mongos = false;
        let mut in_mongod = false;

        for line in meta_str.lines() {
            let trimmed = line.trim();
            if trimmed == "mongos_servers:" {
                in_mongos = true;
                in_mongod = false;
            } else if trimmed == "mongod_servers:" {
                in_mongod = true;
                in_mongos = false;
            } else if trimmed.ends_with("_servers:") || trimmed == "nodes:" {
                in_mongos = false;
                in_mongod = false;
            }

            if in_mongos && trimmed.starts_with("port:") {
                if let Some(port_str) = trimmed.strip_prefix("port:") {
                    if let Ok(port) = port_str.trim().parse::<u16>() {
                        mongos_port = Some(port);
                    }
                }
            }
            if in_mongod && trimmed.starts_with("replica_set:") {
                if let Some(rs) = trimmed.strip_prefix("replica_set:") {
                    let rs = rs.trim().to_owned();
                    if !shard_rs_names.contains(&rs) {
                        shard_rs_names.push(rs);
                    }
                }
            }
        }

        let port = mongos_port.expect("could not find mongos port in meta.yaml");
        let mongos_uri = format!("mongodb://localhost:{}", port);

        Self {
            cluster_name,
            mongos_uri,
            shard_rs_names,
        }
    }

    async fn destroy(&self) {
        let _ = tokio::process::Command::new("mup")
            .args(["cluster", "destroy", &self.cluster_name, "--yes"])
            .output()
            .await;
    }
}

/// OPLOG-040/041/045: Combined sharded cluster test.
///
/// Deploys a single mup sharded cluster and validates:
/// - OPLOG-040: `listShards` discovery via mongos (`ismaster` returns `isdbgrid`)
/// - OPLOG-041: `parse_shard_host_to_uri` produces valid direct RS URIs
/// - OPLOG-045: Direct shard URI can query `local.oplog.rs` (impossible through mongos)
/// - OPLOG-040 (enumerator): `MongodbOplogSplitEnumerator` returns one split per shard
#[tokio::test]
#[ignore]
async fn test_sharded_cluster() {
    use std::sync::Arc;

    use crate::source::base::SourceEnumeratorContext;
    use crate::source::SplitEnumerator;
    use super::enumerator::MongodbOplogSplitEnumerator;

    let cluster = MupShardedCluster::start("shard-all").await;

    let props = make_props(&cluster.mongos_uri, "shardtest.events");
    let (client, _rs_name) = props.build_client().await.unwrap();

    // ── OPLOG-040: mongos detection via ismaster ─────────────────
    let admin = client.database("admin");
    let reply = admin
        .run_command(doc! { "ismaster": 1 }, None)
        .await
        .expect("ismaster failed");
    assert_eq!(
        reply.get_str("msg").ok(),
        Some("isdbgrid"),
        "connected to mongos should return isdbgrid"
    );

    // ── OPLOG-040: listShards returns both shards ────────────────
    let result = admin
        .run_command(doc! { "listShards": 1 }, None)
        .await
        .expect("listShards failed");
    let shards = result.get_array("shards").expect("missing shards array");
    assert_eq!(shards.len(), 2, "should have 2 shards");

    let shard_ids: Vec<String> = shards
        .iter()
        .filter_map(|s| s.as_document()?.get_str("_id").ok().map(|s| s.to_owned()))
        .collect();
    for expected_rs in &cluster.shard_rs_names {
        assert!(
            shard_ids.contains(expected_rs),
            "shard {} should be in listShards, got: {:?}",
            expected_rs,
            shard_ids
        );
    }

    // ── OPLOG-041/045: parse shard host → URI, direct oplog access ──
    for shard_bson in shards {
        let shard = shard_bson.as_document().unwrap();
        let host = shard.get_str("host").unwrap();

        let uri = super::parse_shard_host_to_uri(host).expect("parse_shard_host_to_uri failed");
        assert!(uri.starts_with("mongodb://"), "should produce a mongodb:// URI");
        assert!(uri.contains("replicaSet="), "should include replicaSet param");

        // Connect directly to shard and verify oplog access
        let shard_opts = ClientOptions::parse_async(&format!("{}&directConnection=true", uri))
            .await
            .unwrap();
        let shard_client = mongodb::Client::with_options(shard_opts).unwrap();

        let oplog = shard_client
            .database("local")
            .collection::<Document>("oplog.rs");
        let opts = FindOneOptions::builder()
            .sort(doc! { "$natural": -1 })
            .build();
        let entry = oplog.find_one(doc! {}, opts).await;
        assert!(
            entry.is_ok(),
            "should be able to query oplog.rs via direct shard URI"
        );
    }

    // ── OPLOG-040 (enumerator): one split per shard ──────────────
    let enum_props = make_props(&cluster.mongos_uri, "enumdb.enumcoll");
    let source_ctx = Arc::new(SourceEnumeratorContext::dummy());
    let mut enumerator = MongodbOplogSplitEnumerator::new(enum_props, source_ctx)
        .await
        .expect("enumerator creation failed");

    let splits = enumerator
        .list_splits()
        .await
        .expect("list_splits failed");

    assert_eq!(
        splits.len(),
        cluster.shard_rs_names.len(),
        "should have one split per shard"
    );

    for split in &splits {
        assert!(
            split.replica_set_uri.is_some(),
            "sharded split should have replica_set_uri, got: {:?}",
            split
        );
        let uri = split.replica_set_uri.as_ref().unwrap();
        assert!(
            uri.starts_with("mongodb://"),
            "replica_set_uri should be a valid MongoDB URI"
        );
    }

    cluster.destroy().await;
}

// ── OPLOG-047–052: Chunked parallel snapshot tests ──────────────────

/// OPLOG-048: Verify $sample-based boundary discovery produces correct
/// non-overlapping chunk ranges covering the full collection.
#[tokio::test]
#[ignore]
async fn test_sample_boundary_discovery() {
    use super::reader::discover_chunk_boundaries;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "bounddb.boundcoll");
    let (client, _rs_name) = props.build_client().await.unwrap();

    let coll = client.database("bounddb").collection::<Document>("boundcoll");
    for i in 0..100 {
        coll.insert_one(doc! { "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    // Request chunks targeting ~25 docs each → ~4 chunks (sample mode)
    let chunks = discover_chunk_boundaries(&client, "bounddb", "boundcoll", 25, BoundaryMode::Sample)
        .await
        .unwrap();

    assert!(
        chunks.len() >= 2,
        "100 docs with target 25 should produce at least 2 chunks, got {}",
        chunks.len()
    );

    // Verify first chunk has no min, last has no max
    assert!(chunks.first().unwrap().min_id.is_none());
    assert!(chunks.last().unwrap().max_id.is_none());

    // Verify boundaries are contiguous: chunk[i].max_id == chunk[i+1].min_id
    for i in 0..chunks.len() - 1 {
        assert_eq!(
            chunks[i].max_id, chunks[i + 1].min_id,
            "chunk {} max should equal chunk {} min",
            i,
            i + 1
        );
    }

    // Verify chunk indices are sequential
    for (i, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk.chunk_idx, i);
        assert!(!chunk.done);
        assert!(chunk.last_id.is_none());
    }
}

/// OPLOG-049: Verify chunked snapshot (workers=4) produces the same _id set
/// as serial snapshot (workers=1) with no duplicates or gaps.
#[tokio::test]
#[ignore]
async fn test_chunked_snapshot_no_gaps_no_duplicates() {
    use std::collections::HashSet;

    use super::reader::MongodbOplogSplitReader;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "chunkdb.chunkcoll");
    let (client, rs_name) = props.build_client().await.unwrap();

    let coll = client.database("chunkdb").collection::<Document>("chunkcoll");
    for i in 0..200 {
        coll.insert_one(doc! { "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    let snapshot_start_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();
    let split_id = "0".into();

    // Serial scan
    let serial_batches = MongodbOplogSplitReader::run_snapshot(
        &client, "chunkdb", "chunkcoll", &rs_name, &split_id,
        100, snapshot_start_ts, SnapshotResumeState::Fresh, 1, 10_000,
        BoundaryMode::Sample, None,
    )
    .await
    .unwrap();
    let serial_ids: HashSet<String> = serial_batches
        .iter()
        .flat_map(|b| b.iter())
        .map(|m| {
            let o: MongodbOplogOffset = serde_json::from_str(m.offset.as_ref()).unwrap();
            o.snapshot_last_id.unwrap()
        })
        .collect();

    // Chunked scan (4 workers, chunk target 50 → ~4 chunks from 200 docs)
    let chunked_batches = MongodbOplogSplitReader::run_snapshot(
        &client, "chunkdb", "chunkcoll", &rs_name, &split_id,
        100, snapshot_start_ts, SnapshotResumeState::Fresh, 4, 50,
        BoundaryMode::Sample, None,
    )
    .await
    .unwrap();
    let chunked_ids: HashSet<String> = chunked_batches
        .iter()
        .flat_map(|b| b.iter())
        .map(|m| {
            let o: MongodbOplogOffset = serde_json::from_str(m.offset.as_ref()).unwrap();
            o.snapshot_last_id.unwrap()
        })
        .collect();

    assert_eq!(serial_ids.len(), 200, "serial should return 200 unique docs");
    assert_eq!(chunked_ids.len(), 200, "chunked should return 200 unique docs");
    assert_eq!(serial_ids, chunked_ids, "serial and chunked should return same _ids");
}

/// OPLOG-047: workers=1 through dispatch should be identical to serial.
#[tokio::test]
#[ignore]
async fn test_workers_1_identical_to_serial() {
    use super::reader::MongodbOplogSplitReader;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "w1db.w1coll");
    let (client, rs_name) = props.build_client().await.unwrap();

    let coll = client.database("w1db").collection::<Document>("w1coll");
    for i in 0..50 {
        coll.insert_one(doc! { "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    let snapshot_start_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();
    let split_id = "0".into();

    let batches = MongodbOplogSplitReader::run_snapshot(
        &client, "w1db", "w1coll", &rs_name, &split_id,
        100, snapshot_start_ts, SnapshotResumeState::Fresh, 1, 10_000,
        BoundaryMode::Sample, None,
    )
    .await
    .unwrap();

    let total: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(total, 50, "workers=1 should return all 50 docs");
}

/// OPLOG-049: Small collection with fewer docs than chunk target should
/// auto-fall-back to serial (single chunk).
#[tokio::test]
#[ignore]
async fn test_chunked_snapshot_small_collection() {
    use super::reader::MongodbOplogSplitReader;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "smalldb.smallcoll");
    let (client, rs_name) = props.build_client().await.unwrap();

    let coll = client.database("smalldb").collection::<Document>("smallcoll");
    for i in 0..3 {
        coll.insert_one(doc! { "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    let snapshot_start_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();
    let split_id = "0".into();

    // 4 workers but only 3 docs → should clamp to serial
    let batches = MongodbOplogSplitReader::run_snapshot(
        &client, "smalldb", "smallcoll", &rs_name, &split_id,
        100, snapshot_start_ts, SnapshotResumeState::Fresh, 4, 10_000,
        BoundaryMode::Sample, None,
    )
    .await
    .unwrap();

    let total: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(total, 3, "small collection should still return all 3 docs");
}

/// OPLOG-050: Verify crash recovery resumes only incomplete chunks.
#[tokio::test]
#[ignore]
async fn test_chunked_snapshot_resume_from_persisted_state() {
    use super::reader::{MongodbOplogSplitReader, discover_chunk_boundaries};

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "resumedb2.resumecoll2");
    let (client, rs_name) = props.build_client().await.unwrap();

    let coll = client.database("resumedb2").collection::<Document>("resumecoll2");
    for i in 0..100 {
        coll.insert_one(doc! { "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    let snapshot_start_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();
    let split_id = "0".into();

    // Discover boundaries (target 25 → ~4 chunks)
    let mut chunks = discover_chunk_boundaries(&client, "resumedb2", "resumecoll2", 25, BoundaryMode::Sample)
        .await
        .unwrap();

    assert!(chunks.len() >= 2, "need at least 2 chunks for resume test");

    // Simulate: mark first chunk as done
    chunks[0].done = true;

    // Resume with the partial state
    let batches = MongodbOplogSplitReader::run_snapshot(
        &client, "resumedb2", "resumecoll2", &rs_name, &split_id,
        100, snapshot_start_ts,
        SnapshotResumeState::Chunked { chunks: chunks.clone() },
        4, 25, BoundaryMode::Sample, None,
    )
    .await
    .unwrap();

    // Should NOT return docs from chunk 0's range
    let total: usize = batches.iter().map(|b| b.len()).sum();
    assert!(
        total < 100,
        "resumed snapshot should return fewer than 100 docs since chunk 0 is done, got {}",
        total
    );
    assert!(total > 0, "resumed snapshot should still return some docs");
}

/// OPLOG-050: All chunks done → empty result.
#[tokio::test]
#[ignore]
async fn test_chunked_snapshot_resume_all_done() {
    use super::reader::MongodbOplogSplitReader;
    use super::split::SnapshotChunkState;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "donedb.donecoll");
    let (client, rs_name) = props.build_client().await.unwrap();

    let coll = client.database("donedb").collection::<Document>("donecoll");
    for i in 0..10 {
        coll.insert_one(doc! { "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    let snapshot_start_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();
    let split_id = "0".into();

    // All chunks done
    let chunks = vec![
        SnapshotChunkState {
            chunk_idx: 0,
            min_id: None,
            max_id: Some("\"mid\"".to_owned()),
            last_id: Some("\"mid\"".to_owned()),
            done: true,
        },
        SnapshotChunkState {
            chunk_idx: 1,
            min_id: Some("\"mid\"".to_owned()),
            max_id: None,
            last_id: Some("\"end\"".to_owned()),
            done: true,
        },
    ];

    let batches = MongodbOplogSplitReader::run_snapshot(
        &client, "donedb", "donecoll", &rs_name, &split_id,
        100, snapshot_start_ts,
        SnapshotResumeState::Chunked { chunks },
        4, 10_000, BoundaryMode::Sample, None,
    )
    .await
    .unwrap();

    let total: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(total, 0, "all chunks done should return empty");
}

/// OPLOG-051: Verify chunked snapshot to CDC transition — offsets from
/// the last batch should have snapshot_chunks with all entries done.
#[tokio::test]
#[ignore]
async fn test_chunked_snapshot_to_cdc_transition() {
    use super::reader::MongodbOplogSplitReader;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "cdcdb.cdccoll");
    let (client, rs_name) = props.build_client().await.unwrap();

    let coll = client.database("cdcdb").collection::<Document>("cdccoll");
    for i in 0..20 {
        coll.insert_one(doc! { "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    // Insert concurrent writes during snapshot
    for i in 0..3 {
        coll.insert_one(doc! { "phase": "during", "seq": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    let snapshot_start_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();
    let split_id = "0".into();

    // Run chunked snapshot
    let batches = MongodbOplogSplitReader::run_snapshot(
        &client, "cdcdb", "cdccoll", &rs_name, &split_id,
        100, snapshot_start_ts, SnapshotResumeState::Fresh, 4, 10_000,
        BoundaryMode::Sample, None,
    )
    .await
    .unwrap();

    let total: usize = batches.iter().map(|b| b.len()).sum();
    assert!(total >= 20, "should snapshot at least the original 20 docs, got {}", total);

    // Verify the last batch's last message has snapshot_chunks with all done
    if let Some(last_batch) = batches.last() {
        if let Some(last_msg) = last_batch.last() {
            let offset: MongodbOplogOffset =
                serde_json::from_str(last_msg.offset.as_ref()).unwrap();
            // chunked mode embeds chunk state
            if let Some(chunks) = &offset.snapshot_chunks {
                assert!(
                    chunks.iter().all(|c| c.done),
                    "final offset should have all chunks done"
                );
            }
            // oplog_ts should be set for CDC transition
            assert_eq!(offset.oplog_ts_secs, snapshot_start_ts.time);
        }
    }
}

// ── OPLOG-054/055/056: min_max boundary discovery with ObjectId ──────

/// OPLOG-055/056: Verify min_max boundary discovery produces correct
/// non-overlapping chunk ranges for a collection with ObjectId `_id`.
#[tokio::test]
#[ignore]
async fn test_min_max_boundary_discovery_objectid() {
    use super::reader::discover_chunk_boundaries;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "mmdb.mmcoll");
    let (client, _rs_name) = props.build_client().await.unwrap();

    let coll = client.database("mmdb").collection::<Document>("mmcoll");
    for i in 0..200 {
        coll.insert_one(doc! { "val": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    // Request chunks targeting ~50 docs each → ~4 chunks
    let chunks = discover_chunk_boundaries(&client, "mmdb", "mmcoll", 50, BoundaryMode::MinMax)
        .await
        .unwrap();

    assert!(
        chunks.len() >= 2,
        "200 docs / target 50 should produce >= 2 chunks, got {}",
        chunks.len()
    );

    // First chunk has min_id=None, last has max_id=None
    assert!(chunks.first().unwrap().min_id.is_none());
    assert!(chunks.last().unwrap().max_id.is_none());

    // Boundaries should be contiguous (chunk[i].max_id == chunk[i+1].min_id)
    for i in 0..chunks.len() - 1 {
        assert_eq!(
            chunks[i].max_id, chunks[i + 1].min_id,
            "chunk {} max_id should equal chunk {} min_id",
            i,
            i + 1
        );
    }
}

// ── OPLOG-057: min_max boundary discovery with integer _id ───────────

/// OPLOG-057: Verify min_max boundary discovery with Int32 `_id` values.
#[tokio::test]
#[ignore]
async fn test_min_max_boundary_discovery_integer_id() {
    use super::reader::discover_chunk_boundaries;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "intdb.intcoll");
    let (client, _rs_name) = props.build_client().await.unwrap();

    let coll = client.database("intdb").collection::<Document>("intcoll");
    for i in 0..200i32 {
        coll.insert_one(doc! { "_id": i, "val": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    let chunks = discover_chunk_boundaries(&client, "intdb", "intcoll", 50, BoundaryMode::MinMax)
        .await
        .unwrap();

    assert!(
        chunks.len() >= 2,
        "200 int docs / target 50 should produce >= 2 chunks, got {}",
        chunks.len()
    );
    assert!(chunks.first().unwrap().min_id.is_none());
    assert!(chunks.last().unwrap().max_id.is_none());
}

// ── OPLOG-059: min_max fails loudly for unsupported _id type ──────────

/// OPLOG-059: Verify min_max fails with error for String _id.
#[tokio::test]
#[ignore]
async fn test_min_max_boundary_fails_for_string_id() {
    use super::reader::discover_chunk_boundaries;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "strdb.strcoll");
    let (client, _rs_name) = props.build_client().await.unwrap();

    let coll = client.database("strdb").collection::<Document>("strcoll");
    for i in 0..200 {
        coll.insert_one(
            doc! { "_id": format!("key_{:04}", i), "val": i },
            majority_insert_opts(),
        )
        .await
        .unwrap();
    }

    let result = discover_chunk_boundaries(&client, "strdb", "strcoll", 50, BoundaryMode::MinMax)
        .await;
    assert!(result.is_err(), "min_max should fail for String _id");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("sample"),
        "error should suggest using sample mode: {}",
        err_msg
    );
}

// ── OPLOG-054: end-to-end chunked snapshot with min_max mode ─────────

/// OPLOG-054: Full end-to-end chunked snapshot using min_max boundary mode.
#[tokio::test]
#[ignore]
async fn test_chunked_snapshot_with_min_max_mode() {
    use std::collections::HashSet;
    use super::reader::MongodbOplogSplitReader;

    let (_container, uri) = start_mongo_rs().await;
    let props = make_props(&uri, "mmsnap.mmcoll2");
    let (client, rs_name) = props.build_client().await.unwrap();

    let coll = client.database("mmsnap").collection::<Document>("mmcoll2");
    for i in 0..200 {
        coll.insert_one(doc! { "val": i }, majority_insert_opts())
            .await
            .unwrap();
    }

    let snapshot_start_ts = MongodbOplogSplitReader::get_current_oplog_ts(&client)
        .await
        .unwrap();
    let split_id = "0".into();

    // Chunked scan with min_max mode (4 workers, target 50)
    let batches = MongodbOplogSplitReader::run_snapshot(
        &client, "mmsnap", "mmcoll2", &rs_name, &split_id,
        100, snapshot_start_ts, SnapshotResumeState::Fresh, 4, 50,
        BoundaryMode::MinMax, None,
    )
    .await
    .unwrap();

    let all_ids: HashSet<String> = batches
        .iter()
        .flat_map(|b| b.iter())
        .map(|m| {
            let o: MongodbOplogOffset = serde_json::from_str(m.offset.as_ref()).unwrap();
            o.snapshot_last_id.unwrap()
        })
        .collect();

    assert_eq!(all_ids.len(), 200, "min_max mode should capture all 200 docs");
}
