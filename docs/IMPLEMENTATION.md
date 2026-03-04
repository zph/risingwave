# Implementation Status

## mongo-oplog Connector

**Status**: In Progress
**Spec**: `docs/specs/mongodb-oplog-tailer-requirements.md`

### Overview

Native RisingWave source connector that tails MongoDB's `local.oplog.rs` capped collection using the High-Watermark Tailer pattern. Replaces the need for Debezium JNI for MongoDB CDC.

- **Minimum MongoDB version**: 3.6 (using `mongodb` Rust driver v2.8.2)
- **Read preference**: `secondaryPreferred` (default when URI has no `readPreference`), configurable via URI
- **Message format**: Debezium-compatible JSON (reuses `DebeziumMongoJsonParser`)
- **Sharded clusters**: Automatic shard discovery via mongos (`listShards`); one split per shard

### Architecture

```
MongodbOplogSplitReader
├── Snapshot Phase (on first start)
│   └── collection.find({}).sort({_id: 1}) → SourceMessage yield
├── WatermarkPollTask (tokio::spawn)
│   └── replSetGetStatus every 100ms → Arc<AtomicU64>
└── CDC Phase (oplog tailing)
    └── tailable cursor on local.oplog.rs → buffer → watermark gate → yield
```

### SQL DDL

```sql
CREATE SOURCE mongo_events WITH (
    connector = 'mongo-oplog',
    mongodb.url = 'mongodb://user:pass@host1:27017/?replicaSet=rs0&readPreference=secondaryPreferred',
    mongodb.namespace = 'mydb.mycollection'
) FORMAT DEBEZIUM_MONGO ENCODE JSON;
```

### Files

| File | Purpose |
|:-----|:--------|
| `src/connector/src/source/mongodb_oplog/mod.rs` | Properties, config, connector registration |
| `src/connector/src/source/mongodb_oplog/split.rs` | Split metadata + offset persistence |
| `src/connector/src/source/mongodb_oplog/enumerator.rs` | Split enumeration (single-RS or sharded auto-discovery) |
| `src/connector/src/source/mongodb_oplog/reader.rs` | Core reader: snapshot + oplog tailing |
| `src/connector/src/source/mongodb_oplog/message.rs` | Oplog → Debezium JSON serialization |
| `src/connector/src/source/mongodb_oplog/oplog_types.rs` | Constants, offset types, watermark encoding |

### Modified Files

| File | Change |
|:-----|:-------|
| `src/connector/src/macros.rs` | Register `MongodbOplog` in `for_all_classified_sources!` |
| `src/connector/src/source/mod.rs` | Add `pub mod mongodb_oplog` + re-export |
| `src/frontend/src/handler/create_source.rs` | Import `MONGO_OPLOG_CONNECTOR` |
| `src/frontend/src/handler/create_source/validate.rs` | Add format compatibility entry |

### Configuration Parameters

| Parameter | Default | Description |
|:----------|:--------|:------------|
| `mongodb.url` | (required) | MongoDB connection URI |
| `mongodb.namespace` | (required) | `db.collection` to capture |
| `mongodb.watermark.poll_interval_ms` | `100` | Watermark poll interval |
| `mongodb.buffer.max_bytes` | `104857600` | Max buffer size (100MB) |
| `mongodb.watermark.stall_timeout_secs` | `30` | Stall warning timeout |
| `mongodb.heartbeat.interval_secs` | `60` | Heartbeat interval for idle |
| `mongodb.snapshot.batch_size` | `1024` | Snapshot batch size |
| `mongodb.shard.discovery.interval_secs` | `30` | Shard discovery polling interval (mongos only) |
| `scan.startup.mode` | `snapshot` | `snapshot`, `latest`, or `earliest` |

### Read Preference Configuration

Read preference controls which MongoDB replica set member the connector reads from (OPLOG-037).

**Default:** `secondaryPreferred` — applied programmatically when the URI does not include a `readPreference` option. This offloads read load from the Primary while falling back to it when no Secondary is available.

**Configuration:** Set read preference via standard MongoDB URI options in `mongodb.url`:

```
mongodb://user:pass@host1:27017/?replicaSet=rs0&readPreference=secondary
```

**Tag sets** for targeting specific members (e.g., analytics nodes in a specific data center):

```
mongodb://user:pass@host1:27017/?replicaSet=rs0&readPreference=secondary&readPreferenceTags=dc:us-east,workload:analytics
```

See [MongoDB Connection String URI — Read Preference Options](https://www.mongodb.com/docs/manual/reference/connection-string/#read-preference-options) for full details.

### EARS Spec Tag Coverage

Spec tags `OPLOG-001` through `OPLOG-046` are defined in the requirements doc.
Code references the relevant spec tag in comments (e.g., `// OPLOG-006: durability invariant`).

### MongoDB Server Version Compatibility

| Rust Driver Version | Min MongoDB Server | Notes |
|:----|:----|:----|
| 1.x (EOL) | 3.6 | End of life |
| **2.x** | **3.6** | **Current in RisingWave (v2.8.2)** |
| 3.0 – 3.2 | 4.0 | Newer API, drops 3.6 support |
| 3.3+ | 4.2 | 4.0 support removed |

Sources: [Compatibility](https://www.mongodb.com/docs/drivers/rust/current/compatibility/), [Releases](https://github.com/mongodb/mongo-rust-driver/releases)

### Implementation Phases

- [x] Phase 0: Update EARS spec for native integration + MongoDB v4.0+ support
- [x] Phase 1: Skeleton module structure + registration
- [x] Phase 2: Properties + Split + Enumerator with tests (19 unit tests passing)
- [x] Phase 3: Message serialization (oplog → Debezium JSON)
- [x] Phase 4: Core reader (high-watermark tailer + snapshot)
- [x] Phase 5: Mongos shard auto-discovery (OPLOG-040 through OPLOG-046)
- [x] Phase 6: Integration tests (48 unit + 11 integration)
  - Single-RS tests via testcontainers (mongo:3.6)
  - Sharded cluster tests via mup (OPLOG-040/041/045)
  - Buffer/watermark invariant unit tests (OPLOG-006/007/008a/008b)
  - BSON type coverage (OPLOG-027)
  - Startup modes (OPLOG-036) and snapshot→CDC transition (OPLOG-035)

- [ ] Phase 7: E2E test — mongo-oplog source → S3 Parquet sink via MinIO
  - Python E2E script (`e2e_test/s3/mongo_oplog_parquet_sink.py`)
  - Seeds MongoDB via pymongo, creates RW source/MV/sink pipeline
  - Verifies .parquet files appear in MinIO bucket
  - Reads parquet back via S3 source table, asserts row count >= 20
  - Makefile target: `make test-e2e-mongo-parquet`

### Future Improvements

- **Batched read-back**: Collect update `_id` values and issue batched `find({_id: {$in: [...]}})` every 100ms or at max batch size (1024), instead of individual `findOne` per update
- **MongoDB 3.6 support**: Implemented — using `mongodb` Rust driver v2.8.2 which supports server 3.6+
- **Change Stream pre/post images**: Use MongoDB 6.0+ `changeStreamPreAndPostImages` to avoid read-back
