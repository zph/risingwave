# MongoDB Oplog High-Watermark Tailer Requirements

## Overview

This document specifies the functional and non-functional requirements for the MongoDB Oplog High-Watermark Tailer, a **native RisingWave source connector** (`mongo-oplog`) that provides CDC (Change Data Capture) from MongoDB. Instead of relying on MongoDB Change Streams (the current Debezium-based approach via JNI), this connector tails the `local.oplog.rs` collection directly, buffers entries, and releases only majority-committed (durable) entries as `SourceMessage` values for parsing by `DebeziumMongoJsonParser`.

State persistence is handled by RisingWave's split checkpoint mechanism. Kafka is not in the data path.

**System Name:** Oplog Tailer
**Version:** 2.0
**Last Updated:** 2026-03-03
**Spec Tag Prefix:** OPLOG
**Minimum MongoDB Version:** 3.6 (using `mongodb` Rust driver v2.8.2 which supports server 3.6 through 7.0+)

## Terminology

| Term | Definition |
|:-----|:-----------|
| Oplog | MongoDB's `local.oplog.rs` capped collection that records all write operations on replica set members |
| Majority Commit Point | The timestamp (`lastCommittedOpTime`) of the latest operation acknowledged by a majority of replica set members |
| High-Watermark | The `lastCommittedOpTime` value used as the release threshold for buffered entries |
| Durable Entry | An oplog entry whose timestamp `t_entry <= t_majority_commit` |
| Dirty Entry | An oplog entry not yet acknowledged by a majority; may be rolled back |
| Topology Change | A replica set election, step-down, or member reconfiguration event |
| Rollback | When a former Primary's uncommitted writes are discarded after a failover |
| Split State | RisingWave's checkpoint-persisted offset state for a source split |
| Snapshot | Full-collection scan performed on first start to capture existing data before oplog tailing |

## Architecture

```
+--------------------+       +---------------------------------------------------+       +------------+
|                    |       |       RisingWave Source Executor                    |       |            |
| MongoDB Replica    |       | MongodbOplogSplitReader                            |       | RisingWave |
| Set Member         | ----> |   OplogTailCursor -> Buffer -> SourceMessage yield  | ----> | Stream     |
| (default:Secondary)|       |        ^                                           |       | Engine     |
|  local.oplog.rs    |       |        | WatermarkPollTask                         |       |            |
+--------------------+       |        v                                           |       +------------+
                             |   Split State (checkpoint via RW barriers)         |
                             +---------------------------------------------------+
```

### Components

| Component | Responsibility |
|:----------|:---------------|
| Oplog Reader | Tails `local.oplog.rs` on a replica set member using a tailable cursor |
| Majority Poller | Periodically queries `replSetGetStatus` to obtain `lastCommittedOpTime` |
| Buffer Manager | Holds oplog entries ordered by timestamp until they become durable |
| Message Serializer | Converts durable entries to Debezium-compatible JSON `SourceMessage` values |
| Split State | Offset persisted via RisingWave barrier checkpoints for crash recovery |
| Rollback Detector | Monitors for topology changes and oplog discontinuities |
| Snapshot Scanner | Full-collection scan on first start to capture pre-existing data |

## Requirements

### 1. Oplog Tailing

**OPLOG-001:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL connect to a MongoDB replica set member (default: Secondary, configurable via read preference) and open a tailable cursor on the `local.oplog.rs` collection. The connector requires MongoDB v3.6+ using the Rust `mongodb` driver v2.8.2. The oplog tailing approach relies only on `local.oplog.rs` and `replSetGetStatus`, both available since MongoDB 2.x.

**Rationale:**
Every replica set member maintains its own `local.oplog.rs`. Tailing a Secondary offloads read load from the Primary. A tailable cursor provides a continuous stream of new entries without polling. Entries only appear on a Secondary after replication, and the watermark gate (OPLOG-006) further ensures majority-commit durability.

**Verification:**
Unit test: verify cursor creation against a test replica set. Integration test: confirm entries arrive via cursor after writes.

---

**OPLOG-002:** Event Driven

**Requirement:**
WHEN a new entry appears in `local.oplog.rs`, the Oplog Tailer SHALL read the entry and append it to the internal buffer ordered by the entry's `ts` (timestamp) field.

**Rationale:**
Every oplog entry MUST be captured and held for durability evaluation before release.

**Verification:**
Integration test: insert documents into MongoDB, verify corresponding oplog entries appear in buffer with correct ordering.

---

**OPLOG-003:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL support filtering oplog entries by namespace (`ns` field) to capture only entries belonging to configured database(s) and collection(s).

**Rationale:**
Users typically want CDC from specific collections, not the entire replica set's write traffic.

**Verification:**
Unit test: supply namespace filter config, verify only matching entries pass through.

---

### 2. Majority Commit Point Tracking

**OPLOG-004:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL poll the connected replica set member's `replSetGetStatus` command at a configurable interval (default: 100ms) and extract `optimes.lastCommittedOpTime.ts` as the current high-watermark. The `replSetGetStatus` command is available from any replica set member (Primary or Secondary).

**Rationale:**
The `lastCommittedOpTime` is the definitive indicator of which operations are durable across a majority of nodes. It is available from any member, not only the Primary.

**Verification:**
Unit test: mock `replSetGetStatus` response, verify correct extraction. Integration test: confirm value updates after writes are majority-acknowledged.

---

**OPLOG-005:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL expose the current high-watermark timestamp via RisingWave source metrics infrastructure.

**Rationale:**
Operators need visibility into the lag between the oplog tail and the majority commit point.

**Verification:**
Integration test: verify metric reports a non-zero timestamp after startup.

---

### 3. Durability Invariant and Buffer Release

**OPLOG-006:** State Driven

**Requirement:**
WHILE the buffer contains entries, the Oplog Tailer SHALL release an entry as a `SourceMessage` only WHEN `t_entry <= t_majority_commit`.

**Rationale:**
This is the core safety invariant. Releasing entries before majority acknowledgement risks sending data that MongoDB will roll back, creating a consistency gap in RisingWave materialized views.

**Verification:**
Unit test: populate buffer with entries at various timestamps, set high-watermark, verify only entries at or below the watermark are released. Property-based test: for any sequence of entries and watermark advances, no entry with `t_entry > t_majority_commit` is ever released.

---

**OPLOG-007:** Event Driven

**Requirement:**
WHEN the high-watermark advances, the Oplog Tailer SHALL scan the buffer and release all entries with `t_entry <= t_majority_commit` in timestamp order.

**Rationale:**
Batch release on watermark advance is more efficient than checking per-entry arrival. Timestamp ordering preserves MongoDB's causal consistency.

**Verification:**
Unit test: advance watermark multiple times, verify entries release in strict `ts` order.

---

**OPLOG-008:** State Driven

**Requirement:**
WHILE the in-memory buffer size exceeds the configurable maximum (default: 100MB), the Oplog Tailer SHALL block the oplog cursor read loop and resume reading immediately WHEN buffer space becomes available after entries are flushed.

**Rationale:**
Unbounded buffering risks OOM during periods of high write throughput or slow majority acknowledgement. The backpressure mechanism MUST be a blocking wait (not exponential backoff) because the oplog is a capped collection -- backing off too aggressively risks the oplog wrapping past our cursor position, causing permanent data loss. Immediate resume on flush minimizes the window of cursor staleness.

**Verification:**
Unit test: fill buffer to limit, verify cursor read blocks. Advance watermark to flush entries, verify cursor resumes within one poll cycle. Measure that no entries are dropped.

---

**OPLOG-008a:** State Driven

**Requirement:**
WHILE the buffer is full and the high-watermark has not advanced for longer than a configurable stall timeout (default: 30 seconds), the Oplog Tailer SHALL log a structured warning at WARN level every stall timeout interval containing the current buffer size, the stalled watermark timestamp, and the oplog cursor position.

**Rationale:**
A stalled majority commit point (e.g., replica set lost quorum) combined with a full buffer is a critical operational state. Operators need clear alerting to intervene before the oplog wraps past the cursor.

**Verification:**
Unit test: hold watermark fixed while buffer is full, verify log emitted after stall timeout. Verify repeated warnings at each interval.

---

**OPLOG-008b:** Unwanted Behaviour

**Requirement:**
IF the gap between the oplog cursor position and the oplog head exceeds 80% of the oplog window size (estimated from `replSetGetStatus`'s `optimes` range), THEN the Oplog Tailer SHALL log a structured error at ERROR level.

**Rationale:**
The oplog is a capped collection with finite size. If the cursor falls too far behind the head (because the buffer is full and the watermark is stalled), the oplog will overwrite entries the cursor hasn't yet read. This is an unrecoverable data loss scenario that requires operator intervention (scale the oplog, restore quorum, or accept a gap). The 80% threshold gives advance warning before the wrap occurs.

**Verification:**
Unit test: simulate oplog cursor near wrap condition, verify error log. Integration test: create a small oplog, write enough data to approach wrap, verify alert fires.

---

**OPLOG-008c:** Unwanted Behaviour

**Requirement:**
IF the oplog wraps past the Oplog Tailer's cursor position (detected by the cursor returning `CursorNotFound` or the resume timestamp no longer existing in the oplog), THEN the Oplog Tailer SHALL:
1. Log a structured error at ERROR level indicating data loss
2. Clear the buffer
3. Resume tailing from the oldest available oplog entry
4. Update the split state with the new position

**Rationale:**
Once the oplog wraps past the cursor, the missed entries are permanently lost. The tailer must recover to a consistent forward position. There is no safe alternative -- the data is gone from MongoDB.

**Verification:**
Integration test: configure a small oplog, stall the tailer, write enough data to force wrap, verify tailer recovers from oldest available entry and logs fire.

---

### 4. Message Serialization

**OPLOG-009:** Event Driven

**Requirement:**
WHEN a durable entry is released from the buffer, the Oplog Tailer SHALL serialize it as a Debezium-compatible MongoDB CDC JSON `SourceMessage` (key + payload bytes) for consumption by `DebeziumMongoJsonParser`.

**Rationale:**
Debezium-compatible format enables RisingWave to parse messages using the existing `DebeziumMongoJsonParser` without any changes to RisingWave core.

**Verification:**
Contract test: produce entry via tailer, parse with `DebeziumMongoJsonParser`, verify correct row data.

---

**OPLOG-010:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL produce `SourceMessage` keys containing the document `_id` field in BSON Extended JSON format, consistent with Debezium MongoDB connector key format: `{"schema":null,"payload":{"id":"<extended_json_id>"}}`.

**Rationale:**
`DebeziumMongoJsonParser` expects Debezium-style keys for correct upsert and delete semantics.

**Verification:**
Unit test: verify serialized key format matches Debezium reference output for ObjectId, string, and composite `_id` values.

---

**OPLOG-011:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL map oplog operation types to Debezium operation codes as follows: `i` (insert) to `c` (create), `u` (update) to `u` (update), `d` (delete) to `d` (delete).

**Rationale:**
Correct operation mapping is required for RisingWave to apply inserts, updates, and deletes to materialized views.

**Verification:**
Unit test: for each oplog op type, verify the emitted Debezium message contains the correct `op` field.

---

**OPLOG-012:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL include the `source` metadata block in each Debezium message containing at minimum: `ts_ms` (source timestamp in milliseconds), `db` (database name), `collection` (collection name), and `rs` (replica set name).

**Rationale:**
Source metadata is consumed by RisingWave for watermark computation and provenance tracking.

**Verification:**
Unit test: verify presence and correctness of all required `source` fields in serialized messages.

---

**OPLOG-013:** *Removed — N/A for native connector (no Kafka in data path).*

---

### 5. State Persistence and Crash Recovery

**OPLOG-014:** Event Driven

**Requirement:**
WHEN RisingWave issues a barrier checkpoint, the Oplog Tailer SHALL persist the current oplog offset (maximum `ts` value of yielded entries) via the split state checkpoint mechanism.

**Rationale:**
Persisting the offset enables crash recovery without replaying the entire oplog.

**Verification:**
Unit test: after yielding entries, verify split state contains correct timestamp. Integration test: restart source, verify it resumes from persisted position.

---

**OPLOG-015:** Event Driven

**Requirement:**
WHEN the Oplog Tailer starts and a split state checkpoint exists, it SHALL read the last persisted oplog timestamp from the split state and begin tailing the oplog from that timestamp (exclusive).

**Rationale:**
Resume-from-checkpoint avoids duplicate processing of entries already yielded downstream.

**Verification:**
Integration test: write entries, checkpoint, restart tailer, write more entries, verify only new entries appear downstream.

---

**OPLOG-016:** *Removed — RisingWave's checkpoint mechanism handles state persistence; no separate state store backends needed.*

---

### 6. Rollback and Topology Change Handling

**OPLOG-017:** Event Driven

**Requirement:**
WHEN the Oplog Tailer detects that the target replica set member becomes unreachable or leaves the replica set (via connection error or topology notification), the Oplog Tailer SHALL discard all buffered entries, reconnect to a member matching the configured read preference, and resume tailing.

**Rationale:**
After a failover, buffered entries may include dirty writes. Discarding them is safe because they were never released (OPLOG-006 guarantees only durable entries leave the buffer).

**Verification:**
Integration test: trigger replica set election during tailing, verify buffer is cleared and new connection established.

---

**OPLOG-018:** Event Driven

**Requirement:**
WHEN the Oplog Tailer detects an oplog discontinuity (the new oplog position is older than the buffer's oldest entry), the Oplog Tailer SHALL clear the buffer, log a warning, and restart tailing from the current oplog position.

**Rationale:**
An oplog discontinuity signals a rollback or oplog truncation. Continuing from a stale position would miss or duplicate entries.

**Verification:**
Unit test: simulate discontinuity by injecting a backwards timestamp, verify buffer clear and restart behavior.

---

**OPLOG-019:** Event Driven

**Requirement:**
WHEN a rollback is detected, the Oplog Tailer SHALL log a structured WARN containing the former connection address and the discarded buffer size.

**Rationale:**
Rollbacks are operationally significant events. Operators need visibility for incident response.

**Verification:**
Unit test: trigger rollback path, verify log output.

---

### 7. Configuration

**OPLOG-020:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL accept configuration via the SQL `CREATE SOURCE ... WITH (...)` clause, with the following required parameters: `mongodb.url` (connection URI) and `mongodb.namespace` (database.collection filter).

**Rationale:**
Native RisingWave connectors are configured via SQL DDL `WITH` clause properties.

**Verification:**
Unit test: parse configuration from `BTreeMap`. Integration test: create source via DDL with required parameters.

---

**OPLOG-021:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL accept the following optional configuration parameters via SQL `WITH` clause keys with `mongodb.*` prefix and their defaults:

| Parameter | Default | Description |
|:----------|:--------|:------------|
| `mongodb.watermark.poll_interval_ms` | 100 | Interval (ms) between `replSetGetStatus` polls |
| `mongodb.buffer.max_bytes` | 104857600 (100MB) | Maximum in-memory buffer size in bytes |
| `mongodb.watermark.stall_timeout_secs` | 30 | Duration of stalled watermark before warning (OPLOG-008a) |
| `mongodb.heartbeat.interval_secs` | 60 | Heartbeat emission interval for idle periods |
| `mongodb.snapshot.batch_size` | 1024 | Number of documents per batch during snapshot scan |
| `mongodb.shard.discovery.interval_secs` | 30 | Shard discovery polling interval in seconds (mongos mode only, OPLOG-046) |
| `scan.startup.mode` | `snapshot` | Startup mode: `snapshot`, `latest`, `earliest` |

**Rationale:**
Tunable parameters allow operators to balance latency, throughput, and resource usage for their deployment.

**Verification:**
Unit test: verify each default is applied when not specified. Verify overrides take effect.

---

### 8. Observability

**OPLOG-022:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL report metrics via the RisingWave `SourceMetrics` infrastructure, including at minimum:

| Metric | Type | Description |
|:-------|:-----|:------------|
| `oplog_entries_read_total` | Counter | Total oplog entries read |
| `oplog_entries_released_total` | Counter | Total entries released downstream |
| `oplog_entries_discarded_total` | Counter | Total entries discarded due to rollback |
| `oplog_buffer_size_bytes` | Gauge | Current buffer size in bytes |
| `oplog_buffer_entries` | Gauge | Current number of buffered entries |
| `oplog_high_watermark_ts` | Gauge | Current majority commit point timestamp |
| `oplog_tail_ts` | Gauge | Timestamp of most recent oplog entry read |
| `oplog_lag_seconds` | Gauge | `tail_ts - high_watermark_ts` in seconds |
| `oplog_rollbacks_total` | Counter | Rollback events detected |
| `oplog_reconnects_total` | Counter | MongoDB reconnection events |

**Rationale:**
Comprehensive metrics are essential for production operation, alerting, and debugging.

**Verification:**
Integration test: run tailer, verify listed metrics are present and update correctly.

---

**OPLOG-023:** *Removed — RisingWave handles structured logging.*

---

### 9. Health and Liveness

**OPLOG-024:** *Removed — no standalone healthz endpoint for native connector; RisingWave source executor lifecycle handles health.*

---

**OPLOG-025:** State Driven

**Requirement:**
WHILE the Oplog Tailer has not yielded any new `SourceMessage` for longer than a configurable heartbeat timeout (default: 60 seconds), the Oplog Tailer SHALL yield an empty `Vec<SourceMessage>` as a CDC heartbeat.

**Rationale:**
RisingWave uses source message timestamps for watermark advancement. Heartbeats prevent watermark stalls during periods of no writes to the monitored collection.

**Verification:**
Integration test: pause writes to MongoDB, verify heartbeat arrives within timeout period.

---

### 10. Debezium Message Compatibility

**OPLOG-026:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL produce `SourceMessage` payloads conforming to the Debezium MongoDB connector envelope schema version 2.x, containing `before` (null for inserts), `after` (full document as a JSON string of BSON Extended JSON v2 canonical format), `source`, `op`, and `ts_ms` fields.

**Rationale:**
RisingWave's `DebeziumMongoJsonParser` expects this exact envelope format. Compatibility avoids any changes to the RisingWave codebase.

**Verification:**
Contract test: compare tailer output against Debezium MongoDB connector output for identical MongoDB operations. Verify both parse identically through `DebeziumMongoJsonParser`.

---

**OPLOG-027:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL handle all BSON types present in the oplog and serialize them to BSON Extended JSON (v2 canonical format) including at minimum: ObjectId, String, Int32, Int64, Double, Decimal128, Boolean, Date, Timestamp, Binary, Array, and embedded Document. This serialization MUST work with MongoDB v3.6 and later.

**Rationale:**
MongoDB documents can contain any BSON type. Incomplete serialization would cause parsing failures in RisingWave.

**Verification:**
Unit test: round-trip each listed BSON type through serialization and verify correct Extended JSON output.

---

### 11. Security

**OPLOG-028:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL support MongoDB authentication via the `mongodb.url` connection string, which supports SCRAM-SHA-256, x.509, and other mechanisms natively in the MongoDB URI format. This is compatible with MongoDB v3.6+ authentication mechanisms.

**Rationale:**
Production MongoDB deployments require authentication. The connection string format supports all standard mechanisms without additional configuration parameters.

**Verification:**
Integration test: connect with SCRAM credentials via URI, verify successful tailing.

---

**OPLOG-029:** *Removed — no Kafka in the data path.*

---

**OPLOG-030:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL enforce that `mongodb.url` is provided as a RisingWave secret (via the `EnforceSecret` trait) and SHALL NOT log connection URIs containing credentials.

**Rationale:**
Credential leakage is a critical security vulnerability.

**Verification:**
Unit test: verify `EnforceSecret` blocks plain-text `mongodb.url`. Code review: audit all log statements for credential fields.

---

### 12. Graceful Shutdown

**OPLOG-031:** *Removed — RisingWave stream engine handles source executor lifecycle and graceful shutdown.*

---

### 13. Update Post-Image

**OPLOG-032:** Event Driven

**Requirement:**
WHEN processing an oplog `u` (update) entry that contains only partial update modifiers (`$set`, `$unset`, etc.) rather than the full replacement document, the Oplog Tailer SHALL perform a `findOne({_id: <document_id>})` read-back against the same replica set member to retrieve the full post-image document for the `after` field.

**Rationale:**
`DebeziumMongoJsonParser` expects the `after` field to contain the complete document. Oplog update entries on MongoDB v3.6+ may contain only diff modifiers. This read-back approach matches what Debezium itself does. The read targets the same replica set member, so latency is minimal.

**Verification:**
Unit test: given a partial update oplog entry, verify read-back produces full document in `after` field. Integration test: update a document field, verify full post-image arrives in RisingWave.

---

### 14. Snapshot Phase

**OPLOG-033:** Event Driven

**Requirement:**
WHEN the Oplog Tailer starts with no saved offset (first start) and `scan.startup.mode = 'snapshot'`, it SHALL:
1. Record the current oplog timestamp as `snapshot_start_ts`
2. Execute `collection.find({}).sort({_id: 1})` with `readConcern: "majority"` and `no_cursor_timeout(true)`
3. Emit each document as a Debezium `"c"` (create) `SourceMessage` in batches of `mongodb.snapshot.batch_size`
4. After scan completes, set `snapshot_done = true` in split state and transition to oplog tailing from `snapshot_start_ts`

**Rationale:**
The oplog is a capped collection with limited history. It does NOT contain the full state of a collection. A snapshot is required to capture existing data. The `_id`-ordered scan enables deterministic resumability. `readConcern: "majority"` ensures consistency with the watermark invariant. Streaming via cursor avoids full-collection buffering in memory.

**Verification:**
Integration test: insert documents before creating source, verify all pre-existing documents arrive in materialized view.

---

**OPLOG-034:** Event Driven

**Requirement:**
WHEN the Oplog Tailer is performing a snapshot scan and RisingWave issues a barrier checkpoint, the Oplog Tailer SHALL persist the `_id` of the last yielded document in the split state for crash recovery. WHEN restarting with `snapshot_done = false` and a saved `last_id`, the scanner SHALL resume with `find({_id: {$gt: last_id}}).sort({_id: 1})`.

**Rationale:**
Large collection scans may be interrupted by crashes or restarts. Persisting the last `_id` enables resumability without restarting the entire scan.

**Verification:**
Unit test: verify split state serialization includes `snapshot_last_id`. Integration test: interrupt snapshot mid-scan, restart, verify scan resumes without duplicates.

---

**OPLOG-035:** Event Driven

**Requirement:**
WHEN the snapshot scan completes, the Oplog Tailer SHALL begin oplog tailing from `snapshot_start_ts` (not the current timestamp) to capture any writes that occurred during the snapshot. Duplicate inserts for documents that existed pre-snapshot are handled by downstream upsert semantics on the `_id` primary key.

**Rationale:**
The snapshot and oplog tailing phases may overlap temporally. Starting the oplog from `snapshot_start_ts` ensures no writes are missed during the transition.

**Verification:**
Integration test: insert documents during snapshot scan, verify they appear in materialized view after transition to CDC mode.

---

### 15. Startup Mode Configuration

**OPLOG-036:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL support three startup modes:
- `snapshot` (default): full snapshot scan followed by oplog tailing from `snapshot_start_ts`
- `latest`: skip snapshot, begin oplog tailing from the current oplog position
- `earliest`: skip snapshot, begin oplog tailing from the oldest available oplog entry

**Rationale:**
Different use cases require different startup behaviors. `latest` is useful when historical data is not needed. `earliest` enables catching up from the oldest available oplog entry (limited by oplog size).

**Verification:**
Unit test: verify each mode sets correct initial offset. Integration test: create source with each mode, verify expected behavior.

---

### 16. Read Preference

**OPLOG-037:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL default to `secondaryPreferred` read preference for both snapshot scans and oplog tailing WHEN the MongoDB connection URI does not specify a `readPreference` option. Users MAY override read preference by including standard MongoDB URI options (e.g., `?readPreference=secondary&readPreferenceTags=dc:us-east`). The connector SHALL NOT provide separate `mongodb.read_preference` or `mongodb.read_preference.tags` WITH-clause parameters; all read preference configuration MUST be done via the URI string. This MUST work with MongoDB v3.6+ replica sets.

**Rationale:**
Tailing a Secondary's oplog offloads read load from the Primary. `secondaryPreferred` is chosen as the default over `secondary` to allow the connector to fall back to the Primary when no Secondary is available, improving availability. Every replica set member has its own `local.oplog.rs`. The watermark gate (OPLOG-006) ensures correctness regardless of which member is tailed. Configuring via the standard MongoDB URI keeps the connector's config surface small and leverages the URI format that MongoDB operators already know.

**Verification:**
Unit test: verify that `ClientOptions::parse` with no `readPreference` in URI results in `selection_criteria = None`, and that `build_client()` sets `secondaryPreferred`. Verify that an explicit `readPreference` in the URI is preserved.

---

### 17. Sharded Cluster Support

**OPLOG-038:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL support sharded MongoDB clusters via automatic shard discovery (OPLOG-040 through OPLOG-046). WHEN the `mongodb.url` points to a mongos router, the enumerator SHALL discover all shards and create one split per shard, each tailed independently. Users MAY also use the single-RS mode by pointing `mongodb.url` directly at a replica set URI.

**Rationale:**
Each shard in a sharded cluster is its own replica set with its own oplog. Automatic discovery via mongos eliminates the need for one `CREATE SOURCE` per shard and dynamically adapts to cluster topology changes.

**Verification:**
Unit test: verify dual-mode detection (mongos vs replica set). Integration test: create source against a sharded cluster, verify all shards are discovered and tailed.

---

### 18. MongoDB Version Compatibility

**OPLOG-039:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL support MongoDB versions 3.6 and later. The Rust `mongodb` driver v2.8.2 supports server 3.6 through 7.0+. The implementation SHALL NOT use features introduced after MongoDB 3.6 as hard requirements, including:
- Change Streams (3.6+ but NOT used — we tail oplog directly)
- `$changeStream` aggregation stage (NOT used)
- `changeStreamPreAndPostImages` (6.0+ — NOT required, read-back used instead per OPLOG-032)
- SCRAM-SHA-256 authentication (4.0+ — NOT required; SCRAM-SHA-1 available in 3.6)

The following MongoDB features used by the connector are all available in 3.6+:
- `local.oplog.rs` tailable cursor (available since 2.x)
- `replSetGetStatus` admin command with `lastCommittedOpTime` (available since 2.x)
- `readConcern: "majority"` (available since 3.2)

**Driver Compatibility Matrix:**

| Rust Driver Version | Min MongoDB Server | Notes |
|:----|:----|:----|
| 1.x (EOL) | 3.6 | End of life |
| **2.x** | **3.6** | **Current in RisingWave (v2.8.2)** |
| 3.0 – 3.2 | 4.0 | Newer API, drops 3.6 support |
| 3.3+ | 4.2 | 4.0 support removed |

**Rationale:**
The oplog tailing approach works on any replica set (even pre-3.6). Using driver v2.8.2 sets the floor at MongoDB 3.6, which is the minimum required version. Driver v3.x would raise the floor to 4.0+.

**Verification:**
Integration test: verify connector functions against MongoDB 3.6 and latest stable version. Document minimum version requirement.

---

### 19. Sharded Cluster Auto-Discovery

**OPLOG-040:** Event Driven

**Requirement:**
WHEN `mongodb.url` points to a mongos instance (detected via `ismaster` response containing `msg: "isdbgrid"`), the Oplog Tailer's enumerator SHALL call `listShards` on the `admin` database and return one split per non-draining shard. Each split SHALL carry the shard's direct replica set URI.

**Rationale:**
Mongos is the entry point for all sharded cluster operations. `listShards` is the authoritative API for shard topology and is available on all MongoDB 3.6+ sharded clusters. Auto-discovery eliminates the need for one `CREATE SOURCE` per shard.

**Verification:**
Unit test: parse `listShards` response into splits. Integration test: connect to a sharded cluster via mongos, verify correct number of splits returned.

---

**OPLOG-041:** Ubiquitous

**Requirement:**
The Oplog Tailer SHALL parse the `host` field from `listShards` (format: `rsName/host1:port,host2:port,...`) into a MongoDB replica set URI (`mongodb://host1:port,host2:port,.../?replicaSet=rsName`) and store it in `MongodbOplogSplit.replica_set_uri`.

**Rationale:**
The `host` field format is a MongoDB convention used across all tools. Parsing it into a standard URI enables the Rust `mongodb` driver to connect directly to the shard's replica set for oplog tailing.

**Verification:**
Unit test: verify parsing of multi-host and single-host formats. Verify error on invalid format.

---

**OPLOG-042:** Event Driven

**Requirement:**
WHEN a new shard appears in `listShards` that was not previously tracked by the enumerator, the enumerator SHALL return a split with `start_offset = None` to trigger a full snapshot followed by oplog tailing on that shard. The enumerator SHALL log at INFO level when a new shard is discovered.

**Rationale:**
New shards may already contain data from chunk migration. A snapshot ensures all existing documents are captured before oplog tailing begins.

**Verification:**
Integration test: add a shard to a running cluster, verify new split appears and snapshot is triggered.

---

**OPLOG-043:** State Driven

**Requirement:**
WHILE a shard has `draining: true` in `listShards`, the enumerator SHALL continue returning its split to allow the oplog reader to consume remaining events from that shard.

**Rationale:**
Shard removal is a two-phase process. The shard's oplog may still contain un-consumed entries during the draining period. Stopping the reader prematurely would cause data loss.

**Verification:**
Unit test: verify that a shard with `draining: true` is included in the split list. Integration test: initiate `removeShard`, verify reader continues until shard is fully removed.

---

**OPLOG-044:** Event Driven

**Requirement:**
WHEN a shard is absent from `listShards` (fully removed from the cluster), the enumerator SHALL stop returning its `split_id`, causing RisingWave to stop the corresponding reader. The enumerator SHALL log at INFO level when a shard is removed.

**Rationale:**
Once a shard is fully decommissioned, its replica set is no longer part of the cluster and its oplog is no longer relevant.

**Verification:**
Integration test: complete shard removal, verify split disappears from enumerator and reader stops.

---

**OPLOG-045:** Ubiquitous

**Requirement:**
Each `SplitReader` SHALL use `split.replica_set_uri` (if present) to build its MongoDB client for oplog tailing, NEVER the mongos URI. The `local.oplog.rs` collection is inaccessible through mongos; MongoDB returns an error when any client attempts to access the `local` database through a mongos router.

**Rationale:**
This is a fundamental MongoDB architectural constraint. The `local` database is node-local and not routed through mongos. The two-tier connection model (mongos for discovery, direct RS for tailing) is the industry-standard pattern used by Debezium, Monstache, and other CDC tools.

**Verification:**
Code review: verify reader never uses the mongos URI for oplog access. Integration test: connect via mongos, verify each shard reader connects directly to its replica set.

---

**OPLOG-046:** Ubiquitous

**Requirement:**
The shard discovery polling interval SHALL be configurable via `mongodb.shard.discovery.interval_secs` (default: 30). The enumerator SHALL poll `listShards` at this interval to detect topology changes.

**Rationale:**
Tunable interval allows operators to balance responsiveness to topology changes against the overhead of `listShards` queries.

**Verification:**
Unit test: verify default value is 30. Verify override takes effect.

---

## Tradeoff Analysis

### Why Oplog Tailing vs. Change Streams

| Dimension | Change Streams (Current) | Oplog Tailer (This Spec) |
|:----------|:------------------------|:-------------------------|
| **Setup complexity** | Low (Debezium handles everything) | Lower (native connector, no JNI/Java) |
| **MongoDB version** | Requires 3.6+ (Change Streams) | Requires 3.6+ (driver v2.8.2; oplog available since 2.x) |
| **Permissions** | Requires `changeStream` privilege | Requires `read` on `local` database |
| **Durability control** | Implicit (Change Streams are post-commit) | Explicit via majority commit point polling |
| **Rollback safety** | Handled by MongoDB driver | Handled by buffer + watermark logic |
| **Visibility into lag** | Limited | Full (buffer size, watermark lag metrics) |
| **Pre-image/post-image** | Requires MongoDB 6.0+ for pre-image | Read-back for updates (OPLOG-032) |
| **Filtered collections** | Via pipeline aggregation | Via namespace filter |
| **Read load** | Primary by default | Secondary by default |

### Consistency Guarantee

The mathematical invariant enforced by OPLOG-006:

```
t_entry <= t_majority_commit
```

This guarantees that RisingWave only ingests operations that are durable across a majority of MongoDB replica set members. The tailer may lag behind the oplog head, but it will never present data that MongoDB would roll back. This is a **lagging-but-correct** consistency model.

### Buffer Sizing Tradeoffs

- **Smaller buffer** (10MB): Lower memory footprint, but may stall oplog reading during write bursts when the majority commit point lags.
- **Larger buffer** (1GB): Tolerates longer majority commit lag, but higher memory usage and longer recovery flush on shutdown.
- **Default** (100MB): Balances throughput for typical workloads (~10K ops/sec with 1KB average document size gives ~10 seconds of buffering).

## Future Considerations

- **Multi-collection multiplexing**: A single source instance serving multiple collections via namespace filter patterns.
- **Batched read-back**: For update-heavy workloads, batch `findOne` calls into periodic bulk reads. Collect update `_id` values and issue batched `find({_id: {$in: [...]}})` requests every configurable interval (default: 100ms) or when a maximum batch size is reached (default: 1024). This amortizes round-trip latency across multiple updates (OPLOG-032 optimization).
- **Change Stream pre/post images**: On MongoDB 6.0+, use `changeStreamPreAndPostImages` collection option to avoid read-back entirely.
- **MongoDB 3.6 support**: Implemented — using `mongodb` Rust driver v2.8.2 which supports server 3.6+.
