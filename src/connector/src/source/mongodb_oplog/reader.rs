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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_async_stream::try_stream;
use mongodb::Cursor;
use mongodb::bson::{Bson, Document, Timestamp, doc};
use mongodb::options::{CursorType, FindOneOptions, FindOptions, ReadConcern};
use tokio::sync::{Mutex, mpsc};

use super::MongodbOplogProperties;
use super::message::{oplog_entry_to_source_message, snapshot_doc_to_source_message};
use super::oplog_types::{decode_watermark, encode_watermark};
use super::split::{MongodbOplogOffset, MongodbOplogSplit, SnapshotChunkState};
use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::common::into_chunk_stream;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitReader,
};

/// Split reader for the mongo-oplog source (OPLOG-001 through OPLOG-039).
///
/// Implements the High-Watermark Tailer pattern:
/// 1. Optional snapshot phase (OPLOG-033/034/035)
/// 2. Spawn watermark poller (OPLOG-004)
/// 3. Tail oplog with tailable cursor, gated by majority commit watermark (OPLOG-006)
pub struct MongodbOplogSplitReader {
    properties: MongodbOplogProperties,
    split: MongodbOplogSplit,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for MongodbOplogSplitReader {
    type Properties = MongodbOplogProperties;
    type Split = MongodbOplogSplit;

    async fn new(
        properties: MongodbOplogProperties,
        splits: Vec<MongodbOplogSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        assert_eq!(splits.len(), 1);
        let split = splits.into_iter().next().unwrap();

        // OPLOG-047/048: validate snapshot parallelism config
        properties.validate_snapshot_config()?;
        // OPLOG-061/062: validate readback batching config
        properties.validate_readback_config()?;

        Ok(Self {
            properties,
            split,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_ctx = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_ctx)
    }
}

/// Shared state from the watermark poller background task (OPLOG-004, OPLOG-008b).
struct WatermarkState {
    /// Majority commit point encoded as u64 (OPLOG-004).
    watermark: AtomicU64,
    /// Oldest oplog entry timestamp, for window estimation (OPLOG-008b).
    oplog_first_ts: AtomicU64,
    /// Newest oplog entry timestamp, for window estimation (OPLOG-008b).
    oplog_last_ts: AtomicU64,
}

/// Resume state for snapshot dispatch (OPLOG-049/050).
pub(crate) enum SnapshotResumeState {
    /// Serial mode (workers=1) — resume from optional last _id.
    Serial { resume_id: Option<String> },
    /// Chunked mode — resume from persisted chunk states.
    Chunked { chunks: Vec<SnapshotChunkState> },
    /// Fresh start, no persisted offset.
    Fresh,
}

/// A batch sent from a chunk worker to the main reader (OPLOG-049).
type ChunkBatch = Result<(usize, Vec<SourceMessage>), crate::error::ConnectorError>;

impl MongodbOplogSplitReader {
    /// Spawn a background task that polls `replSetGetStatus` and updates the
    /// watermark atomically (OPLOG-004). Also polls oplog window bounds every
    /// 10th tick for window-usage alerting (OPLOG-008b).
    fn spawn_watermark_poller(
        client: &mongodb::Client,
        poll_interval: Duration,
    ) -> Arc<WatermarkState> {
        let state = Arc::new(WatermarkState {
            watermark: AtomicU64::new(0),
            oplog_first_ts: AtomicU64::new(0),
            oplog_last_ts: AtomicU64::new(0),
        });
        let state_clone = state.clone();
        let admin_db = client.database("admin");
        let oplog_coll = client
            .database("local")
            .collection::<Document>("oplog.rs");

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            let mut tick_count: u64 = 0;
            loop {
                interval.tick().await;
                tick_count += 1;

                // Poll replSetGetStatus for watermark (every tick)
                match admin_db
                    .run_command(doc! { "replSetGetStatus": 1 }, None)
                    .await
                {
                    Ok(status) => {
                        if let Ok(optimes) = status.get_document("optimes") {
                            if let Ok(last_committed) =
                                optimes.get_document("lastCommittedOpTime")
                            {
                                if let Ok(ts) = last_committed.get_timestamp("ts") {
                                    let encoded = encode_watermark(ts.time, ts.increment);
                                    state_clone.watermark.store(encoded, Ordering::Release);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to poll replSetGetStatus for watermark");
                    }
                }

                // Poll oplog window bounds (every 10th tick to limit overhead) (OPLOG-008b)
                if tick_count % 10 == 0 {
                    // Oldest entry
                    let oldest_opts = FindOneOptions::builder()
                        .sort(doc! { "$natural": 1 })
                        .build();
                    if let Ok(Some(oldest)) = oplog_coll
                        .find_one(doc! {}, oldest_opts)
                        .await
                    {
                        if let Ok(ts) = oldest.get_timestamp("ts") {
                            state_clone.oplog_first_ts.store(
                                encode_watermark(ts.time, ts.increment),
                                Ordering::Release,
                            );
                        }
                    }
                    // Newest entry
                    let newest_opts = FindOneOptions::builder()
                        .sort(doc! { "$natural": -1 })
                        .build();
                    if let Ok(Some(newest)) = oplog_coll
                        .find_one(doc! {}, newest_opts)
                        .await
                    {
                        if let Ok(ts) = newest.get_timestamp("ts") {
                            state_clone.oplog_last_ts.store(
                                encode_watermark(ts.time, ts.increment),
                                Ordering::Release,
                            );
                        }
                    }
                }
            }
        });

        state
    }

    /// Maximum number of retries for transient snapshot cursor failures.
    const SNAPSHOT_MAX_RETRIES: usize = 5;

    /// Check whether `snapshot_start_ts` is still within the oplog window (OPLOG-053).
    ///
    /// Queries the oldest oplog entry. If `snapshot_start_ts` has already been
    /// overwritten, returns an error. If it is within the 80% danger zone,
    /// logs a warning. Emits `oplog_snapshot_window_remaining_pct` gauge when
    /// metrics context is provided.
    async fn check_oplog_window_during_snapshot(
        client: &mongodb::Client,
        snapshot_start_ts: Timestamp,
        metrics_ctx: Option<(&SourceContextRef, &[String; 4])>,
    ) -> ConnectorResult<()> {
        let oplog_coll = client
            .database("local")
            .collection::<Document>("oplog.rs");

        let oldest_opts = FindOneOptions::builder()
            .sort(doc! { "$natural": 1 })
            .build();
        let newest_opts = FindOneOptions::builder()
            .sort(doc! { "$natural": -1 })
            .build();

        let oldest = oplog_coll
            .find_one(doc! {}, oldest_opts)
            .await
            .ok()
            .flatten();
        let newest = oplog_coll
            .find_one(doc! {}, newest_opts)
            .await
            .ok()
            .flatten();

        if let (Some(oldest_doc), Some(newest_doc)) = (oldest, newest) {
            let oldest_ts = oldest_doc.get_timestamp("ts").ok();
            let newest_ts = newest_doc.get_timestamp("ts").ok();

            if let Some(oldest_ts) = oldest_ts {
                if oldest_ts > snapshot_start_ts {
                    // OPLOG-053: emit 0% before failing
                    if let Some((ctx, labels)) = &metrics_ctx {
                        ctx.metrics
                            .oplog_snapshot_window_remaining_pct
                            .with_guarded_label_values(*labels)
                            .set(0);
                    }
                    return Err(anyhow::anyhow!(
                        "Oplog window exhausted during snapshot: snapshot_start_ts={}.{} \
                         is older than oldest oplog entry={}.{}. The oplog has wrapped \
                         and CDC continuity cannot be guaranteed. Increase the MongoDB \
                         oplog size or reduce collection size before retrying with \
                         scan.startup.mode='snapshot'.",
                        snapshot_start_ts.time,
                        snapshot_start_ts.increment,
                        oldest_ts.time,
                        oldest_ts.increment,
                    )
                    .into());
                }

                if let Some(newest_ts) = newest_ts {
                    let window_secs = newest_ts.time.saturating_sub(oldest_ts.time);
                    let remaining_pct = if window_secs > 0 {
                        let remaining = snapshot_start_ts.time.saturating_sub(oldest_ts.time);
                        ((remaining as u64) * 100 / (window_secs as u64)) as i64
                    } else {
                        100 // degenerate: single-entry oplog
                    };

                    // OPLOG-053: emit window remaining gauge
                    if let Some((ctx, labels)) = &metrics_ctx {
                        ctx.metrics
                            .oplog_snapshot_window_remaining_pct
                            .with_guarded_label_values(*labels)
                            .set(remaining_pct);
                    }

                    if is_near_oplog_wrap(
                        snapshot_start_ts.time,
                        oldest_ts.time,
                        newest_ts.time,
                    ) {
                        tracing::warn!(
                            snapshot_start_ts_secs = snapshot_start_ts.time,
                            oldest_oplog_ts_secs = oldest_ts.time,
                            newest_oplog_ts_secs = newest_ts.time,
                            remaining_pct,
                            "OPLOG-053: Oplog window running low during snapshot: \
                             snapshot_start_ts is within 20% of oplog tail. Snapshot \
                             must complete soon or oplog will wrap, causing data loss. \
                             Consider increasing oplog size."
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Dispatch snapshot scan to serial or chunked mode (OPLOG-047/049).
    ///
    /// When `workers_per_shard == 1`, delegates to `run_snapshot_serial`.
    /// When `workers_per_shard > 1`, discovers boundaries and runs
    /// `run_snapshot_chunked` with a pool of concurrent workers.
    pub(crate) async fn run_snapshot(
        client: &mongodb::Client,
        db_name: &str,
        coll_name: &str,
        rs_name: &str,
        split_id: &SplitId,
        batch_size: usize,
        snapshot_start_ts: Timestamp,
        resume_state: SnapshotResumeState,
        workers_per_shard: usize,
        chunk_target_docs: u64,
        boundary_mode: super::BoundaryMode,
        metrics_ctx: Option<(&SourceContextRef, &[String; 4])>,
    ) -> ConnectorResult<Vec<Vec<SourceMessage>>> {
        match workers_per_shard {
            0 => Err(anyhow::anyhow!("workers_per_shard must be >= 1").into()),
            1 => {
                // Serial mode: extract resume_id from state
                let resume_id = match resume_state {
                    SnapshotResumeState::Serial { resume_id } => resume_id,
                    SnapshotResumeState::Chunked { chunks } => {
                        // Downgrade: if all chunks done, return empty
                        if chunks.iter().all(|c| c.done) {
                            return Ok(vec![]);
                        }
                        // Otherwise find the minimum last_id across incomplete chunks
                        chunks
                            .iter()
                            .filter(|c| !c.done)
                            .filter_map(|c| c.last_id.clone())
                            .min()
                    }
                    SnapshotResumeState::Fresh => None,
                };
                Self::run_snapshot_serial(
                    client,
                    db_name,
                    coll_name,
                    rs_name,
                    split_id,
                    batch_size,
                    snapshot_start_ts,
                    resume_id,
                    metrics_ctx,
                )
                .await
            }
            _ => {
                // Chunked mode: discover or reuse boundaries
                let chunks = match resume_state {
                    SnapshotResumeState::Chunked { chunks } => chunks,
                    _ => {
                        discover_chunk_boundaries(
                            client,
                            db_name,
                            coll_name,
                            chunk_target_docs,
                            boundary_mode,
                        )
                        .await?
                    }
                };

                // If only 1 chunk (small collection), fall back to serial
                if chunks.len() <= 1 {
                    let resume_id = chunks
                        .first()
                        .and_then(|c| if c.done { None } else { c.last_id.clone() });
                    return Self::run_snapshot_serial(
                        client,
                        db_name,
                        coll_name,
                        rs_name,
                        split_id,
                        batch_size,
                        snapshot_start_ts,
                        resume_id,
                        metrics_ctx,
                    )
                    .await;
                }

                Self::run_snapshot_chunked(
                    client,
                    db_name,
                    coll_name,
                    rs_name,
                    split_id,
                    batch_size,
                    snapshot_start_ts,
                    chunks,
                    workers_per_shard,
                    metrics_ctx,
                )
                .await
            }
        }
    }

    /// Run snapshot scan with retry on transient cursor failures (serial mode).
    ///
    /// Uses the `snapshot_last_id` checkpoint mechanism for resumability: on a
    /// cursor error the scan re-opens from the last successfully processed `_id`,
    /// avoiding duplicate documents. Retries use exponential backoff consistent
    /// with other connectors (Kinesis, NATS).
    pub(crate) async fn run_snapshot_serial(
        client: &mongodb::Client,
        db_name: &str,
        coll_name: &str,
        rs_name: &str,
        split_id: &SplitId,
        batch_size: usize,
        snapshot_start_ts: Timestamp,
        resume_id: Option<String>,
        metrics_ctx: Option<(&SourceContextRef, &[String; 4])>,
    ) -> ConnectorResult<Vec<Vec<SourceMessage>>> {
        let coll = client.database(db_name).collection::<Document>(coll_name);

        let mut all_batches = Vec::new();
        let mut last_good_id: Option<String> = resume_id;
        let mut retries = 0usize;

        loop {
            let mut filter = Document::new();
            if let Some(ref last_id_json) = last_good_id {
                let last_id: Bson = serde_json::from_str(last_id_json)?;
                filter.insert("_id", doc! { "$gt": last_id });
            }

            let find_opts = FindOptions::builder()
                .sort(doc! { "_id": 1 })
                .batch_size(batch_size as u32)
                .read_concern(ReadConcern::majority())
                .no_cursor_timeout(true)
                .build();
            let cursor_result: Result<Cursor<Document>, _> =
                coll.find(filter, find_opts).await;

            let mut cursor = match cursor_result {
                Ok(c) => c,
                Err(e) => {
                    retries += 1;
                    if retries > Self::SNAPSHOT_MAX_RETRIES {
                        return Err(anyhow::anyhow!(
                            "snapshot cursor creation failed after {} retries: {}",
                            Self::SNAPSHOT_MAX_RETRIES,
                            e
                        )
                        .into());
                    }
                    let delay = Duration::from_millis(100 * (1 << retries.min(6)));
                    tracing::warn!(
                        error = %e,
                        retry = retries,
                        delay_ms = delay.as_millis() as u64,
                        "Snapshot cursor creation failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };

            let mut batch = Vec::with_capacity(batch_size);

            let cursor_error = loop {
                match cursor.advance().await {
                    Ok(true) => {
                        let document = cursor.deserialize_current()?;
                        let id_bson = document
                            .get("_id")
                            .ok_or_else(|| anyhow::anyhow!("snapshot document missing _id"))?;
                        let id_json =
                            serde_json::to_string(&id_bson.clone().into_canonical_extjson())?;

                        let offset_str = MongodbOplogOffset {
                            snapshot_done: false,
                            snapshot_last_id: Some(id_json.clone()),
                            oplog_ts_secs: snapshot_start_ts.time,
                            oplog_ts_ord: snapshot_start_ts.increment,
                            snapshot_chunks: None,
                        }
                        .to_json_string();

                        let msg = snapshot_doc_to_source_message(
                            &document,
                            rs_name,
                            db_name,
                            coll_name,
                            split_id.clone(),
                            offset_str,
                        )?;
                        batch.push(msg);
                        last_good_id = Some(id_json);

                        if batch.len() >= batch_size {
                            // OPLOG-053: snapshot docs counter
                            if let Some((ctx, labels)) = &metrics_ctx {
                                ctx.metrics
                                    .oplog_snapshot_docs_total
                                    .with_guarded_label_values(*labels)
                                    .inc_by(batch_size as u64);
                            }

                            all_batches.push(std::mem::take(&mut batch));
                            batch = Vec::with_capacity(batch_size);

                            // OPLOG-053: check oplog window every batch
                            Self::check_oplog_window_during_snapshot(
                                client,
                                snapshot_start_ts,
                                metrics_ctx,
                            )
                            .await?;
                        }
                    }
                    Ok(false) => {
                        // Cursor exhausted — snapshot complete
                        break None;
                    }
                    Err(e) => {
                        break Some(e);
                    }
                }
            };

            if !batch.is_empty() {
                all_batches.push(batch);
            }

            match cursor_error {
                None => {
                    // Clean completion
                    return Ok(all_batches);
                }
                Some(e) => {
                    retries += 1;
                    if retries > Self::SNAPSHOT_MAX_RETRIES {
                        return Err(anyhow::anyhow!(
                            "snapshot cursor iteration failed after {} retries: {}",
                            Self::SNAPSHOT_MAX_RETRIES,
                            e
                        )
                        .into());
                    }
                    let delay = Duration::from_millis(100 * (1 << retries.min(6)));
                    tracing::warn!(
                        error = %e,
                        retry = retries,
                        delay_ms = delay.as_millis() as u64,
                        last_good_id = ?last_good_id,
                        "Snapshot cursor iteration failed, reopening from last good _id"
                    );
                    tokio::time::sleep(delay).await;
                    // Loop continues — re-opens cursor from last_good_id
                }
            }
        }
    }

    /// Run chunked parallel snapshot (OPLOG-049).
    ///
    /// Spawns up to `workers_per_shard` concurrent tokio tasks, each scanning
    /// a disjoint `_id` range. Workers send batches through a bounded mpsc
    /// channel; the main task collects batches and snapshots chunk state into
    /// each yielded offset for crash recovery (OPLOG-050).
    async fn run_snapshot_chunked(
        client: &mongodb::Client,
        db_name: &str,
        coll_name: &str,
        rs_name: &str,
        split_id: &SplitId,
        batch_size: usize,
        snapshot_start_ts: Timestamp,
        chunks: Vec<SnapshotChunkState>,
        workers_per_shard: usize,
        metrics_ctx: Option<(&SourceContextRef, &[String; 4])>,
    ) -> ConnectorResult<Vec<Vec<SourceMessage>>> {
        let num_chunks = chunks.len();
        let pending_chunks: Vec<_> = chunks
            .iter()
            .filter(|c| !c.done)
            .cloned()
            .collect();

        if pending_chunks.is_empty() {
            return Ok(vec![]);
        }

        // OPLOG-052: set chunks_total metric
        if let Some((ctx, labels)) = &metrics_ctx {
            ctx.metrics
                .oplog_snapshot_chunks_total
                .with_guarded_label_values(*labels)
                .set(num_chunks as i64);
            let done_count = chunks.iter().filter(|c| c.done).count();
            ctx.metrics
                .oplog_snapshot_chunks_done
                .with_guarded_label_values(*labels)
                .set(done_count as i64);
        }

        let shared_state = Arc::new(Mutex::new(chunks));
        let (tx, mut rx) = mpsc::channel::<ChunkBatch>(4 * workers_per_shard);

        // Work queue: indices of pending chunks
        let work_queue: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(
            pending_chunks.iter().map(|c| c.chunk_idx).collect(),
        ));

        // Spawn workers (up to workers_per_shard, or pending chunk count)
        let num_workers = workers_per_shard.min(pending_chunks.len());
        for _ in 0..num_workers {
            let client = client.clone();
            let db_name = db_name.to_owned();
            let coll_name = coll_name.to_owned();
            let rs_name = rs_name.to_owned();
            let split_id = split_id.clone();
            let shared_state = shared_state.clone();
            let work_queue = work_queue.clone();
            let tx = tx.clone();

            tokio::spawn(async move {
                loop {
                    // Grab next chunk from work queue
                    let chunk_idx = {
                        let mut queue = work_queue.lock().await;
                        queue.pop()
                    };
                    let chunk_idx = match chunk_idx {
                        Some(idx) => idx,
                        None => return, // No more work
                    };

                    // Read chunk state
                    let chunk_state = {
                        let state = shared_state.lock().await;
                        state[chunk_idx].clone()
                    };

                    if chunk_state.done {
                        continue;
                    }

                    // Run this chunk
                    if let Err(e) = Self::run_snapshot_chunk(
                        &client,
                        &db_name,
                        &coll_name,
                        &rs_name,
                        &split_id,
                        batch_size,
                        snapshot_start_ts,
                        chunk_idx,
                        &chunk_state,
                        shared_state.clone(),
                        &tx,
                    )
                    .await
                    {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            });
        }

        // Drop our sender so rx closes when all workers finish
        drop(tx);

        let mut all_batches: Vec<Vec<SourceMessage>> = Vec::new();
        let mut chunk_doc_counts: Vec<u64> = vec![0; num_chunks];

        while let Some(result) = rx.recv().await {
            let (chunk_idx, batch) = result?;
            let batch_len = batch.len();
            chunk_doc_counts[chunk_idx] += batch_len as u64;

            // Snapshot chunk states and rewrite offsets
            let current_chunks = shared_state.lock().await.clone();
            let mut batch = batch;
            for msg in &mut batch {
                rewrite_offset_with_chunks(msg, &current_chunks, snapshot_start_ts);
            }

            // OPLOG-052: check if this chunk just completed
            if current_chunks[chunk_idx].done {
                if let Some((ctx, labels)) = &metrics_ctx {
                    ctx.metrics
                        .oplog_snapshot_chunks_done
                        .with_guarded_label_values(*labels)
                        .inc();
                    ctx.metrics
                        .oplog_snapshot_docs_per_chunk
                        .with_guarded_label_values(*labels)
                        .observe(chunk_doc_counts[chunk_idx] as f64);
                }
            }

            // OPLOG-053: snapshot docs counter
            if let Some((ctx, labels)) = &metrics_ctx {
                ctx.metrics
                    .oplog_snapshot_docs_total
                    .with_guarded_label_values(*labels)
                    .inc_by(batch_len as u64);
            }

            if !batch.is_empty() {
                all_batches.push(batch);
            }
        }

        Ok(all_batches)
    }

    /// Run a single chunk's snapshot scan (OPLOG-049 worker).
    ///
    /// Scans `{_id: {$gte: min_id, $lt: max_id}}` with retry logic
    /// matching the serial snapshot. Sends batches through `tx` and
    /// updates shared state after each batch.
    async fn run_snapshot_chunk(
        client: &mongodb::Client,
        db_name: &str,
        coll_name: &str,
        rs_name: &str,
        split_id: &SplitId,
        batch_size: usize,
        snapshot_start_ts: Timestamp,
        chunk_idx: usize,
        chunk_state: &SnapshotChunkState,
        shared_state: Arc<Mutex<Vec<SnapshotChunkState>>>,
        tx: &mpsc::Sender<ChunkBatch>,
    ) -> ConnectorResult<()> {
        let coll = client.database(db_name).collection::<Document>(coll_name);
        let mut last_good_id: Option<String> = chunk_state.last_id.clone();
        let mut retries = 0usize;

        loop {
            // Build filter: {_id: {$gt: resume_point, $lt: max_id}}
            let mut filter = Document::new();
            let mut id_filter = Document::new();

            // Lower bound: resume from last_good_id, or chunk min_id
            if let Some(ref last_id_json) = last_good_id {
                let last_id: Bson = serde_json::from_str(last_id_json)?;
                id_filter.insert("$gt", last_id);
            } else if let Some(ref min_id_json) = chunk_state.min_id {
                let min_id: Bson = serde_json::from_str(min_id_json)?;
                id_filter.insert("$gte", min_id);
            }

            // Upper bound: chunk max_id (exclusive)
            if let Some(ref max_id_json) = chunk_state.max_id {
                let max_id: Bson = serde_json::from_str(max_id_json)?;
                id_filter.insert("$lt", max_id);
            }

            if !id_filter.is_empty() {
                filter.insert("_id", id_filter);
            }

            let find_opts = FindOptions::builder()
                .sort(doc! { "_id": 1 })
                .batch_size(batch_size as u32)
                .read_concern(ReadConcern::majority())
                .no_cursor_timeout(true)
                .build();

            let cursor_result: Result<Cursor<Document>, _> = coll.find(filter, find_opts).await;
            let mut cursor = match cursor_result {
                Ok(c) => c,
                Err(e) => {
                    retries += 1;
                    if retries > Self::SNAPSHOT_MAX_RETRIES {
                        return Err(anyhow::anyhow!(
                            "chunk {} cursor creation failed after {} retries: {}",
                            chunk_idx,
                            Self::SNAPSHOT_MAX_RETRIES,
                            e
                        )
                        .into());
                    }
                    let delay = Duration::from_millis(100 * (1 << retries.min(6)));
                    tracing::warn!(
                        error = %e,
                        chunk_idx,
                        retry = retries,
                        "Chunk cursor creation failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };

            let mut batch = Vec::with_capacity(batch_size);
            let cursor_error = loop {
                match cursor.advance().await {
                    Ok(true) => {
                        let document = cursor.deserialize_current()?;
                        let id_bson = document
                            .get("_id")
                            .ok_or_else(|| anyhow::anyhow!("snapshot document missing _id"))?;
                        let id_json =
                            serde_json::to_string(&id_bson.clone().into_canonical_extjson())?;

                        // Build offset (chunks will be rewritten by main task)
                        let offset_str = MongodbOplogOffset {
                            snapshot_done: false,
                            snapshot_last_id: Some(id_json.clone()),
                            oplog_ts_secs: snapshot_start_ts.time,
                            oplog_ts_ord: snapshot_start_ts.increment,
                            snapshot_chunks: None, // rewritten by main task
                        }
                        .to_json_string();

                        let msg = snapshot_doc_to_source_message(
                            &document,
                            rs_name,
                            db_name,
                            coll_name,
                            split_id.clone(),
                            offset_str,
                        )?;
                        batch.push(msg);
                        last_good_id = Some(id_json.clone());

                        if batch.len() >= batch_size {
                            // Update shared state
                            {
                                let mut state = shared_state.lock().await;
                                state[chunk_idx].last_id = Some(id_json);
                            }

                            // OPLOG-053: check oplog window
                            Self::check_oplog_window_during_snapshot(
                                client,
                                snapshot_start_ts,
                                None, // metrics emitted by main task
                            )
                            .await?;

                            // Send batch
                            if tx
                                .send(Ok((chunk_idx, std::mem::take(&mut batch))))
                                .await
                                .is_err()
                            {
                                return Ok(()); // receiver dropped
                            }
                            batch = Vec::with_capacity(batch_size);
                        }
                    }
                    Ok(false) => {
                        break None; // Cursor exhausted
                    }
                    Err(e) => {
                        break Some(e);
                    }
                }
            };

            // Send remaining batch
            if !batch.is_empty() {
                // Update shared state with latest id
                if let Some(ref id) = last_good_id {
                    let mut state = shared_state.lock().await;
                    state[chunk_idx].last_id = Some(id.clone());
                }
                if tx
                    .send(Ok((chunk_idx, std::mem::take(&mut batch))))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }

            match cursor_error {
                None => {
                    // Clean completion — mark chunk done
                    {
                        let mut state = shared_state.lock().await;
                        state[chunk_idx].done = true;
                    }
                    return Ok(());
                }
                Some(e) => {
                    retries += 1;
                    if retries > Self::SNAPSHOT_MAX_RETRIES {
                        return Err(anyhow::anyhow!(
                            "chunk {} cursor iteration failed after {} retries: {}",
                            chunk_idx,
                            Self::SNAPSHOT_MAX_RETRIES,
                            e
                        )
                        .into());
                    }
                    let delay = Duration::from_millis(100 * (1 << retries.min(6)));
                    tracing::warn!(
                        error = %e,
                        chunk_idx,
                        retry = retries,
                        "Chunk cursor iteration failed, reopening"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// The core async data stream generator.
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        let (db_name, coll_name) = self.properties.parse_namespace()?;
        let db_name = db_name.to_owned();
        let coll_name = coll_name.to_owned();
        let split_id: SplitId = self.split.split_id.clone();
        let startup_mode = self.properties.startup_mode().to_owned();

        // Connect to MongoDB (OPLOG-039: requires v3.6+ per driver constraint, OPLOG-037: read preference)
        let (client, rs_name) = self.properties.build_client().await?;
        let rs_name = rs_name.as_str();

        // Parse existing offset
        let offset = self.split.offset()?;

        // Metrics labels (shared by snapshot + CDC phases)
        let metrics_labels = [
            self.source_ctx.source_id.to_string(),
            self.source_ctx.source_name.clone(),
            self.source_ctx.fragment_id.to_string(),
            split_id.to_string(),
        ];
        let snapshot_metrics_ctx = Some((&self.source_ctx, &metrics_labels));

        // OPLOG-054: compute boundary mode
        let boundary_mode = self.properties.boundary_mode()?;

        // --- Phase 1: Snapshot (OPLOG-033/034/035/036) ---
        let oplog_resume_ts = match (&offset, startup_mode.as_str()) {
            // Already completed snapshot or resuming CDC
            (Some(o), _) if o.snapshot_done => Some(Timestamp {
                time: o.oplog_ts_secs,
                increment: o.oplog_ts_ord,
            }),
            // Resuming interrupted snapshot
            (Some(o), _) if !o.snapshot_done => {
                let snapshot_start_ts =
                    Self::get_current_oplog_ts(&client).await?;
                let batch_size = self.properties.snapshot_batch_size as usize;
                let workers = self.properties.snapshot_workers_per_shard as usize;

                // OPLOG-050: construct resume state from persisted offset
                let resume_state = if let Some(chunks) = o.snapshot_chunks.clone() {
                    SnapshotResumeState::Chunked { chunks }
                } else {
                    SnapshotResumeState::Serial { resume_id: o.snapshot_last_id.clone() }
                };

                let chunk_target = self.properties.snapshot_chunk_target_docs;
                let batches = Self::run_snapshot(
                    &client,
                    &db_name,
                    &coll_name,
                    rs_name,
                    &split_id,
                    batch_size,
                    snapshot_start_ts,
                    resume_state,
                    workers,
                    chunk_target,
                    boundary_mode,
                    snapshot_metrics_ctx,
                )
                .await?;

                for batch in batches {
                    yield batch;
                }

                Some(snapshot_start_ts)
            }
            // Fresh start with snapshot mode
            (None, "snapshot") => {
                let snapshot_start_ts =
                    Self::get_current_oplog_ts(&client).await?;
                let batch_size = self.properties.snapshot_batch_size as usize;
                let workers = self.properties.snapshot_workers_per_shard as usize;
                let chunk_target = self.properties.snapshot_chunk_target_docs;

                let batches = Self::run_snapshot(
                    &client,
                    &db_name,
                    &coll_name,
                    rs_name,
                    &split_id,
                    batch_size,
                    snapshot_start_ts,
                    SnapshotResumeState::Fresh,
                    workers,
                    chunk_target,
                    boundary_mode,
                    snapshot_metrics_ctx,
                )
                .await?;

                for batch in batches {
                    yield batch;
                }

                Some(snapshot_start_ts)
            }
            // Latest: start from current position
            (None, "latest") => {
                let ts = Self::get_current_oplog_ts(&client).await?;
                Some(ts)
            }
            // Earliest: start from oldest oplog entry
            (None, "earliest") => None,
            _ => {
                return Err(anyhow::anyhow!(
                    "invalid scan.startup.mode: {}",
                    startup_mode
                )
                .into());
            }
        };

        // --- Phase 2: CDC (oplog tailing with high-watermark gating) ---

        // Spawn watermark poller (OPLOG-004, OPLOG-008b)
        let poll_interval =
            Duration::from_millis(self.properties.watermark_poll_interval_ms);
        let watermark_state = Self::spawn_watermark_poller(&client, poll_interval);

        let buffer_max_bytes = self.properties.buffer_max_bytes as usize;
        let heartbeat_interval =
            Duration::from_secs(self.properties.heartbeat_interval_secs);
        // OPLOG-061: max IDs per batched $in read-back query
        let readback_batch_max = self.properties.readback_batch_max_count as usize;

        // Stall detection state (OPLOG-008a)
        let stall_timeout =
            Duration::from_secs(self.properties.watermark_stall_timeout_secs);
        let mut last_watermark_advance = tokio::time::Instant::now();
        let mut last_watermark_value: u64 = 0;
        let mut last_stall_warn_time: Option<tokio::time::Instant> = None;

        // Resume position for outer reconnect loop (OPLOG-017)
        let mut last_yielded_ts: Option<Timestamp> = oplog_resume_ts;

        // Outer reconnect loop (OPLOG-008c, OPLOG-017)
        loop {
            // Build oplog filter from last_yielded_ts (OPLOG-003)
            let ns_filter = format!("{}.{}", db_name, coll_name);
            let mut oplog_filter = doc! { "ns": &ns_filter };
            if let Some(resume_ts) = last_yielded_ts {
                oplog_filter.insert("ts", doc! { "$gt": resume_ts });
            }

            let oplog_coll = client
                .database("local")
                .collection::<Document>("oplog.rs");

            let oplog_find_opts = FindOptions::builder()
                .cursor_type(CursorType::TailableAwait)
                .no_cursor_timeout(true)
                .batch_size(1024u32)
                .build();
            let cursor_result = oplog_coll
                .find(oplog_filter, oplog_find_opts)
                .await;

            let mut cursor = match cursor_result {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(error = %e, "OPLOG-008c: Failed to open oplog cursor");
                    self.source_ctx
                        .metrics
                        .oplog_reconnects_total
                        .with_guarded_label_values(&metrics_labels)
                        .inc();
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            // Buffer for entries awaiting watermark release (OPLOG-006/007)
            let mut buffer: BTreeMap<(u32, u32), (Document, String)> = BTreeMap::new();
            let mut buffer_bytes: usize = 0;

            let mut last_yield_time = tokio::time::Instant::now();
            let mut last_watermark_check = tokio::time::Instant::now();
            let watermark_check_interval = Duration::from_millis(50);

            let mut cursor_error: Option<mongodb::error::Error> = None;

            // Inner poll loop: sequential polling (try_stream doesn't support tokio::select!)
            loop {
                // Step 1: Try to read from cursor if buffer has room (OPLOG-008)
                if buffer_bytes < buffer_max_bytes {
                    // Use a short timeout so we don't block watermark checks
                    match tokio::time::timeout(
                        Duration::from_millis(100),
                        cursor.advance(),
                    )
                    .await
                    {
                        Ok(Ok(true)) => {
                            let entry = cursor.deserialize_current()?;
                            if let Ok(ts) = entry.get_timestamp("ts") {
                                let key = (ts.time, ts.increment);
                                let entry_size = estimate_doc_size(&entry);
                                let offset_str = MongodbOplogOffset {
                                    snapshot_done: true,
                                    snapshot_last_id: None,
                                    oplog_ts_secs: ts.time,
                                    oplog_ts_ord: ts.increment,
                                    snapshot_chunks: None,
                                }
                                .to_json_string();
                                buffer.insert(key, (entry, offset_str));
                                buffer_bytes += entry_size;

                                // OPLOG-022: metrics
                                self.source_ctx
                                    .metrics
                                    .oplog_entries_read_total
                                    .with_guarded_label_values(&metrics_labels)
                                    .inc();
                                self.source_ctx
                                    .metrics
                                    .oplog_tail_ts
                                    .with_guarded_label_values(&metrics_labels)
                                    .set(ts.time as i64);
                                self.source_ctx
                                    .metrics
                                    .oplog_buffer_entries
                                    .with_guarded_label_values(&metrics_labels)
                                    .set(buffer.len() as i64);
                                self.source_ctx
                                    .metrics
                                    .oplog_buffer_size_bytes
                                    .with_guarded_label_values(&metrics_labels)
                                    .set(buffer_bytes as i64);
                            }
                        }
                        Ok(Ok(false)) => {
                            tracing::warn!("OPLOG-017: Tailable cursor returned false, reconnecting");
                            break;
                        }
                        Ok(Err(e)) => {
                            cursor_error = Some(e);
                            break;
                        }
                        Err(_timeout) => {
                            // No new entries within timeout, continue to watermark check
                        }
                    }
                }

                // Step 2: Check watermark and release durable entries (OPLOG-006/007)
                let now = tokio::time::Instant::now();
                if now.duration_since(last_watermark_check) >= watermark_check_interval {
                    last_watermark_check = now;
                    let wm = watermark_state.watermark.load(Ordering::Acquire);

                    // OPLOG-005: update watermark metric
                    if wm > 0 {
                        let (wm_secs, _wm_ord) = decode_watermark(wm);
                        self.source_ctx
                            .metrics
                            .oplog_high_watermark_ts
                            .with_guarded_label_values(&metrics_labels)
                            .set(wm_secs as i64);

                        // Compute lag from newest oplog entry
                        let tail_ts =
                            watermark_state.oplog_last_ts.load(Ordering::Acquire);
                        if tail_ts > 0 {
                            let (tail_secs, _) = decode_watermark(tail_ts);
                            self.source_ctx
                                .metrics
                                .oplog_lag_seconds
                                .with_guarded_label_values(&metrics_labels)
                                .set(tail_secs.saturating_sub(wm_secs) as i64);
                        }
                    }

                    // Stall detection (OPLOG-008a)
                    if wm != last_watermark_value {
                        last_watermark_value = wm;
                        last_watermark_advance = now;
                        last_stall_warn_time = None;
                    }
                    if should_emit_stall_warning(
                        buffer_bytes,
                        buffer_max_bytes,
                        now.duration_since(last_watermark_advance),
                        stall_timeout,
                        last_stall_warn_time.map(|t| now.duration_since(t)),
                    ) {
                        tracing::warn!(
                            buffer_bytes,
                            buffer_entries = buffer.len(),
                            watermark_ts_secs =
                                if wm > 0 { decode_watermark(wm).0 } else { 0 },
                            stall_secs =
                                now.duration_since(last_watermark_advance).as_secs(),
                            "OPLOG-008a: Watermark stalled while buffer is full"
                        );
                        last_stall_warn_time = Some(now);
                    }

                    // Oplog window check (OPLOG-008b)
                    let first =
                        watermark_state.oplog_first_ts.load(Ordering::Acquire);
                    let last = watermark_state.oplog_last_ts.load(Ordering::Acquire);
                    if first > 0 && last > first {
                        let (first_secs, _) = decode_watermark(first);
                        let (last_secs, _) = decode_watermark(last);
                        let cursor_secs = buffer
                            .keys()
                            .next()
                            .map(|&(s, _)| s)
                            .or(last_yielded_ts.map(|ts| ts.time))
                            .unwrap_or(last_secs);
                        if is_near_oplog_wrap(cursor_secs, first_secs, last_secs)
                        {
                            let window_secs =
                                last_secs.saturating_sub(first_secs);
                            let remaining =
                                cursor_secs.saturating_sub(first_secs);
                            tracing::error!(
                                remaining_secs = remaining,
                                window_secs,
                                cursor_ts_secs = cursor_secs,
                                oplog_first_secs = first_secs,
                                oplog_last_secs = last_secs,
                                "OPLOG-008b: Cursor within 20% of oplog oldest \
                                 entry, risk of data loss from oplog wrap"
                            );
                        }
                    }

                    // Release durable entries (OPLOG-006/007)
                    if wm > 0 {
                        let (wm_secs, wm_ord) = decode_watermark(wm);

                        let keys_to_remove =
                            collect_releasable_keys(&buffer, wm_secs, wm_ord);
                        let mut released = Vec::new();

                        // OPLOG-063: collect update _id values for batched read-back
                        let mut update_ids: Vec<Bson> = Vec::new();
                        for &(ts_secs, ts_ord) in &keys_to_remove {
                            if let Some((entry, _)) = buffer.get(&(ts_secs, ts_ord)) {
                                if entry.get_str("op").unwrap_or("n") == "u" {
                                    if let Ok(o2) = entry.get_document("o2") {
                                        if let Some(id) = o2.get("_id") {
                                            update_ids.push(id.clone());
                                        }
                                    }
                                }
                            }
                        }

                        // Batched read-back: single find({_id:{$in:[...]}}) per chunk
                        let post_images = if !update_ids.is_empty() {
                            Self::batch_read_back_post_images(
                                &client,
                                &db_name,
                                &coll_name,
                                &update_ids,
                                readback_batch_max,
                            )
                            .await
                        } else {
                            HashMap::new()
                        };

                        for &(ts_secs, ts_ord) in &keys_to_remove {
                            if let Some((entry, offset_str)) =
                                buffer.get(&(ts_secs, ts_ord))
                            {
                                let op = entry.get_str("op").unwrap_or("n");
                                // OPLOG-063: look up post_image from batched result
                                let post_image = if op == "u" {
                                    entry
                                        .get_document("o2")
                                        .ok()
                                        .and_then(|o2| o2.get("_id"))
                                        .and_then(|id| post_images.get(&bson_to_key(id)).cloned())
                                } else {
                                    None
                                };

                                if let Some(msg) = oplog_entry_to_source_message(
                                    entry,
                                    post_image.as_ref(),
                                    rs_name,
                                    &db_name,
                                    &coll_name,
                                    split_id.clone(),
                                    offset_str.clone(),
                                )? {
                                    released.push(msg);
                                }

                                // Track last yielded for reconnect resume
                                // (OPLOG-017)
                                last_yielded_ts = Some(Timestamp {
                                    time: ts_secs,
                                    increment: ts_ord,
                                });
                            }
                        }

                        for key in &keys_to_remove {
                            if let Some((entry, _)) = buffer.remove(key) {
                                buffer_bytes = buffer_bytes
                                    .saturating_sub(estimate_doc_size(&entry));
                            }
                        }

                        if !released.is_empty() {
                            // OPLOG-022: metrics
                            self.source_ctx
                                .metrics
                                .oplog_entries_released_total
                                .with_guarded_label_values(&metrics_labels)
                                .inc_by(released.len() as u64);
                            self.source_ctx
                                .metrics
                                .oplog_buffer_entries
                                .with_guarded_label_values(&metrics_labels)
                                .set(buffer.len() as i64);
                            self.source_ctx
                                .metrics
                                .oplog_buffer_size_bytes
                                .with_guarded_label_values(&metrics_labels)
                                .set(buffer_bytes as i64);
                            last_yield_time = tokio::time::Instant::now();
                            yield released;
                        }
                    }
                }

                // Step 3: Heartbeat on idle (OPLOG-025)
                if tokio::time::Instant::now().duration_since(last_yield_time)
                    >= heartbeat_interval
                {
                    last_yield_time = tokio::time::Instant::now();
                    yield vec![];
                }
            }
            // End of inner poll loop — handle reconnect

            // OPLOG-019: Rollback logging on buffer discard
            if !buffer.is_empty() {
                tracing::warn!(
                    discarded_entries = buffer.len(),
                    discarded_bytes = buffer_bytes,
                    "OPLOG-019: Discarding uncommitted buffer entries on reconnect"
                );
                self.source_ctx
                    .metrics
                    .oplog_entries_discarded_total
                    .with_guarded_label_values(&metrics_labels)
                    .inc_by(buffer.len() as u64);
                self.source_ctx
                    .metrics
                    .oplog_rollbacks_total
                    .with_guarded_label_values(&metrics_labels)
                    .inc();
            }

            // OPLOG-008c/017: Handle the specific error
            if let Some(e) = cursor_error {
                tracing::error!(
                    error = %e,
                    "OPLOG-008c/017: Oplog cursor error, reconnecting"
                );

                // OPLOG-018: Discontinuity detection — if cursor was lost due
                // to oplog wrap (CursorNotFound), resume from the oldest
                // available entry to avoid a silent gap.
                if e.to_string().contains("CursorNotFound")
                    || e.to_string().contains("cursor id")
                {
                    tracing::error!(
                        "OPLOG-018: Oplog may have wrapped past cursor, \
                         resuming from oldest available entry"
                    );
                    match Self::get_oldest_oplog_ts(&client).await {
                        Ok(oldest_ts) => {
                            last_yielded_ts = Some(oldest_ts);
                        }
                        Err(e2) => {
                            tracing::error!(
                                error = %e2,
                                "Failed to get oldest oplog entry"
                            );
                        }
                    }
                }
            } else {
                // Cursor returned false (dead tailable cursor)
                tracing::warn!("OPLOG-017: Tailable cursor died, reconnecting");
            }

            self.source_ctx
                .metrics
                .oplog_reconnects_total
                .with_guarded_label_values(&metrics_labels)
                .inc();

            // Brief backoff before reconnect
            tokio::time::sleep(Duration::from_secs(1)).await;
            // Continue outer loop to reopen cursor
        }
    }

    /// Get the current oplog timestamp (latest entry) for snapshot start position.
    pub(crate) async fn get_current_oplog_ts(
        client: &mongodb::Client,
    ) -> ConnectorResult<Timestamp> {
        let oplog = client
            .database("local")
            .collection::<Document>("oplog.rs");

        let opts = FindOneOptions::builder()
            .sort(doc! { "$natural": -1 })
            .build();
        let latest = oplog.find_one(doc! {}, opts).await?;

        match latest {
            Some(entry) => {
                let ts = entry
                    .get_timestamp("ts")
                    .map_err(|e| anyhow::anyhow!("oplog entry missing 'ts': {}", e))?;
                Ok(ts)
            }
            None => {
                Err(anyhow::anyhow!("oplog is empty, cannot determine start position").into())
            }
        }
    }

    /// Get the oldest oplog timestamp for discontinuity recovery (OPLOG-018).
    pub(crate) async fn get_oldest_oplog_ts(
        client: &mongodb::Client,
    ) -> ConnectorResult<Timestamp> {
        let oplog = client
            .database("local")
            .collection::<Document>("oplog.rs");

        let opts = FindOneOptions::builder()
            .sort(doc! { "$natural": 1 })
            .build();
        let oldest = oplog.find_one(doc! {}, opts).await?;

        match oldest {
            Some(entry) => {
                let ts = entry.get_timestamp("ts").map_err(|e| {
                    anyhow::anyhow!("oldest oplog entry missing 'ts': {}", e)
                })?;
                Ok(ts)
            }
            None => Err(anyhow::anyhow!("oplog is empty").into()),
        }
    }

    /// Read-back for update operations to get the full post-image (OPLOG-032).
    /// Kept as fallback; the hot path now uses `batch_read_back_post_images`.
    #[allow(dead_code)]
    async fn read_back_post_image(
        client: &mongodb::Client,
        entry: &Document,
        db_name: &str,
        coll_name: &str,
    ) -> Option<Document> {
        let o2 = entry.get_document("o2").ok()?;
        let id = o2.get("_id")?;

        let coll = client
            .database(db_name)
            .collection::<Document>(coll_name);

        match coll.find_one(doc! { "_id": id.clone() }, None).await {
            Ok(found) => found,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to read-back post-image for update");
                None
            }
        }
    }

    /// Batched read-back for update operations (OPLOG-063).
    ///
    /// Issues `find({_id: {$in: [...]}})` queries in sub-batches of
    /// `batch_max` IDs. Returns a map from `_id` → full document.
    /// IDs not found (e.g., deleted between update and read-back) are
    /// logged at warn level and omitted from the result.
    pub(crate) async fn batch_read_back_post_images(
        client: &mongodb::Client,
        db_name: &str,
        coll_name: &str,
        ids: &[Bson],
        batch_max: usize,
    ) -> HashMap<String, Document> {
        use futures::TryStreamExt;

        let coll = client
            .database(db_name)
            .collection::<Document>(coll_name);

        let mut result = HashMap::with_capacity(ids.len());

        for chunk in ids.chunks(batch_max) {
            let filter = doc! { "_id": { "$in": chunk.to_vec() } };
            match coll.find(filter, None).await {
                Ok(cursor) => {
                    let docs: Vec<Document> = match cursor.try_collect().await {
                        Ok(docs) => docs,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                chunk_size = chunk.len(),
                                "OPLOG-063: Failed to collect batched read-back cursor"
                            );
                            continue;
                        }
                    };
                    for doc in docs {
                        if let Some(id) = doc.get("_id") {
                            result.insert(bson_to_key(id), doc.clone());
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        chunk_size = chunk.len(),
                        "OPLOG-063: Failed to issue batched read-back query"
                    );
                }
            }
        }

        // Log any IDs that were not found
        let missing = ids.len() - result.len();
        if missing > 0 {
            tracing::warn!(
                missing_count = missing,
                total = ids.len(),
                "OPLOG-063: Some update _id values not found during batched read-back \
                 (documents may have been deleted between update and read-back)"
            );
        }

        result
    }
}

/// Convert a BSON value to a deterministic string key for HashMap lookups.
///
/// Uses the canonical extended JSON representation which is unique and
/// deterministic for any given BSON value.
pub(crate) fn bson_to_key(bson: &Bson) -> String {
    bson.to_string()
}

/// Rough estimate of BSON document size in bytes for buffer management.
fn estimate_doc_size(doc: &Document) -> usize {
    mongodb::bson::to_vec(doc).map(|v| v.len()).unwrap_or(256)
}

/// Check if an oplog entry at `(ts_secs, ts_ord)` is durable relative to
/// the majority commit watermark `(wm_secs, wm_ord)` (OPLOG-006).
///
/// An entry is durable when `t_entry <= t_majority_commit`.
pub(crate) fn is_durable(ts_secs: u32, ts_ord: u32, wm_secs: u32, wm_ord: u32) -> bool {
    ts_secs < wm_secs || (ts_secs == wm_secs && ts_ord <= wm_ord)
}

/// Collect keys from `buffer` whose timestamps are at or before the watermark
/// `(wm_secs, wm_ord)` (OPLOG-006/007).
///
/// Returns keys in ascending timestamp order (BTreeMap iteration order).
pub(crate) fn collect_releasable_keys(
    buffer: &BTreeMap<(u32, u32), (Document, String)>,
    wm_secs: u32,
    wm_ord: u32,
) -> Vec<(u32, u32)> {
    let mut keys = Vec::new();
    for &(ts_secs, ts_ord) in buffer.keys() {
        if is_durable(ts_secs, ts_ord, wm_secs, wm_ord) {
            keys.push((ts_secs, ts_ord));
        } else {
            break; // BTreeMap is sorted; no later key can be durable
        }
    }
    keys
}

/// Check if the cursor position is within the danger zone relative to the
/// oplog window (OPLOG-008b).
///
/// Returns `true` when the cursor is within 20% of the oplog's oldest entry,
/// meaning the capped oplog could wrap past the cursor and cause data loss.
pub(crate) fn is_near_oplog_wrap(cursor_secs: u32, first_secs: u32, last_secs: u32) -> bool {
    let window_secs = last_secs.saturating_sub(first_secs);
    if window_secs == 0 {
        return false;
    }
    let remaining = cursor_secs.saturating_sub(first_secs);
    remaining * 100 < window_secs * 20
}

/// Rewrite a `SourceMessage`'s offset to include chunk state (OPLOG-050).
///
/// Deserializes the offset, sets `snapshot_chunks`, and re-serializes.
fn rewrite_offset_with_chunks(
    msg: &mut SourceMessage,
    chunks: &[SnapshotChunkState],
    snapshot_start_ts: Timestamp,
) {
    if let Ok(mut offset) = MongodbOplogOffset::from_json_str(msg.offset.as_ref()) {
        offset.snapshot_chunks = Some(chunks.to_vec());
        offset.oplog_ts_secs = snapshot_start_ts.time;
        offset.oplog_ts_ord = snapshot_start_ts.increment;
        msg.offset = offset.to_json_string().into();
    }
}

/// Compute quantile split indices from a sorted sample (OPLOG-048).
///
/// Given `total_ids` sorted samples and a desired `num_chunks`, returns
/// indices at `total_ids * k / num_chunks` for `k` in `1..num_chunks`.
/// If `total_ids < num_chunks`, clamps to `total_ids` chunks (returning
/// fewer boundaries).
pub(crate) fn compute_quantile_boundaries(total_ids: usize, num_chunks: usize) -> Vec<usize> {
    if num_chunks <= 1 || total_ids == 0 {
        return vec![];
    }
    let effective_chunks = num_chunks.min(total_ids);
    let mut indices = Vec::with_capacity(effective_chunks.saturating_sub(1));
    for k in 1..effective_chunks {
        indices.push(total_ids * k / effective_chunks);
    }
    indices
}

/// Convert a 12-byte ObjectId to a u128 for arithmetic (OPLOG-056).
///
/// The ObjectId bytes are placed in the lower 96 bits of the u128,
/// big-endian, so that numeric ordering matches ObjectId ordering.
pub(crate) fn objectid_to_u128(oid: &mongodb::bson::oid::ObjectId) -> u128 {
    let bytes = oid.bytes();
    let mut buf = [0u8; 16];
    // Place 12 bytes at offset 4 (big-endian, lower 96 bits)
    buf[4..16].copy_from_slice(&bytes);
    u128::from_be_bytes(buf)
}

/// Convert a u128 back to a 12-byte ObjectId (OPLOG-056).
///
/// Takes the lower 96 bits (bytes 4..16 of the big-endian representation).
pub(crate) fn u128_to_objectid(val: u128) -> mongodb::bson::oid::ObjectId {
    let buf = val.to_be_bytes();
    let mut bytes = [0u8; 12];
    bytes.copy_from_slice(&buf[4..16]);
    mongodb::bson::oid::ObjectId::from_bytes(bytes)
}

/// Compute N-1 synthetic split points between min_id and max_id (OPLOG-055).
///
/// Supported `_id` types (OPLOG-056/057):
/// - `ObjectId`: 12-byte → u128 arithmetic midpoints
/// - `Int32` / `Int64`: integer arithmetic midpoints
///
/// Returns `Err` for:
/// - Mixed types (min and max differ) — OPLOG-058
/// - Unsupported types (e.g., String, Boolean) — OPLOG-059
///
/// Returns `Ok(vec![])` when `num_chunks <= 1` or `min == max`.
pub(crate) fn compute_synthetic_splits(
    min_id: &Bson,
    max_id: &Bson,
    num_chunks: usize,
) -> ConnectorResult<Vec<Bson>> {
    if num_chunks <= 1 {
        return Ok(vec![]);
    }

    match (min_id, max_id) {
        (Bson::ObjectId(min_oid), Bson::ObjectId(max_oid)) => {
            let min_val = objectid_to_u128(min_oid);
            let max_val = objectid_to_u128(max_oid);
            if min_val >= max_val {
                return Ok(vec![]);
            }
            let mut splits = Vec::with_capacity(num_chunks - 1);
            for k in 1..num_chunks {
                let point = min_val + (max_val - min_val) * k as u128 / num_chunks as u128;
                splits.push(Bson::ObjectId(u128_to_objectid(point)));
            }
            Ok(splits)
        }
        (Bson::Int32(min_v), Bson::Int32(max_v)) => {
            if min_v >= max_v {
                return Ok(vec![]);
            }
            let range = (*max_v as i64) - (*min_v as i64);
            let mut splits = Vec::with_capacity(num_chunks - 1);
            for k in 1..num_chunks {
                let point = *min_v as i64 + range * k as i64 / num_chunks as i64;
                splits.push(Bson::Int32(point as i32));
            }
            Ok(splits)
        }
        (Bson::Int64(min_v), Bson::Int64(max_v)) => {
            if min_v >= max_v {
                return Ok(vec![]);
            }
            let min_val = *min_v as i128;
            let max_val = *max_v as i128;
            let range = max_val - min_val;
            let mut splits = Vec::with_capacity(num_chunks - 1);
            for k in 1..num_chunks {
                let point = min_val + range * k as i128 / num_chunks as i128;
                splits.push(Bson::Int64(point as i64));
            }
            Ok(splits)
        }
        _ if std::mem::discriminant(min_id) != std::mem::discriminant(max_id) => {
            // OPLOG-058: mixed _id types
            Err(anyhow::anyhow!(
                "min_max boundary mode requires homogeneous _id types, but found min={}, max={}. \
                 Set mongodb.snapshot.boundary_mode=sample to use $sample-based splitting instead.",
                bson_type_name(min_id),
                bson_type_name(max_id),
            )
            .into())
        }
        _ => {
            // OPLOG-059: unsupported same type (String, Boolean, etc.)
            Err(anyhow::anyhow!(
                "min_max boundary mode does not support _id type '{}'. \
                 Only ObjectId, Int32, and Int64 are supported. \
                 Set mongodb.snapshot.boundary_mode=sample to use $sample-based splitting instead.",
                bson_type_name(min_id),
            )
            .into())
        }
    }
}

/// Human-readable BSON type name for error messages.
fn bson_type_name(val: &Bson) -> &'static str {
    match val {
        Bson::Double(_) => "Double",
        Bson::String(_) => "String",
        Bson::Array(_) => "Array",
        Bson::Document(_) => "Document",
        Bson::Boolean(_) => "Boolean",
        Bson::Null => "Null",
        Bson::RegularExpression(_) => "RegularExpression",
        Bson::Int32(_) => "Int32",
        Bson::Int64(_) => "Int64",
        Bson::Timestamp(_) => "Timestamp",
        Bson::Binary(_) => "Binary",
        Bson::ObjectId(_) => "ObjectId",
        Bson::DateTime(_) => "DateTime",
        Bson::Symbol(_) => "Symbol",
        Bson::Decimal128(_) => "Decimal128",
        Bson::Undefined => "Undefined",
        Bson::MaxKey => "MaxKey",
        Bson::MinKey => "MinKey",
        Bson::DbPointer(_) => "DbPointer",
        Bson::JavaScriptCode(_) => "JavaScriptCode",
        Bson::JavaScriptCodeWithScope(_) => "JavaScriptCodeWithScope",
    }
}

/// Discover chunk boundaries via `$sample` aggregation (OPLOG-048/060).
///
/// Pipeline: `[{$sample: {size: sample_size}}, {$project: {_id: 1}}, {$sort: {_id: 1}}]`
///
/// The collection is divided into chunks of ~`chunk_target_docs` each.
/// `num_chunks = max(1, estimated_count / chunk_target_docs)`.
///
/// Deduplicates sampled IDs after sorting to handle SERVER-20385 duplicates (OPLOG-060).
/// Does not use `allowDiskUse`.
///
/// Returns `Vec<SnapshotChunkState>` with boundary ranges. First chunk
/// has `min_id=None`, last has `max_id=None`.
pub(crate) async fn discover_chunk_boundaries_sample(
    client: &mongodb::Client,
    db_name: &str,
    coll_name: &str,
    chunk_target_docs: u64,
) -> ConnectorResult<Vec<SnapshotChunkState>> {
    let coll = client
        .database(db_name)
        .collection::<Document>(coll_name);

    // Estimate total document count (O(1) metadata lookup on 3.6+)
    let estimated_count = coll.estimated_document_count(None).await? as u64;
    if estimated_count == 0 {
        return Ok(vec![single_chunk()]);
    }

    let num_chunks = std::cmp::max(1, estimated_count / chunk_target_docs) as usize;
    if num_chunks <= 1 {
        return Ok(vec![single_chunk()]);
    }

    // Sample enough IDs for good quantile estimates
    let sample_size = std::cmp::min(10 * num_chunks as u64, 100_000).max(num_chunks as u64);

    let pipeline = vec![
        doc! { "$sample": { "size": sample_size as i64 } },
        doc! { "$project": { "_id": 1 } },
        doc! { "$sort": { "_id": 1 } },
    ];

    let mut cursor = coll.aggregate(pipeline, None).await?;
    let mut sampled_ids: Vec<Bson> = Vec::with_capacity(sample_size as usize);

    while cursor.advance().await? {
        let doc = cursor.deserialize_current()?;
        if let Some(id) = doc.get("_id") {
            sampled_ids.push(id.clone());
        }
    }

    // OPLOG-060: dedup sampled IDs to handle SERVER-20385 duplicates
    sampled_ids.dedup();

    if sampled_ids.is_empty() {
        return Ok(vec![single_chunk()]);
    }

    let boundary_indices = compute_quantile_boundaries(sampled_ids.len(), num_chunks);

    // Convert boundary IDs to ExtJSON strings
    let boundary_ids: Vec<String> = boundary_indices
        .iter()
        .map(|&idx| {
            serde_json::to_string(&sampled_ids[idx].clone().into_canonical_extjson())
                .expect("BSON to ExtJSON serialization should not fail")
        })
        .collect();

    Ok(build_chunks_from_boundaries(&boundary_ids))
}

/// Discover chunk boundaries via min/max index lookups (OPLOG-055).
///
/// Uses two O(1) index-scan queries to find the min and max `_id`, then
/// generates N-1 synthetic arithmetic split points. Only supports
/// ObjectId, Int32, and Int64 `_id` types; fails loudly for others (OPLOG-058/059).
pub(crate) async fn discover_chunk_boundaries_min_max(
    client: &mongodb::Client,
    db_name: &str,
    coll_name: &str,
    chunk_target_docs: u64,
) -> ConnectorResult<Vec<SnapshotChunkState>> {
    let coll = client
        .database(db_name)
        .collection::<Document>(coll_name);

    // Estimate total document count (O(1) metadata lookup on 3.6+)
    let estimated_count = coll.estimated_document_count(None).await? as u64;
    if estimated_count == 0 {
        return Ok(vec![single_chunk()]);
    }

    let num_chunks = std::cmp::max(1, estimated_count / chunk_target_docs) as usize;
    if num_chunks <= 1 {
        return Ok(vec![single_chunk()]);
    }

    // Find min _id (index scan: sort {_id: 1} limit 1)
    let min_doc = coll
        .find_one(
            doc! {},
            FindOneOptions::builder().sort(doc! { "_id": 1 }).build(),
        )
        .await?;
    let min_id = min_doc
        .and_then(|d| d.get("_id").cloned())
        .ok_or_else(|| anyhow::anyhow!("min_max boundary discovery: collection has no _id field"))?;

    // Find max _id (index scan: sort {_id: -1} limit 1)
    let max_doc = coll
        .find_one(
            doc! {},
            FindOneOptions::builder().sort(doc! { "_id": -1 }).build(),
        )
        .await?;
    let max_id = max_doc
        .and_then(|d| d.get("_id").cloned())
        .ok_or_else(|| anyhow::anyhow!("min_max boundary discovery: collection has no _id field"))?;

    // Compute synthetic splits (OPLOG-056/057/058/059)
    let splits = compute_synthetic_splits(&min_id, &max_id, num_chunks)?;

    if splits.is_empty() {
        // min == max or 1 chunk
        return Ok(vec![single_chunk()]);
    }

    // Convert split points to ExtJSON strings
    let boundary_ids: Vec<String> = splits
        .into_iter()
        .map(|bson| {
            serde_json::to_string(&bson.into_canonical_extjson())
                .expect("BSON to ExtJSON serialization should not fail")
        })
        .collect();

    Ok(build_chunks_from_boundaries(&boundary_ids))
}

/// Dispatcher: choose boundary discovery mode based on config (OPLOG-054).
pub(crate) async fn discover_chunk_boundaries(
    client: &mongodb::Client,
    db_name: &str,
    coll_name: &str,
    chunk_target_docs: u64,
    boundary_mode: super::BoundaryMode,
) -> ConnectorResult<Vec<SnapshotChunkState>> {
    match boundary_mode {
        super::BoundaryMode::MinMax => {
            discover_chunk_boundaries_min_max(client, db_name, coll_name, chunk_target_docs).await
        }
        super::BoundaryMode::Sample => {
            discover_chunk_boundaries_sample(client, db_name, coll_name, chunk_target_docs).await
        }
    }
}

/// Single chunk covering the entire collection (empty or trivially small).
fn single_chunk() -> SnapshotChunkState {
    SnapshotChunkState {
        chunk_idx: 0,
        min_id: None,
        max_id: None,
        last_id: None,
        done: false,
    }
}

/// Build `Vec<SnapshotChunkState>` from boundary ID strings.
///
/// Creates chunks: `[None..b0], [b0..b1], ..., [bN..None]`.
fn build_chunks_from_boundaries(boundary_ids: &[String]) -> Vec<SnapshotChunkState> {
    let mut chunks = Vec::with_capacity(boundary_ids.len() + 1);
    for i in 0..=boundary_ids.len() {
        let min_id = if i == 0 {
            None
        } else {
            Some(boundary_ids[i - 1].clone())
        };
        let max_id = if i == boundary_ids.len() {
            None
        } else {
            Some(boundary_ids[i].clone())
        };
        chunks.push(SnapshotChunkState {
            chunk_idx: i,
            min_id,
            max_id,
            last_id: None,
            done: false,
        });
    }
    chunks
}

/// Determine whether a stall warning should be emitted (OPLOG-008a).
///
/// Returns `true` when the buffer is at capacity, the watermark hasn't advanced
/// for longer than `stall_timeout`, and enough time has passed since the last
/// warning.
pub(crate) fn should_emit_stall_warning(
    buffer_bytes: usize,
    buffer_max_bytes: usize,
    stall_elapsed: Duration,
    stall_timeout: Duration,
    since_last_warn: Option<Duration>,
) -> bool {
    if buffer_bytes < buffer_max_bytes {
        return false;
    }
    if stall_elapsed < stall_timeout {
        return false;
    }
    since_last_warn
        .map(|d| d >= stall_timeout)
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use mongodb::bson::{doc, Document};

    use super::*;

    fn make_buffer_entry(ts_secs: u32, ts_ord: u32) -> ((u32, u32), (Document, String)) {
        let entry = doc! {
            "ts": mongodb::bson::Timestamp { time: ts_secs, increment: ts_ord },
            "op": "i",
            "ns": "db.coll",
            "o": { "_id": format!("{}_{}", ts_secs, ts_ord) }
        };
        let offset = format!("offset_{}_{}", ts_secs, ts_ord);
        ((ts_secs, ts_ord), (entry, offset))
    }

    // ── OPLOG-006: Durability invariant ──────────────────────────────

    #[test]
    fn test_is_durable_entry_before_watermark() {
        // Entry at t=100:1, watermark at t=200:1 → durable
        assert!(is_durable(100, 1, 200, 1));
    }

    #[test]
    fn test_is_durable_entry_at_watermark() {
        // Entry exactly at watermark → durable
        assert!(is_durable(200, 5, 200, 5));
    }

    #[test]
    fn test_is_durable_entry_after_watermark_rejected() {
        // Entry at t=300:1, watermark at t=200:1 → NOT durable
        assert!(!is_durable(300, 1, 200, 1));
    }

    #[test]
    fn test_is_durable_same_second_higher_ord_rejected() {
        // Same second but higher ordinal → NOT durable
        assert!(!is_durable(200, 6, 200, 5));
    }

    #[test]
    fn test_is_durable_same_second_lower_ord_accepted() {
        // Same second, lower ordinal → durable
        assert!(is_durable(200, 3, 200, 5));
    }

    #[test]
    fn test_is_durable_zero_watermark() {
        // Watermark at 0:0 — only entries at 0:0 are durable
        assert!(is_durable(0, 0, 0, 0));
        assert!(!is_durable(1, 0, 0, 0));
    }

    // ── OPLOG-007: Batch release in timestamp order ──────────────────

    #[test]
    fn test_collect_releasable_keys_releases_up_to_watermark() {
        let mut buffer = BTreeMap::new();
        for (k, v) in [
            make_buffer_entry(100, 1),
            make_buffer_entry(100, 2),
            make_buffer_entry(200, 1),
            make_buffer_entry(200, 5),
            make_buffer_entry(300, 1),
        ] {
            buffer.insert(k, v);
        }

        // Watermark at (200, 5) should release 4 entries, not entry (300, 1)
        let keys = collect_releasable_keys(&buffer, 200, 5);
        assert_eq!(
            keys,
            vec![(100, 1), (100, 2), (200, 1), (200, 5)]
        );
    }

    #[test]
    fn test_collect_releasable_keys_in_ascending_order() {
        let mut buffer = BTreeMap::new();
        // Insert out of order — BTreeMap sorts them
        for (k, v) in [
            make_buffer_entry(300, 2),
            make_buffer_entry(100, 5),
            make_buffer_entry(200, 3),
            make_buffer_entry(100, 1),
        ] {
            buffer.insert(k, v);
        }

        let keys = collect_releasable_keys(&buffer, 999, 0);
        assert_eq!(
            keys,
            vec![(100, 1), (100, 5), (200, 3), (300, 2)],
            "released keys must be in strict ascending timestamp order"
        );
    }

    #[test]
    fn test_collect_releasable_keys_none_when_all_after_watermark() {
        let mut buffer = BTreeMap::new();
        for (k, v) in [
            make_buffer_entry(500, 1),
            make_buffer_entry(600, 1),
        ] {
            buffer.insert(k, v);
        }

        let keys = collect_releasable_keys(&buffer, 100, 0);
        assert!(keys.is_empty(), "no entries should be released when all are after watermark");
    }

    #[test]
    fn test_collect_releasable_keys_empty_buffer() {
        let buffer: BTreeMap<(u32, u32), (Document, String)> = BTreeMap::new();
        let keys = collect_releasable_keys(&buffer, 999, 999);
        assert!(keys.is_empty());
    }

    #[test]
    fn test_collect_releasable_keys_all_released() {
        let mut buffer = BTreeMap::new();
        for (k, v) in [
            make_buffer_entry(10, 1),
            make_buffer_entry(20, 1),
            make_buffer_entry(30, 1),
        ] {
            buffer.insert(k, v);
        }

        let keys = collect_releasable_keys(&buffer, 999, 0);
        assert_eq!(keys.len(), 3, "all entries should be released when watermark is far ahead");
    }

    #[test]
    fn test_no_dirty_entry_ever_released() {
        // Property: for any released key, is_durable must be true
        let mut buffer = BTreeMap::new();
        for ts in 1..=100u32 {
            let (k, v) = make_buffer_entry(ts, 1);
            buffer.insert(k, v);
        }

        let wm_secs = 50u32;
        let wm_ord = 1u32;
        let keys = collect_releasable_keys(&buffer, wm_secs, wm_ord);

        for (ts_secs, ts_ord) in &keys {
            assert!(
                is_durable(*ts_secs, *ts_ord, wm_secs, wm_ord),
                "released entry ({}, {}) should be durable at watermark ({}, {})",
                ts_secs,
                ts_ord,
                wm_secs,
                wm_ord,
            );
        }

        // Also verify no durable entry was left behind
        for &(ts_secs, ts_ord) in buffer.keys() {
            if !keys.contains(&(ts_secs, ts_ord)) {
                assert!(
                    !is_durable(ts_secs, ts_ord, wm_secs, wm_ord),
                    "unreleased entry ({}, {}) should NOT be durable",
                    ts_secs,
                    ts_ord,
                );
            }
        }
    }

    // ── OPLOG-008a: Stall detection ──────────────────────────────────

    #[test]
    fn test_stall_warning_when_buffer_full_and_watermark_stuck() {
        assert!(should_emit_stall_warning(
            100_000_000,           // buffer_bytes (full)
            100_000_000,           // buffer_max_bytes
            Duration::from_secs(31), // stall_elapsed > timeout
            Duration::from_secs(30), // stall_timeout
            None,                  // no previous warning
        ));
    }

    #[test]
    fn test_no_stall_warning_when_buffer_below_max() {
        assert!(!should_emit_stall_warning(
            50_000_000,
            100_000_000,
            Duration::from_secs(60),
            Duration::from_secs(30),
            None,
        ));
    }

    #[test]
    fn test_no_stall_warning_when_watermark_recently_advanced() {
        assert!(!should_emit_stall_warning(
            100_000_000,
            100_000_000,
            Duration::from_secs(5), // only 5s stalled, timeout is 30s
            Duration::from_secs(30),
            None,
        ));
    }

    #[test]
    fn test_stall_warning_suppressed_until_next_interval() {
        // Last warning was 10s ago, timeout is 30s → suppress
        assert!(!should_emit_stall_warning(
            100_000_000,
            100_000_000,
            Duration::from_secs(60),
            Duration::from_secs(30),
            Some(Duration::from_secs(10)),
        ));
    }

    #[test]
    fn test_stall_warning_fires_at_next_interval() {
        // Last warning was 30s ago, timeout is 30s → fire
        assert!(should_emit_stall_warning(
            100_000_000,
            100_000_000,
            Duration::from_secs(60),
            Duration::from_secs(30),
            Some(Duration::from_secs(30)),
        ));
    }

    // ── OPLOG-008b: Oplog window danger zone ─────────────────────────

    #[test]
    fn test_near_wrap_at_boundary() {
        // Window: first=1000, last=2000 (1000s window)
        // Cursor at 1100 → remaining=100, 10% of window → DANGER
        assert!(is_near_oplog_wrap(1100, 1000, 2000));
    }

    #[test]
    fn test_not_near_wrap_when_well_ahead() {
        // Cursor at 1800 → remaining=800, 80% of window → safe
        assert!(!is_near_oplog_wrap(1800, 1000, 2000));
    }

    #[test]
    fn test_near_wrap_at_exactly_20_percent() {
        // Window: 0..1000 (1000s). 20% threshold = 200.
        // Cursor at 200 → remaining=200, 200*100=20000, window*20=20000
        // 20000 < 20000 is FALSE → not in danger zone (boundary is exclusive)
        assert!(!is_near_oplog_wrap(200, 0, 1000));
        // Cursor at 199 → 19900 < 20000 → danger
        assert!(is_near_oplog_wrap(199, 0, 1000));
    }

    #[test]
    fn test_near_wrap_cursor_at_oldest() {
        // Cursor exactly at first → remaining=0, always dangerous
        assert!(is_near_oplog_wrap(1000, 1000, 2000));
    }

    #[test]
    fn test_not_near_wrap_zero_window() {
        // first == last → window is 0, no danger (degenerate case)
        assert!(!is_near_oplog_wrap(500, 500, 500));
    }

    #[test]
    fn test_not_near_wrap_cursor_past_last() {
        // Cursor ahead of last (shouldn't happen, but safe)
        assert!(!is_near_oplog_wrap(3000, 1000, 2000));
    }

    // ── OPLOG-053: Oplog window guard during snapshot ───────────────────

    #[test]
    fn test_snapshot_ts_wrap_detected_by_is_near_oplog_wrap() {
        // Scenario: snapshot started at t=500, oplog window is now 900..2000.
        // snapshot_start_ts (500) < oldest (900) → already wrapped.
        // is_near_oplog_wrap returns true because remaining is negative (saturates to 0).
        assert!(is_near_oplog_wrap(500, 900, 2000));
    }

    #[test]
    fn test_snapshot_ts_in_danger_zone() {
        // Scenario: snapshot started at t=1050, oplog window is 1000..2000.
        // remaining = 50, window = 1000, 50/1000 = 5% → within 20% danger zone.
        assert!(is_near_oplog_wrap(1050, 1000, 2000));
    }

    #[test]
    fn test_snapshot_ts_safe_during_snapshot() {
        // Scenario: snapshot started at t=1800, oplog window is 1000..2000.
        // remaining = 800, 80% → safe.
        assert!(!is_near_oplog_wrap(1800, 1000, 2000));
    }

    // ── OPLOG-048: Quantile boundary computation ────────────────────────

    #[test]
    fn test_quantile_boundaries_4_chunks_20_samples() {
        // 20 samples / 4 chunks → boundaries at indices 5, 10, 15
        let indices = compute_quantile_boundaries(20, 4);
        assert_eq!(indices, vec![5, 10, 15]);
    }

    #[test]
    fn test_quantile_boundaries_1_chunk() {
        // 1 chunk → no split boundaries needed
        let indices = compute_quantile_boundaries(100, 1);
        assert!(indices.is_empty());
    }

    #[test]
    fn test_quantile_boundaries_more_chunks_than_samples() {
        // 3 samples but 10 chunks → clamp to 3 chunks, boundaries at [1, 2]
        let indices = compute_quantile_boundaries(3, 10);
        assert_eq!(indices, vec![1, 2]);
    }

    #[test]
    fn test_quantile_boundaries_equal_chunks_and_samples() {
        // 4 samples, 4 chunks → boundaries at [1, 2, 3]
        let indices = compute_quantile_boundaries(4, 4);
        assert_eq!(indices, vec![1, 2, 3]);
    }

    #[test]
    fn test_quantile_boundaries_zero_samples() {
        let indices = compute_quantile_boundaries(0, 4);
        assert!(indices.is_empty());
    }

    #[test]
    fn test_quantile_boundaries_zero_chunks() {
        let indices = compute_quantile_boundaries(20, 0);
        assert!(indices.is_empty());
    }

    // ── OPLOG-056: ObjectId ↔ u128 conversion ──────────────────────────

    #[test]
    fn test_objectid_to_u128_roundtrip() {
        let oid = mongodb::bson::oid::ObjectId::from_bytes([
            0x65, 0xbc, 0x9f, 0xb6, 0xc4, 0x85, 0xf4, 0x19, 0xa7, 0xa8, 0x77, 0xfe,
        ]);
        let val = objectid_to_u128(&oid);
        let restored = u128_to_objectid(val);
        assert_eq!(oid, restored, "roundtrip must preserve ObjectId");
    }

    #[test]
    fn test_objectid_to_u128_all_zeros() {
        let oid = mongodb::bson::oid::ObjectId::from_bytes([0u8; 12]);
        let val = objectid_to_u128(&oid);
        assert_eq!(val, 0u128);
        assert_eq!(u128_to_objectid(val), oid);
    }

    #[test]
    fn test_objectid_to_u128_all_ff() {
        let oid = mongodb::bson::oid::ObjectId::from_bytes([0xffu8; 12]);
        let val = objectid_to_u128(&oid);
        // 12 bytes of 0xff = 2^96 - 1
        let expected: u128 = (1u128 << 96) - 1;
        assert_eq!(val, expected);
        assert_eq!(u128_to_objectid(val), oid);
    }

    #[test]
    fn test_objectid_to_u128_midpoint_is_between() {
        let low = mongodb::bson::oid::ObjectId::from_bytes([0x00; 12]);
        let high = mongodb::bson::oid::ObjectId::from_bytes([0xff; 12]);
        let low_val = objectid_to_u128(&low);
        let high_val = objectid_to_u128(&high);
        let mid_val = low_val + (high_val - low_val) / 2;
        assert!(mid_val > low_val);
        assert!(mid_val < high_val);
        // Roundtrip the midpoint
        let mid_oid = u128_to_objectid(mid_val);
        assert_eq!(objectid_to_u128(&mid_oid), mid_val);
    }

    #[test]
    fn test_objectid_to_u128_preserves_ordering() {
        let a = mongodb::bson::oid::ObjectId::from_bytes([
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);
        let b = mongodb::bson::oid::ObjectId::from_bytes([
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);
        assert!(objectid_to_u128(&a) < objectid_to_u128(&b));
    }

    // ── OPLOG-055/056/057/058/059: compute_synthetic_splits ────────────

    /// Extract ObjectId from Bson for test assertions.
    fn unwrap_oid(b: &Bson) -> &mongodb::bson::oid::ObjectId {
        match b {
            Bson::ObjectId(oid) => oid,
            other => panic!("expected ObjectId, got {:?}", other),
        }
    }

    #[test]
    fn test_synthetic_splits_objectid_basic() {
        // OPLOG-056: ObjectId arithmetic midpoints
        let min_oid = mongodb::bson::oid::ObjectId::from_bytes([0x00; 12]);
        let max_oid = mongodb::bson::oid::ObjectId::from_bytes([0xff; 12]);
        let min = Bson::ObjectId(min_oid);
        let max = Bson::ObjectId(max_oid);
        let splits = compute_synthetic_splits(&min, &max, 4).unwrap();
        assert_eq!(splits.len(), 3, "4 chunks → 3 boundary points");
        // Boundaries should be in ascending order (compare via u128)
        let vals: Vec<u128> = splits.iter().map(|s| objectid_to_u128(unwrap_oid(s))).collect();
        for i in 0..vals.len() - 1 {
            assert!(vals[i] < vals[i + 1], "boundaries must be ascending");
        }
        // All boundaries should be between min and max
        let min_val = objectid_to_u128(&min_oid);
        let max_val = objectid_to_u128(&max_oid);
        for v in &vals {
            assert!(*v > min_val);
            assert!(*v < max_val);
        }
    }

    #[test]
    fn test_synthetic_splits_objectid_2_chunks() {
        // 2 chunks → 1 midpoint
        let min = Bson::ObjectId(mongodb::bson::oid::ObjectId::from_bytes([0x00; 12]));
        let max = Bson::ObjectId(mongodb::bson::oid::ObjectId::from_bytes([0x10; 12]));
        let splits = compute_synthetic_splits(&min, &max, 2).unwrap();
        assert_eq!(splits.len(), 1);
        // Midpoint should be 0x08...
        if let Bson::ObjectId(mid_oid) = &splits[0] {
            assert_eq!(mid_oid.bytes()[0], 0x08);
        } else {
            panic!("expected ObjectId");
        }
    }

    #[test]
    fn test_synthetic_splits_int32() {
        // OPLOG-057: Int32 arithmetic midpoints
        let min = Bson::Int32(0);
        let max = Bson::Int32(100);
        let splits = compute_synthetic_splits(&min, &max, 4).unwrap();
        assert_eq!(splits.len(), 3);
        assert_eq!(splits[0], Bson::Int32(25));
        assert_eq!(splits[1], Bson::Int32(50));
        assert_eq!(splits[2], Bson::Int32(75));
    }

    #[test]
    fn test_synthetic_splits_int64() {
        // OPLOG-057: Int64 arithmetic midpoints
        let min = Bson::Int64(0);
        let max = Bson::Int64(1000);
        let splits = compute_synthetic_splits(&min, &max, 5).unwrap();
        assert_eq!(splits.len(), 4);
        assert_eq!(splits[0], Bson::Int64(200));
        assert_eq!(splits[1], Bson::Int64(400));
        assert_eq!(splits[2], Bson::Int64(600));
        assert_eq!(splits[3], Bson::Int64(800));
    }

    #[test]
    fn test_synthetic_splits_mixed_types_error() {
        // OPLOG-058: mixed _id types → error
        let min = Bson::ObjectId(mongodb::bson::oid::ObjectId::from_bytes([0x00; 12]));
        let max = Bson::Int32(100);
        let result = compute_synthetic_splits(&min, &max, 4);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("sample"),
            "error should suggest using sample mode: {}",
            err_msg
        );
    }

    #[test]
    fn test_synthetic_splits_unsupported_type_error() {
        // OPLOG-059: unsupported _id type (String) → hard error
        let min = Bson::String("aaa".to_owned());
        let max = Bson::String("zzz".to_owned());
        let result = compute_synthetic_splits(&min, &max, 4);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("sample"),
            "error should suggest using sample mode: {}",
            err_msg
        );
    }

    #[test]
    fn test_synthetic_splits_min_equals_max() {
        // Edge case: min == max → 0 splits (single element collection)
        let val = Bson::Int32(42);
        let splits = compute_synthetic_splits(&val, &val, 4).unwrap();
        assert!(splits.is_empty(), "min == max should produce no splits");
    }

    #[test]
    fn test_synthetic_splits_1_chunk() {
        // 1 chunk → 0 split boundaries
        let min = Bson::Int32(0);
        let max = Bson::Int32(100);
        let splits = compute_synthetic_splits(&min, &max, 1).unwrap();
        assert!(splits.is_empty());
    }

    #[test]
    fn test_synthetic_splits_int32_negative_range() {
        let min = Bson::Int32(-100);
        let max = Bson::Int32(100);
        let splits = compute_synthetic_splits(&min, &max, 4).unwrap();
        assert_eq!(splits.len(), 3);
        assert_eq!(splits[0], Bson::Int32(-50));
        assert_eq!(splits[1], Bson::Int32(0));
        assert_eq!(splits[2], Bson::Int32(50));
    }

    // ── OPLOG-050: Offset rewriting with chunk state ────────────────────

    #[test]
    fn test_rewrite_offset_with_chunks() {
        use super::SnapshotChunkState;

        let offset = MongodbOplogOffset {
            snapshot_done: false,
            snapshot_last_id: Some("\"id_5\"".to_owned()),
            oplog_ts_secs: 100,
            oplog_ts_ord: 1,
            snapshot_chunks: None,
        };

        let mut msg = SourceMessage {
            key: None,
            payload: None,
            offset: offset.to_json_string().into(),
            split_id: "0".into(),
            meta: crate::source::SourceMeta::Empty,
        };

        let chunks = vec![
            SnapshotChunkState {
                chunk_idx: 0,
                min_id: None,
                max_id: Some("\"id_50\"".to_owned()),
                last_id: Some("\"id_5\"".to_owned()),
                done: false,
            },
            SnapshotChunkState {
                chunk_idx: 1,
                min_id: Some("\"id_50\"".to_owned()),
                max_id: None,
                last_id: None,
                done: true,
            },
        ];

        let ts = Timestamp {
            time: 200,
            increment: 2,
        };
        rewrite_offset_with_chunks(&mut msg, &chunks, ts);

        let restored = MongodbOplogOffset::from_json_str(msg.offset.as_ref()).unwrap();
        assert_eq!(restored.snapshot_chunks.as_ref().unwrap().len(), 2);
        assert_eq!(restored.oplog_ts_secs, 200);
        assert_eq!(restored.oplog_ts_ord, 2);
        assert!(restored.snapshot_chunks.as_ref().unwrap()[1].done);
    }

    // ── OPLOG-063: bson_to_key determinism ──────────────────────────

    #[test]
    fn test_bson_to_key_objectid_deterministic() {
        use mongodb::bson::oid::ObjectId;
        let oid = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let bson = Bson::ObjectId(oid);
        let k1 = bson_to_key(&bson);
        let k2 = bson_to_key(&bson);
        assert_eq!(k1, k2);
        assert!(!k1.is_empty());
    }

    #[test]
    fn test_bson_to_key_different_types_differ() {
        let int_key = bson_to_key(&Bson::Int32(42));
        let str_key = bson_to_key(&Bson::String("42".to_owned()));
        assert_ne!(int_key, str_key, "int32(42) and string(\"42\") must have different keys");
    }

    #[test]
    fn test_bson_to_key_same_value_same_key() {
        let a = bson_to_key(&Bson::Int64(999));
        let b = bson_to_key(&Bson::Int64(999));
        assert_eq!(a, b);
    }
}
