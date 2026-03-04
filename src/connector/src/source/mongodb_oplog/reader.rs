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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_async_stream::try_stream;
use mongodb::Cursor;
use mongodb::bson::{Bson, Document, Timestamp, doc};
use mongodb::options::{CursorType, FindOneOptions, FindOptions, ReadConcern};

use super::MongodbOplogProperties;
use super::message::{oplog_entry_to_source_message, snapshot_doc_to_source_message};
use super::oplog_types::{decode_watermark, encode_watermark};
use super::split::{MongodbOplogOffset, MongodbOplogSplit};
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

    /// Run snapshot scan with retry on transient cursor failures.
    ///
    /// Uses the `snapshot_last_id` checkpoint mechanism for resumability: on a
    /// cursor error the scan re-opens from the last successfully processed `_id`,
    /// avoiding duplicate documents. Retries use exponential backoff consistent
    /// with other connectors (Kinesis, NATS).
    pub(crate) async fn run_snapshot(
        client: &mongodb::Client,
        db_name: &str,
        coll_name: &str,
        rs_name: &str,
        split_id: &SplitId,
        batch_size: usize,
        snapshot_start_ts: Timestamp,
        resume_id: Option<String>,
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
                            all_batches.push(std::mem::take(&mut batch));
                            batch = Vec::with_capacity(batch_size);
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
                let resume_id = o.snapshot_last_id.clone();
                let batch_size = self.properties.snapshot_batch_size as usize;

                let batches = Self::run_snapshot(
                    &client,
                    &db_name,
                    &coll_name,
                    rs_name,
                    &split_id,
                    batch_size,
                    snapshot_start_ts,
                    resume_id,
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

                let batches = Self::run_snapshot(
                    &client,
                    &db_name,
                    &coll_name,
                    rs_name,
                    &split_id,
                    batch_size,
                    snapshot_start_ts,
                    None,
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

        // Metrics labels (OPLOG-022)
        let metrics_labels = [
            self.source_ctx.source_id.to_string(),
            self.source_ctx.source_name.clone(),
            self.source_ctx.fragment_id.to_string(),
            split_id.to_string(),
        ];

        // Spawn watermark poller (OPLOG-004, OPLOG-008b)
        let poll_interval =
            Duration::from_millis(self.properties.watermark_poll_interval_ms);
        let watermark_state = Self::spawn_watermark_poller(&client, poll_interval);

        let buffer_max_bytes = self.properties.buffer_max_bytes as usize;
        let heartbeat_interval =
            Duration::from_secs(self.properties.heartbeat_interval_secs);

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

                        for &(ts_secs, ts_ord) in &keys_to_remove {
                            if let Some((entry, offset_str)) =
                                buffer.get(&(ts_secs, ts_ord))
                            {
                                let op = entry.get_str("op").unwrap_or("n");
                                let post_image = if op == "u" {
                                    // OPLOG-032: read-back for updates
                                    Self::read_back_post_image(
                                        &client, entry, &db_name, &coll_name,
                                    )
                                    .await
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
}
