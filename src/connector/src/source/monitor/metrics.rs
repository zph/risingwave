// Copyright 2023 RisingWave Labs
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

use std::sync::{Arc, LazyLock};

use prometheus::{Registry, exponential_buckets, histogram_opts};
use risingwave_common::metrics::{
    LabelGuardedHistogramVec, LabelGuardedIntCounterVec, LabelGuardedIntGaugeVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::{
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry,
};

use crate::source::kafka::stats::RdKafkaStats;

#[derive(Debug, Clone)]
pub struct EnumeratorMetrics {
    pub high_watermark: LabelGuardedIntGaugeVec,
    /// PostgreSQL CDC confirmed flush LSN monitoring
    pub pg_cdc_confirmed_flush_lsn: LabelGuardedIntGaugeVec,
    /// PostgreSQL CDC upstream max LSN monitoring
    pub pg_cdc_upstream_max_lsn: LabelGuardedIntGaugeVec,
    /// MySQL CDC binlog file sequence number (min)
    pub mysql_cdc_binlog_file_seq_min: LabelGuardedIntGaugeVec,
    /// MySQL CDC binlog file sequence number (max)
    pub mysql_cdc_binlog_file_seq_max: LabelGuardedIntGaugeVec,
}

pub static GLOBAL_ENUMERATOR_METRICS: LazyLock<EnumeratorMetrics> =
    LazyLock::new(|| EnumeratorMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl EnumeratorMetrics {
    fn new(registry: &Registry) -> Self {
        let high_watermark = register_guarded_int_gauge_vec_with_registry!(
            "source_kafka_high_watermark",
            "High watermark for a exec per partition",
            &["source_id", "partition"],
            registry,
        )
        .unwrap();

        let pg_cdc_confirmed_flush_lsn = register_guarded_int_gauge_vec_with_registry!(
            "pg_cdc_confirmed_flush_lsn",
            "PostgreSQL CDC confirmed flush LSN",
            &["source_id", "slot_name"],
            registry,
        )
        .unwrap();

        let pg_cdc_upstream_max_lsn = register_guarded_int_gauge_vec_with_registry!(
            "pg_cdc_upstream_max_lsn",
            "PostgreSQL CDC upstream max LSN (pg_current_wal_lsn)",
            &["source_id", "slot_name"],
            registry,
        )
        .unwrap();

        let mysql_cdc_binlog_file_seq_min = register_guarded_int_gauge_vec_with_registry!(
            "mysql_cdc_binlog_file_seq_min",
            "MySQL CDC upstream binlog file sequence number (minimum/oldest)",
            &["hostname", "port"],
            registry,
        )
        .unwrap();

        let mysql_cdc_binlog_file_seq_max = register_guarded_int_gauge_vec_with_registry!(
            "mysql_cdc_binlog_file_seq_max",
            "MySQL CDC upstream binlog file sequence number (maximum/newest)",
            &["hostname", "port"],
            registry,
        )
        .unwrap();

        EnumeratorMetrics {
            high_watermark,
            pg_cdc_confirmed_flush_lsn,
            pg_cdc_upstream_max_lsn,
            mysql_cdc_binlog_file_seq_min,
            mysql_cdc_binlog_file_seq_max,
        }
    }

    pub fn unused() -> Self {
        Default::default()
    }
}

impl Default for EnumeratorMetrics {
    fn default() -> Self {
        GLOBAL_ENUMERATOR_METRICS.clone()
    }
}

#[derive(Debug, Clone)]
pub struct SourceMetrics {
    pub partition_input_count: LabelGuardedIntCounterVec,

    // **Note**: for normal messages, the metric is the message's payload size.
    // For messages from load generator, the metric is the size of stream chunk.
    pub partition_input_bytes: LabelGuardedIntCounterVec,
    /// Report latest message id
    pub latest_message_id: LabelGuardedIntGaugeVec,
    pub rdkafka_native_metric: Arc<RdKafkaStats>,

    pub direct_cdc_event_lag_latency: LabelGuardedHistogramVec,

    pub parquet_source_skip_row_count: LabelGuardedIntCounterVec,
    pub file_source_input_row_count: LabelGuardedIntCounterVec,
    pub file_source_dirty_split_count: LabelGuardedIntGaugeVec,
    pub file_source_failed_split_count: LabelGuardedIntCounterVec,

    // kinesis source
    pub kinesis_throughput_exceeded_count: LabelGuardedIntCounterVec,
    pub kinesis_timeout_count: LabelGuardedIntCounterVec,
    pub kinesis_rebuild_shard_iter_count: LabelGuardedIntCounterVec,
    pub kinesis_early_terminate_shard_count: LabelGuardedIntCounterVec,
    pub kinesis_lag_latency_ms: LabelGuardedHistogramVec,

    // mongo-oplog source (OPLOG-022)
    pub oplog_entries_read_total: LabelGuardedIntCounterVec,
    pub oplog_entries_released_total: LabelGuardedIntCounterVec,
    pub oplog_entries_discarded_total: LabelGuardedIntCounterVec,
    pub oplog_buffer_size_bytes: LabelGuardedIntGaugeVec,
    pub oplog_buffer_entries: LabelGuardedIntGaugeVec,
    pub oplog_high_watermark_ts: LabelGuardedIntGaugeVec,
    pub oplog_tail_ts: LabelGuardedIntGaugeVec,
    pub oplog_lag_seconds: LabelGuardedIntGaugeVec,
    pub oplog_rollbacks_total: LabelGuardedIntCounterVec,
    pub oplog_reconnects_total: LabelGuardedIntCounterVec,

    // mongo-oplog snapshot phase (OPLOG-053)
    pub oplog_snapshot_window_remaining_pct: LabelGuardedIntGaugeVec,
    pub oplog_snapshot_docs_total: LabelGuardedIntCounterVec,

    // mongo-oplog chunked snapshot (OPLOG-052)
    pub oplog_snapshot_chunks_total: LabelGuardedIntGaugeVec,
    pub oplog_snapshot_chunks_done: LabelGuardedIntGaugeVec,
    pub oplog_snapshot_docs_per_chunk: LabelGuardedHistogramVec,
}

pub static GLOBAL_SOURCE_METRICS: LazyLock<SourceMetrics> =
    LazyLock::new(|| SourceMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl SourceMetrics {
    fn new(registry: &Registry) -> Self {
        let partition_input_count = register_guarded_int_counter_vec_with_registry!(
            "source_partition_input_count",
            "Total number of rows that have been input from specific partition",
            &[
                "actor_id",
                "source_id",
                "partition",
                "source_name",
                "fragment_id"
            ],
            registry
        )
        .unwrap();
        let partition_input_bytes = register_guarded_int_counter_vec_with_registry!(
            "source_partition_input_bytes",
            "Total bytes that have been input from specific partition",
            &[
                "actor_id",
                "source_id",
                "partition",
                "source_name",
                "fragment_id"
            ],
            registry
        )
        .unwrap();
        let latest_message_id = register_guarded_int_gauge_vec_with_registry!(
            "source_latest_message_id",
            "Latest message id for a exec per partition",
            &["source_id", "actor_id", "partition"],
            registry,
        )
        .unwrap();

        let opts = histogram_opts!(
            "source_cdc_event_lag_duration_milliseconds",
            "source_cdc_lag_latency",
            exponential_buckets(1.0, 2.0, 21).unwrap(), // max 1048s
        );

        let parquet_source_skip_row_count = register_guarded_int_counter_vec_with_registry!(
            "parquet_source_skip_row_count",
            "Total number of rows that have been set to null in parquet source",
            &["actor_id", "source_id", "source_name", "fragment_id"],
            registry
        )
        .unwrap();

        let direct_cdc_event_lag_latency =
            register_guarded_histogram_vec_with_registry!(opts, &["table_name"], registry).unwrap();

        let rdkafka_native_metric = Arc::new(RdKafkaStats::new(registry.clone()));

        let file_source_input_row_count = register_guarded_int_counter_vec_with_registry!(
            "file_source_input_row_count",
            "Total number of rows that have been read in file source",
            &["source_id", "source_name", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();
        let file_source_dirty_split_count = register_guarded_int_gauge_vec_with_registry!(
            "file_source_dirty_split_count",
            "Current number of dirty file splits in file source",
            &["source_id", "source_name", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();
        let file_source_failed_split_count = register_guarded_int_counter_vec_with_registry!(
            "file_source_failed_split_count",
            "Total number of file splits marked dirty in file source",
            &["source_id", "source_name", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let kinesis_throughput_exceeded_count = register_guarded_int_counter_vec_with_registry!(
            "kinesis_throughput_exceeded_count",
            "Total number of times throughput exceeded in kinesis source",
            &["source_id", "source_name", "fragment_id", "shard_id"],
            registry
        )
        .unwrap();

        let kinesis_timeout_count = register_guarded_int_counter_vec_with_registry!(
            "kinesis_timeout_count",
            "Total number of times timeout in kinesis source",
            &["source_id", "source_name", "fragment_id", "shard_id"],
            registry
        )
        .unwrap();

        let kinesis_rebuild_shard_iter_count = register_guarded_int_counter_vec_with_registry!(
            "kinesis_rebuild_shard_iter_count",
            "Total number of times rebuild shard iter in kinesis source",
            &["source_id", "source_name", "fragment_id", "shard_id"],
            registry
        )
        .unwrap();

        let kinesis_early_terminate_shard_count = register_guarded_int_counter_vec_with_registry!(
            "kinesis_early_terminate_shard_count",
            "Total number of times early terminate shard in kinesis source",
            &["source_id", "source_name", "fragment_id", "shard_id"],
            registry
        )
        .unwrap();

        let kinesis_lag_latency_ms = register_guarded_histogram_vec_with_registry!(
            "kinesis_lag_latency_ms",
            "Lag latency in kinesis source",
            &["source_id", "source_name", "fragment_id", "shard_id"],
            registry
        )
        .unwrap();

        let oplog_entries_read_total = register_guarded_int_counter_vec_with_registry!(
            "oplog_entries_read_total",
            "Total oplog entries read from cursor",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_entries_released_total = register_guarded_int_counter_vec_with_registry!(
            "oplog_entries_released_total",
            "Total entries released downstream after watermark check",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_entries_discarded_total = register_guarded_int_counter_vec_with_registry!(
            "oplog_entries_discarded_total",
            "Total entries discarded due to rollback or reconnect",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_buffer_size_bytes = register_guarded_int_gauge_vec_with_registry!(
            "oplog_buffer_size_bytes",
            "Current oplog buffer size in bytes",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_buffer_entries = register_guarded_int_gauge_vec_with_registry!(
            "oplog_buffer_entries",
            "Current number of buffered oplog entries",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_high_watermark_ts = register_guarded_int_gauge_vec_with_registry!(
            "oplog_high_watermark_ts",
            "Current majority commit point timestamp seconds",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_tail_ts = register_guarded_int_gauge_vec_with_registry!(
            "oplog_tail_ts",
            "Timestamp of most recent oplog entry read",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_lag_seconds = register_guarded_int_gauge_vec_with_registry!(
            "oplog_lag_seconds",
            "Lag between tail position and high watermark in seconds",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_rollbacks_total = register_guarded_int_counter_vec_with_registry!(
            "oplog_rollbacks_total",
            "Total rollback events detected",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_reconnects_total = register_guarded_int_counter_vec_with_registry!(
            "oplog_reconnects_total",
            "Total MongoDB reconnection events",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_snapshot_window_remaining_pct = register_guarded_int_gauge_vec_with_registry!(
            "oplog_snapshot_window_remaining_pct",
            "Percentage of oplog window remaining relative to snapshot_start_ts (0-100, 0 = wrapped)",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_snapshot_docs_total = register_guarded_int_counter_vec_with_registry!(
            "oplog_snapshot_docs_total",
            "Total documents read during snapshot phase",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_snapshot_chunks_total = register_guarded_int_gauge_vec_with_registry!(
            "oplog_snapshot_chunks_total",
            "Total number of snapshot chunks for parallel backfill (OPLOG-052)",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_snapshot_chunks_done = register_guarded_int_gauge_vec_with_registry!(
            "oplog_snapshot_chunks_done",
            "Number of completed snapshot chunks (OPLOG-052)",
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        let oplog_snapshot_docs_per_chunk_opts = histogram_opts!(
            "oplog_snapshot_docs_per_chunk",
            "Documents processed per snapshot chunk (OPLOG-052)",
            vec![100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0, 100_000.0, 500_000.0, 1_000_000.0],
        );
        let oplog_snapshot_docs_per_chunk = register_guarded_histogram_vec_with_registry!(
            oplog_snapshot_docs_per_chunk_opts,
            &["source_id", "source_name", "fragment_id", "split_id"],
            registry
        )
        .unwrap();

        SourceMetrics {
            partition_input_count,
            partition_input_bytes,
            latest_message_id,
            rdkafka_native_metric,
            direct_cdc_event_lag_latency,
            parquet_source_skip_row_count,
            file_source_input_row_count,
            file_source_dirty_split_count,
            file_source_failed_split_count,

            kinesis_throughput_exceeded_count,
            kinesis_timeout_count,
            kinesis_rebuild_shard_iter_count,
            kinesis_early_terminate_shard_count,
            kinesis_lag_latency_ms,

            oplog_entries_read_total,
            oplog_entries_released_total,
            oplog_entries_discarded_total,
            oplog_buffer_size_bytes,
            oplog_buffer_entries,
            oplog_high_watermark_ts,
            oplog_tail_ts,
            oplog_lag_seconds,
            oplog_rollbacks_total,
            oplog_reconnects_total,
            oplog_snapshot_window_remaining_pct,
            oplog_snapshot_docs_total,
            oplog_snapshot_chunks_total,
            oplog_snapshot_chunks_done,
            oplog_snapshot_docs_per_chunk,
        }
    }
}

impl Default for SourceMetrics {
    fn default() -> Self {
        GLOBAL_SOURCE_METRICS.clone()
    }
}
