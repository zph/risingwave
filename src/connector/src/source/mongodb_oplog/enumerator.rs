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

use std::collections::HashSet;

use async_trait::async_trait;
use mongodb::bson::doc;

use super::MongodbOplogProperties;
use super::split::MongodbOplogSplit;
use crate::error::ConnectorResult;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

/// Enumerator for the mongo-oplog source.
///
/// Supports two modes:
/// - **Single RS**: Returns a single split (split_id = "0") per source since each
///   source maps to one replica set's oplog (OPLOG-038).
/// - **Sharded (mongos)**: Auto-discovers shards via `listShards` and returns one
///   split per shard (OPLOG-040).
#[derive(Debug)]
pub struct MongodbOplogSplitEnumerator {
    client: mongodb::Client,
    #[expect(dead_code)]
    properties: MongodbOplogProperties,
    /// True if connected to mongos (auto-discovery mode).
    is_sharded: bool,
    /// Previously discovered shard IDs for change detection logging.
    known_shards: HashSet<String>,
}

#[async_trait]
impl SplitEnumerator for MongodbOplogSplitEnumerator {
    type Properties = MongodbOplogProperties;
    type Split = MongodbOplogSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        let (client, _rs_name) = properties.build_client().await?;

        let is_sharded = Self::detect_mongos(&client).await;

        if !is_sharded {
            // Validate: can we access the target database and collection?
            let (db_name, coll_name) = properties.parse_namespace()?;
            let db = client.database(db_name);
            let names = db.list_collection_names(None).await?;
            if !names.contains(&coll_name.to_owned()) {
                tracing::warn!(
                    "Collection {}.{} does not exist yet; it will be created on first write",
                    db_name,
                    coll_name,
                );
            }
        }

        Ok(Self {
            client,
            properties,
            is_sharded,
            known_shards: HashSet::new(),
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<MongodbOplogSplit>> {
        if self.is_sharded {
            self.discover_shards().await
        } else {
            // Single split per source (one oplog per replica set)
            Ok(vec![MongodbOplogSplit::new()])
        }
    }
}

impl MongodbOplogSplitEnumerator {
    /// Detect whether the client is connected to a mongos (sharded cluster).
    /// Uses the `ismaster` command which returns `msg: "isdbgrid"` on mongos.
    async fn detect_mongos(client: &mongodb::Client) -> bool {
        let admin = client.database("admin");
        match admin.run_command(doc! { "ismaster": 1 }, None).await {
            Ok(reply) => reply.get_str("msg").ok() == Some("isdbgrid"),
            Err(_) => false,
        }
    }

    /// Discover shards via `listShards` command on mongos (OPLOG-040).
    async fn discover_shards(&mut self) -> ConnectorResult<Vec<MongodbOplogSplit>> {
        let admin = self.client.database("admin");
        let result = admin.run_command(doc! { "listShards": 1 }, None).await?;
        let shards = result
            .get_array("shards")
            .map_err(|e| anyhow::anyhow!("listShards response missing 'shards': {}", e))?;

        let mut splits = Vec::new();
        let mut current_ids = HashSet::new();

        for shard_bson in shards {
            let shard = shard_bson
                .as_document()
                .ok_or_else(|| anyhow::anyhow!("shard entry is not a document"))?;
            let id = shard
                .get_str("_id")
                .map_err(|e| anyhow::anyhow!("shard missing '_id': {}", e))?;
            let host = shard
                .get_str("host")
                .map_err(|e| anyhow::anyhow!("shard missing 'host': {}", e))?;
            let draining = shard.get_bool("draining").unwrap_or(false);

            current_ids.insert(id.to_owned());

            // OPLOG-041: Parse host field to replica set URI
            let uri = super::parse_shard_host_to_uri(host)?;

            // OPLOG-043: Continue returning draining shards to drain remaining events
            splits.push(MongodbOplogSplit {
                split_id: id.into(),
                replica_set_uri: Some(uri),
                start_offset: None, // framework preserves existing offset for known splits
            });

            if !self.known_shards.contains(id) {
                tracing::info!(
                    shard_id = id,
                    draining,
                    "OPLOG-042: New shard discovered"
                );
            }
        }

        // OPLOG-044: Log removed shards
        for old_id in self.known_shards.difference(&current_ids) {
            tracing::info!(shard_id = %old_id, "OPLOG-044: Shard removed from cluster");
        }

        self.known_shards = current_ids;
        Ok(splits)
    }
}
