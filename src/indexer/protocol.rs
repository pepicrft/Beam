use crate::indexer::types::{IndexDump, IndexSnapshot, SearchHit};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IndexRequest {
    Snapshot,
    Dump { limit: Option<usize> },
    Search { query: String, limit: usize },
    Refresh,
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IndexResponse {
    Ready,
    Snapshot { snapshot: IndexSnapshot },
    Dump { dump: IndexDump },
    SearchResults { hits: Vec<SearchHit> },
    Ack,
    Error { message: String },
}
