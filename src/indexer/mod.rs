mod protocol;
mod scanner;
mod service;
mod types;

pub use protocol::{IndexRequest, IndexResponse};
pub use service::FileIndexHandle;
pub use types::{
    EntryKind, IndexConfig, IndexDump, IndexSnapshot, IndexStats, IndexUpdate, IndexUpdateKind,
    IndexedEntry, SearchHit, default_index_roots,
};
