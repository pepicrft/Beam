mod backend;
mod protocol;
mod scanner;
mod service;
mod storage;
mod types;

pub use protocol::{IndexRequest, IndexResponse};
pub use service::FileIndexHandle;
pub use types::{
    EntryContentType, EntryKind, IndexConfig, IndexDump, IndexSnapshot, IndexStats,
    IndexStorageConfig, IndexUpdate, IndexUpdateKind, IndexedEntry, SearchHit, WatchBackend,
    WatchState, default_index_roots,
};
