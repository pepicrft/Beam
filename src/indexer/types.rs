use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{cmp, path::PathBuf, thread};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexConfig {
    pub roots: Vec<PathBuf>,
    pub exclude_globs: Vec<String>,
    pub ignore_hidden: bool,
    pub follow_symlinks: bool,
    pub watch: bool,
    pub max_concurrency: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            roots: Vec::new(),
            exclude_globs: vec![
                "**/.git/**".to_string(),
                "**/node_modules/**".to_string(),
                "**/target/**".to_string(),
                "**/.DS_Store".to_string(),
            ],
            ignore_hidden: true,
            follow_symlinks: false,
            watch: true,
            max_concurrency: default_concurrency(),
        }
    }
}

impl IndexConfig {
    pub fn with_roots(roots: Vec<PathBuf>) -> Self {
        Self {
            roots,
            ..Self::default()
        }
    }

    pub fn normalized(mut self) -> Self {
        self.max_concurrency = cmp::max(1, self.max_concurrency);
        self.roots.sort();
        self.roots.dedup();
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexedEntry {
    pub root: PathBuf,
    pub absolute_path: PathBuf,
    pub relative_path: PathBuf,
    pub file_name: String,
    pub extension: Option<String>,
    pub kind: EntryKind,
    pub size_bytes: u64,
    pub modified_at: Option<DateTime<Utc>>,
    pub is_hidden: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    pub entry: IndexedEntry,
    pub score: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexStats {
    pub roots: Vec<PathBuf>,
    pub indexed_files: usize,
    pub indexed_directories: usize,
    pub indexed_symlinks: usize,
    pub initial_scan_complete: bool,
    pub currently_scanning: bool,
    pub scan_generation: u64,
    pub last_scan_started_at: Option<DateTime<Utc>>,
    pub last_scan_finished_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

impl IndexStats {
    pub fn pending(roots: Vec<PathBuf>) -> Self {
        Self {
            roots,
            indexed_files: 0,
            indexed_directories: 0,
            indexed_symlinks: 0,
            initial_scan_complete: false,
            currently_scanning: false,
            scan_generation: 0,
            last_scan_started_at: None,
            last_scan_finished_at: None,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexSnapshot {
    pub stats: IndexStats,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexDump {
    pub stats: IndexStats,
    pub entries: Vec<IndexedEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexUpdateKind {
    Started,
    Rebuilt,
    Refreshed,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexUpdate {
    pub kind: IndexUpdateKind,
    pub stats: IndexStats,
}

pub fn default_index_roots() -> Vec<PathBuf> {
    dirs::home_dir().into_iter().collect()
}

fn default_concurrency() -> usize {
    thread::available_parallelism()
        .map(|parallelism| cmp::max(2, parallelism.get() / 2))
        .unwrap_or(4)
}
