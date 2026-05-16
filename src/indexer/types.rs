use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::{cmp, path::PathBuf, thread};

const PROJECT_QUALIFIER: &str = "com";
const PROJECT_ORGANIZATION: &str = "beam";
const PROJECT_APPLICATION: &str = "Beam";
const DEFAULT_MAX_FILE_SIZE_BYTES: u64 = 128 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexStorageConfig {
    pub data_dir: PathBuf,
    pub state_dir: PathBuf,
}

impl IndexStorageConfig {
    pub fn new(data_dir: PathBuf, state_dir: PathBuf) -> Self {
        Self {
            data_dir,
            state_dir,
        }
    }

    pub fn project_default() -> Result<Self> {
        let project_dirs =
            ProjectDirs::from(PROJECT_QUALIFIER, PROJECT_ORGANIZATION, PROJECT_APPLICATION)
                .ok_or_else(|| anyhow!("failed to resolve platform storage directories"))?;
        let state_dir = project_dirs
            .state_dir()
            .map(PathBuf::from)
            .unwrap_or_else(|| project_dirs.data_local_dir().to_path_buf());

        Ok(Self {
            data_dir: project_dirs.data_local_dir().to_path_buf(),
            state_dir,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexConfig {
    pub roots: Vec<PathBuf>,
    pub exclude_globs: Vec<String>,
    pub ignore_hidden: bool,
    pub follow_symlinks: bool,
    pub watch: bool,
    pub refresh_on_start: bool,
    pub index_contents: bool,
    pub respect_ignore_files: bool,
    pub max_file_size_bytes: u64,
    pub max_concurrency: usize,
    pub storage: Option<IndexStorageConfig>,
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
                "*.tmp".to_string(),
                "*.temp".to_string(),
                "**/tmp/**".to_string(),
                "**/temp/**".to_string(),
                "**/[Cc]ache/**".to_string(),
                "**/[Cc]aches/**".to_string(),
                "**/Library/Application Support/**".to_string(),
                "**/Mobile Documents/**/PreferenceSync/**".to_string(),
                "**/Mobile Documents/**/Application Support/**".to_string(),
                "**/Library/CloudStorage/**/.Encrypted/**".to_string(),
                "**/Library/CloudStorage/**/.shortcut-targets-by-id/**".to_string(),
                "**/Library/CloudStorage/**/.file-revisions-by-id/**".to_string(),
                "**/Library/CloudStorage/**/.tmp.drive*".to_string(),
                "**/Library/CloudStorage/**/.tmp.drive*/**".to_string(),
                "**/*.backupdb/**".to_string(),
                "**/*.dSYM/**".to_string(),
                "**/*.flplugin/**".to_string(),
                "**/*.icdplugin/**".to_string(),
                "**/*.ideplugin/**".to_string(),
                "**/*.lproj/**".to_string(),
                "**/*.lrcat/**".to_string(),
                "**/*.lrcat-data/**".to_string(),
                "**/*.lrdata/**".to_string(),
                "**/*.lrlibrary/**".to_string(),
                "**/*.menu/**".to_string(),
                "**/*.playground/**".to_string(),
                "**/*.pvm/**".to_string(),
            ],
            ignore_hidden: true,
            follow_symlinks: false,
            watch: true,
            refresh_on_start: true,
            index_contents: false,
            respect_ignore_files: true,
            max_file_size_bytes: DEFAULT_MAX_FILE_SIZE_BYTES,
            max_concurrency: default_concurrency(),
            storage: None,
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

    pub fn with_storage(mut self, storage: IndexStorageConfig) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn normalized(mut self) -> Result<Self> {
        self.max_concurrency = cmp::max(1, self.max_concurrency);
        self.max_file_size_bytes = cmp::max(1, self.max_file_size_bytes);
        self.roots.sort();
        self.roots.dedup();
        if self.storage.is_none() {
            self.storage = Some(IndexStorageConfig::project_default()?);
        }
        Ok(self)
    }

    pub fn storage(&self) -> Result<&IndexStorageConfig> {
        self.storage
            .as_ref()
            .ok_or_else(|| anyhow!("index config storage has not been normalized"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryContentType {
    Application,
    Archive,
    Audio,
    Code,
    Configuration,
    Directory,
    Document,
    Image,
    Other,
    Package,
    Shortcut,
    Symlink,
    Video,
}

impl EntryContentType {
    pub fn is_launchable(self) -> bool {
        matches!(self, Self::Application | Self::Shortcut)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexedEntry {
    pub root: PathBuf,
    pub absolute_path: PathBuf,
    pub relative_path: PathBuf,
    pub file_name: String,
    pub extension: Option<String>,
    pub kind: EntryKind,
    pub content_type: EntryContentType,
    pub size_bytes: u64,
    pub modified_at: Option<DateTime<Utc>>,
    pub last_accessed_at: Option<DateTime<Utc>>,
    pub is_hidden: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    pub entry: IndexedEntry,
    pub score: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchBackend {
    Recommended,
    Poll,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WatchState {
    pub backend: WatchBackend,
    pub roots: Vec<PathBuf>,
    pub recursive: bool,
    pub last_event_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexStats {
    pub roots: Vec<PathBuf>,
    pub indexed_files: u64,
    pub indexed_directories: u64,
    pub indexed_symlinks: u64,
    pub indexed_bytes: u64,
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
            indexed_bytes: 0,
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
    pub storage: IndexStorageConfig,
    pub watch: Option<WatchState>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexDump {
    pub stats: IndexStats,
    pub storage: IndexStorageConfig,
    pub watch: Option<WatchState>,
    pub entries: Vec<IndexedEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexUpdateKind {
    Loaded,
    Started,
    Rebuilt,
    Refreshed,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexUpdate {
    pub kind: IndexUpdateKind,
    pub stats: IndexStats,
    pub watch: Option<WatchState>,
}

pub fn default_index_roots() -> Vec<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| directories::BaseDirs::new().map(|dirs| dirs.home_dir().to_path_buf()))
        .into_iter()
        .collect()
}

fn default_concurrency() -> usize {
    thread::available_parallelism()
        .map(|parallelism| cmp::max(2, parallelism.get()))
        .unwrap_or(4)
}
