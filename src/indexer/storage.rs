use crate::indexer::types::{IndexStats, IndexStorageConfig, WatchState};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use tokio::fs;

pub(crate) const INDEX_VERSION: &str = "beam-tantivy-v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexPaths {
    pub(crate) data_root: PathBuf,
    pub(crate) state_root: PathBuf,
    pub(crate) db_dir: PathBuf,
    pub(crate) version_file: PathBuf,
    pub(crate) queue_file: PathBuf,
    pub(crate) stats_file: PathBuf,
    pub(crate) watch_file: PathBuf,
}

impl IndexPaths {
    pub(crate) fn new(storage: &IndexStorageConfig) -> Self {
        let data_root = storage.data_dir.join("index");
        let state_root = storage.state_dir.join("index");

        Self {
            db_dir: data_root.join("db"),
            version_file: data_root.join("version"),
            queue_file: state_root.join("queue.json"),
            stats_file: state_root.join("stats.json"),
            watch_file: state_root.join("watch.json"),
            data_root,
            state_root,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PersistedQueueState {
    pub(crate) config: PersistedQueueConfig,
    pub(crate) pending: Vec<PersistedOperation>,
    pub(crate) current: Option<PersistedOperation>,
}

impl PersistedQueueState {
    pub(crate) fn empty(paths: &IndexPaths) -> Self {
        Self {
            config: PersistedQueueConfig::new(paths),
            pending: Vec::new(),
            current: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PersistedQueueConfig {
    pub(crate) storage: PersistedStorageConfig,
    pub(crate) statistics_path: PathBuf,
    pub(crate) watch_path: PathBuf,
}

impl PersistedQueueConfig {
    fn new(paths: &IndexPaths) -> Self {
        Self {
            storage: PersistedStorageConfig {
                on_disk: PersistedOnDiskStorage {
                    path: paths.data_root.clone(),
                },
            },
            statistics_path: paths.stats_file.clone(),
            watch_path: paths.watch_file.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PersistedStorageConfig {
    #[serde(rename = "OnDisk")]
    pub(crate) on_disk: PersistedOnDiskStorage,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PersistedOnDiskStorage {
    pub(crate) path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum PersistedOperation {
    Full,
    Partial { paths: Vec<PathBuf> },
}

pub(crate) async fn prepare_storage(paths: &IndexPaths) -> Result<()> {
    fs::create_dir_all(&paths.data_root)
        .await
        .with_context(|| format!("failed to create {}", paths.data_root.display()))?;
    fs::create_dir_all(&paths.state_root)
        .await
        .with_context(|| format!("failed to create {}", paths.state_root.display()))?;

    let existing_version = read_string_if_exists(&paths.version_file).await?;
    match existing_version.as_deref().map(str::trim) {
        Some(version) if version == INDEX_VERSION => {
            fs::create_dir_all(&paths.db_dir)
                .await
                .with_context(|| format!("failed to create {}", paths.db_dir.display()))?;
        }
        _ => {
            if fs::try_exists(&paths.db_dir).await.unwrap_or(false) {
                let _ = fs::remove_dir_all(&paths.db_dir).await;
            }
            fs::create_dir_all(&paths.db_dir)
                .await
                .with_context(|| format!("failed to recreate {}", paths.db_dir.display()))?;
            fs::write(&paths.version_file, INDEX_VERSION)
                .await
                .with_context(|| format!("failed to write {}", paths.version_file.display()))?;
            let _ = fs::remove_file(&paths.queue_file).await;
            let _ = fs::remove_file(&paths.stats_file).await;
            let _ = fs::remove_file(&paths.watch_file).await;
        }
    }

    Ok(())
}

pub(crate) async fn load_queue(paths: &IndexPaths) -> Result<Option<PersistedQueueState>> {
    load_json(&paths.queue_file).await
}

pub(crate) async fn store_queue(paths: &IndexPaths, queue: &PersistedQueueState) -> Result<()> {
    store_json(&paths.queue_file, queue).await
}

pub(crate) async fn load_stats(paths: &IndexPaths) -> Result<Option<IndexStats>> {
    load_json(&paths.stats_file).await
}

pub(crate) async fn store_stats(paths: &IndexPaths, stats: &IndexStats) -> Result<()> {
    store_json(&paths.stats_file, stats).await
}

pub(crate) async fn load_watch(paths: &IndexPaths) -> Result<Option<WatchState>> {
    load_json(&paths.watch_file).await
}

pub(crate) async fn store_watch(paths: &IndexPaths, watch: &WatchState) -> Result<()> {
    store_json(&paths.watch_file, watch).await
}

async fn load_json<T>(path: &Path) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    match fs::read(path).await {
        Ok(contents) => serde_json::from_slice(&contents)
            .with_context(|| format!("failed to decode {}", path.display()))
            .map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn store_json<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let payload = serde_json::to_vec(value)?;
    fs::write(path, payload)
        .await
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

async fn read_string_if_exists(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path).await {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        INDEX_VERSION, IndexPaths, PersistedOperation, PersistedQueueState, load_queue,
        prepare_storage, store_queue,
    };
    use crate::indexer::IndexStorageConfig;
    use anyhow::Result;
    use tempfile::tempdir;
    use tokio::fs;

    #[tokio::test]
    async fn prepare_storage_creates_xdg_style_layout() -> Result<()> {
        let temp = tempdir()?;
        let storage = IndexStorageConfig::new(temp.path().join("data"), temp.path().join("state"));
        let paths = IndexPaths::new(&storage);

        prepare_storage(&paths).await?;

        assert!(fs::try_exists(&paths.db_dir).await?);
        assert_eq!(
            fs::read_to_string(&paths.version_file).await?,
            INDEX_VERSION.to_string()
        );
        Ok(())
    }

    #[tokio::test]
    async fn queue_state_round_trips() -> Result<()> {
        let temp = tempdir()?;
        let storage = IndexStorageConfig::new(temp.path().join("data"), temp.path().join("state"));
        let paths = IndexPaths::new(&storage);
        prepare_storage(&paths).await?;

        let queue = PersistedQueueState {
            pending: vec![PersistedOperation::Partial {
                paths: vec![temp.path().join("docs")],
            }],
            current: Some(PersistedOperation::Full),
            ..PersistedQueueState::empty(&paths)
        };

        store_queue(&paths, &queue).await?;
        assert_eq!(load_queue(&paths).await?, Some(queue));
        Ok(())
    }
}
