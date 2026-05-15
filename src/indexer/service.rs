use crate::indexer::{
    backend::SearchBackend,
    scanner::{ScanTarget, scan_full, scan_targets},
    storage::{
        IndexPaths, PersistedOperation, PersistedQueueState, load_queue, load_stats, load_watch,
        prepare_storage, store_queue, store_stats, store_watch,
    },
    types::{
        IndexConfig, IndexDump, IndexSnapshot, IndexStats, IndexStorageConfig, IndexUpdate,
        IndexUpdateKind, IndexedEntry, SearchHit, WatchBackend, WatchState,
    },
};
use anyhow::{Result, anyhow};
use chrono::Utc;
use notify::{
    Config as NotifyConfig, Event, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::{broadcast, mpsc, oneshot},
    time::{Instant, sleep},
};

const WATCH_DEBOUNCE: Duration = Duration::from_millis(125);
const INDEX_UPDATE_BUFFER: usize = 64;

#[derive(Clone)]
pub struct FileIndexHandle {
    command_tx: mpsc::UnboundedSender<Command>,
    update_tx: broadcast::Sender<IndexUpdate>,
}

impl FileIndexHandle {
    pub fn spawn(config: IndexConfig, runtime: Handle) -> Result<Self> {
        let config = config.normalized()?;
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (update_tx, _) = broadcast::channel(INDEX_UPDATE_BUFFER);
        let actor_updates = update_tx.clone();
        runtime.spawn(async move {
            let actor = IndexActor::new(config, command_rx, actor_updates);
            actor.run().await;
        });

        Ok(Self {
            command_tx,
            update_tx,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<IndexUpdate> {
        self.update_tx.subscribe()
    }

    pub async fn snapshot(&self) -> Result<IndexSnapshot> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(Command::Snapshot { response_tx: tx })
            .map_err(|_| anyhow!("indexer command channel closed"))?;
        rx.await.map_err(|_| anyhow!("indexer actor dropped"))?
    }

    pub async fn dump(&self, limit: Option<usize>) -> Result<IndexDump> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(Command::Dump {
                response_tx: tx,
                limit,
            })
            .map_err(|_| anyhow!("indexer command channel closed"))?;
        rx.await.map_err(|_| anyhow!("indexer actor dropped"))?
    }

    pub async fn search(&self, query: impl Into<String>, limit: usize) -> Result<Vec<SearchHit>> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(Command::Search {
                query: query.into(),
                limit,
                response_tx: tx,
            })
            .map_err(|_| anyhow!("indexer command channel closed"))?;
        rx.await.map_err(|_| anyhow!("indexer actor dropped"))?
    }

    pub async fn refresh(&self) -> Result<IndexSnapshot> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(Command::Refresh { response_tx: tx })
            .map_err(|_| anyhow!("indexer command channel closed"))?;
        rx.await.map_err(|_| anyhow!("indexer actor dropped"))?
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(Command::Shutdown { response_tx: tx })
            .map_err(|_| anyhow!("indexer command channel closed"))?;
        rx.await.map_err(|_| anyhow!("indexer actor dropped"))?
    }
}

enum Command {
    Snapshot {
        response_tx: oneshot::Sender<Result<IndexSnapshot>>,
    },
    Dump {
        response_tx: oneshot::Sender<Result<IndexDump>>,
        limit: Option<usize>,
    },
    Search {
        query: String,
        limit: usize,
        response_tx: oneshot::Sender<Result<Vec<SearchHit>>>,
    },
    Refresh {
        response_tx: oneshot::Sender<Result<IndexSnapshot>>,
    },
    Shutdown {
        response_tx: oneshot::Sender<Result<()>>,
    },
}

struct IndexActor {
    config: IndexConfig,
    paths: IndexPaths,
    command_rx: mpsc::UnboundedReceiver<Command>,
    update_tx: broadcast::Sender<IndexUpdate>,
    event_rx: mpsc::UnboundedReceiver<Event>,
    completion_rx: mpsc::UnboundedReceiver<OperationMessage>,
    completion_tx: mpsc::UnboundedSender<OperationMessage>,
    watchers: Vec<ActiveWatcher>,
    backend: Option<SearchBackend>,
    state: IndexState,
    pending_operations: VecDeque<IndexOperation>,
    current_operation: Option<IndexOperation>,
    refresh_waiters: Vec<oneshot::Sender<Result<IndexSnapshot>>>,
}

impl IndexActor {
    fn new(
        config: IndexConfig,
        command_rx: mpsc::UnboundedReceiver<Command>,
        update_tx: broadcast::Sender<IndexUpdate>,
    ) -> Self {
        let storage = config
            .storage()
            .expect("normalized config must contain storage")
            .clone();
        let roots = config.roots.clone();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (completion_tx, completion_rx) = mpsc::unbounded_channel();

        Self {
            paths: IndexPaths::new(&storage),
            state: IndexState::new(roots.clone(), storage),
            config,
            command_rx,
            update_tx,
            event_rx,
            completion_rx,
            completion_tx,
            watchers: install_watchers(&roots, &event_tx),
            backend: None,
            pending_operations: VecDeque::new(),
            current_operation: None,
            refresh_waiters: Vec::new(),
        }
    }

    async fn run(mut self) {
        if let Err(error) = self.initialize().await {
            self.state.stats.last_error = Some(error.to_string());
            self.state.stats.currently_scanning = false;
            self.publish(IndexUpdateKind::Error);
            return;
        }

        let mut pending_paths = HashSet::new();
        let mut debounce = Box::pin(sleep(Duration::from_secs(60 * 60 * 24)));
        let mut debounce_active = false;

        loop {
            self.start_next_operation().await;

            tokio::select! {
                maybe_command = self.command_rx.recv() => {
                    let Some(command) = maybe_command else {
                        break;
                    };
                    if self.handle_command(command).await {
                        break;
                    }
                }
                maybe_event = self.event_rx.recv() => {
                    let Some(event) = maybe_event else {
                        continue;
                    };
                    pending_paths.extend(event.paths);
                    if let Some(watch) = &mut self.state.watch {
                        watch.last_event_at = Some(Utc::now());
                    }
                    debounce_active = true;
                    debounce.as_mut().reset(Instant::now() + WATCH_DEBOUNCE);
                }
                maybe_completion = self.completion_rx.recv() => {
                    let Some(message) = maybe_completion else {
                        continue;
                    };
                    self.handle_operation_completion(message).await;
                }
                _ = &mut debounce, if debounce_active => {
                    debounce_active = false;
                    let paths = coalesce_paths(pending_paths.drain().collect());
                    self.enqueue_partial(paths).await;
                }
            }
        }
    }

    async fn initialize(&mut self) -> Result<()> {
        prepare_storage(&self.paths).await?;
        let backend = SearchBackend::open(self.paths.clone()).await?;
        let entries = backend.stored_entries().await?;
        let _previous_queue = load_queue(&self.paths).await?;
        let mut stats = load_stats(&self.paths)
            .await?
            .unwrap_or_else(|| IndexStats::pending(self.config.roots.clone()));
        stats.roots = self.config.roots.clone();
        self.state.stats = stats;
        self.state.entries = entries;
        self.state.rebuild_stats();
        self.state.watch = load_watch(&self.paths).await?;
        self.backend = Some(backend);

        if self.state.watch.is_none() {
            self.state.watch = Some(WatchState {
                backend: detect_watch_backend(&self.watchers),
                roots: self.config.roots.clone(),
                recursive: true,
                last_event_at: None,
            });
        }

        store_stats(&self.paths, &self.state.stats).await?;
        if let Some(watch) = &self.state.watch {
            store_watch(&self.paths, watch).await?;
        }
        store_queue(&self.paths, &PersistedQueueState::empty(&self.paths)).await?;

        self.publish(IndexUpdateKind::Loaded);
        self.enqueue_full(false).await;
        Ok(())
    }

    async fn handle_command(&mut self, command: Command) -> bool {
        match command {
            Command::Snapshot { response_tx } => {
                let _ = response_tx.send(Ok(self.snapshot()));
            }
            Command::Dump { response_tx, limit } => {
                let _ = response_tx.send(Ok(self.dump(limit)));
            }
            Command::Search {
                query,
                limit,
                response_tx,
            } => {
                let result = match &self.backend {
                    Some(backend) => backend.search(&query, limit).await,
                    None => Err(anyhow!("index backend is not available")),
                };
                let _ = response_tx.send(result);
            }
            Command::Refresh { response_tx } => {
                self.refresh_waiters.push(response_tx);
                self.enqueue_full(true).await;
            }
            Command::Shutdown { response_tx } => {
                let _ = response_tx.send(Ok(()));
                return true;
            }
        }

        false
    }

    async fn handle_operation_completion(&mut self, message: OperationMessage) {
        self.current_operation = None;

        match message.result {
            Ok(OperationCompletion::Full { entries }) => {
                self.state.entries = entries;
                self.finish_success(IndexUpdateKind::Rebuilt).await;
                self.persist_queue().await;
                self.resolve_refresh_waiters(Ok(self.snapshot()));
            }
            Ok(OperationCompletion::Partial {
                additions,
                deletions,
            }) => {
                for deletion in deletions {
                    self.state.entries.remove(&deletion);
                }
                self.state.entries.extend(additions);
                self.finish_success(IndexUpdateKind::Refreshed).await;
                self.persist_queue().await;
            }
            Err(error) => {
                self.state.stats.currently_scanning = false;
                self.state.stats.last_error = Some(error.to_string());
                self.state.stats.last_scan_finished_at = Some(Utc::now());
                let _ = store_stats(&self.paths, &self.state.stats).await;
                self.publish(IndexUpdateKind::Error);
                self.persist_queue().await;
                if matches!(message.operation, IndexOperation::Full) {
                    self.resolve_refresh_waiters(Err(error));
                }
            }
        }
    }

    async fn finish_success(&mut self, kind: IndexUpdateKind) {
        self.state.stats.currently_scanning = false;
        self.state.stats.initial_scan_complete = true;
        self.state.stats.scan_generation += 1;
        self.state.stats.last_scan_finished_at = Some(Utc::now());
        self.state.stats.last_error = None;
        self.state.rebuild_stats();
        let _ = store_stats(&self.paths, &self.state.stats).await;
        if let Some(watch) = &self.state.watch {
            let _ = store_watch(&self.paths, watch).await;
        }
        self.publish(kind);
    }

    async fn start_next_operation(&mut self) {
        if self.current_operation.is_some() {
            return;
        }
        let Some(operation) = self.pending_operations.pop_front() else {
            return;
        };
        let Some(backend) = self.backend.clone() else {
            return;
        };

        let completion_tx = self.completion_tx.clone();
        let config = self.config.clone();
        let state_entries = self.state.entries.clone();
        self.current_operation = Some(operation.clone());
        self.state.stats.currently_scanning = true;
        self.state.stats.last_scan_started_at = Some(Utc::now());
        self.publish(IndexUpdateKind::Started);
        self.persist_queue().await;

        tokio::spawn(async move {
            let result = match &operation {
                IndexOperation::Full => run_full_operation(config, backend).await,
                IndexOperation::Partial(paths) => {
                    run_partial_operation(config, backend, state_entries, paths.clone()).await
                }
            };
            let _ = completion_tx.send(OperationMessage { operation, result });
        });
    }

    async fn enqueue_full(&mut self, explicit_refresh: bool) {
        if self.current_operation == Some(IndexOperation::Full)
            || self.pending_operations.contains(&IndexOperation::Full)
        {
            if explicit_refresh {
                self.persist_queue().await;
            }
            return;
        }
        self.pending_operations.clear();
        self.pending_operations.push_back(IndexOperation::Full);
        self.persist_queue().await;
    }

    async fn enqueue_partial(&mut self, paths: Vec<PathBuf>) {
        if paths.is_empty()
            || self.current_operation == Some(IndexOperation::Full)
            || self.pending_operations.contains(&IndexOperation::Full)
        {
            return;
        }

        if let Some(IndexOperation::Partial(existing)) = self
            .pending_operations
            .iter_mut()
            .find(|operation| matches!(operation, IndexOperation::Partial(_)))
        {
            existing.extend(paths);
            *existing = coalesce_paths(existing.clone());
        } else {
            self.pending_operations
                .push_back(IndexOperation::Partial(coalesce_paths(paths)));
        }

        self.persist_queue().await;
    }

    async fn persist_queue(&self) {
        let current = self
            .current_operation
            .as_ref()
            .map(PersistedOperation::from);
        let pending = self
            .pending_operations
            .iter()
            .cloned()
            .map(PersistedOperation::from)
            .collect();
        let queue = PersistedQueueState {
            current,
            pending,
            ..PersistedQueueState::empty(&self.paths)
        };
        let _ = store_queue(&self.paths, &queue).await;
    }

    fn snapshot(&self) -> IndexSnapshot {
        IndexSnapshot {
            stats: self.state.stats.clone(),
            storage: self.state.storage.clone(),
            watch: self.state.watch.clone(),
        }
    }

    fn dump(&self, limit: Option<usize>) -> IndexDump {
        let mut entries = self.sorted_entries();
        if let Some(limit) = limit {
            entries.truncate(limit);
        }

        IndexDump {
            stats: self.state.stats.clone(),
            storage: self.state.storage.clone(),
            watch: self.state.watch.clone(),
            entries,
        }
    }

    fn sorted_entries(&self) -> Vec<IndexedEntry> {
        let mut entries = self.state.entries.values().cloned().collect::<Vec<_>>();
        entries.sort_by(|left, right| left.absolute_path.cmp(&right.absolute_path));
        entries
    }

    fn resolve_refresh_waiters(&mut self, result: Result<IndexSnapshot>) {
        for waiter in self.refresh_waiters.drain(..) {
            let payload = result
                .as_ref()
                .map(Clone::clone)
                .map_err(|error| anyhow!(error.to_string()));
            let _ = waiter.send(payload);
        }
    }

    fn publish(&self, kind: IndexUpdateKind) {
        let _ = self.update_tx.send(IndexUpdate {
            kind,
            stats: self.state.stats.clone(),
            watch: self.state.watch.clone(),
        });
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum IndexOperation {
    Full,
    Partial(Vec<PathBuf>),
}

impl From<IndexOperation> for PersistedOperation {
    fn from(value: IndexOperation) -> Self {
        match value {
            IndexOperation::Full => PersistedOperation::Full,
            IndexOperation::Partial(paths) => PersistedOperation::Partial { paths },
        }
    }
}

impl From<&IndexOperation> for PersistedOperation {
    fn from(value: &IndexOperation) -> Self {
        match value {
            IndexOperation::Full => PersistedOperation::Full,
            IndexOperation::Partial(paths) => PersistedOperation::Partial {
                paths: paths.clone(),
            },
        }
    }
}

struct OperationMessage {
    operation: IndexOperation,
    result: Result<OperationCompletion>,
}

enum OperationCompletion {
    Full {
        entries: HashMap<PathBuf, IndexedEntry>,
    },
    Partial {
        additions: HashMap<PathBuf, IndexedEntry>,
        deletions: Vec<PathBuf>,
    },
}

struct IndexState {
    entries: HashMap<PathBuf, IndexedEntry>,
    stats: IndexStats,
    storage: IndexStorageConfig,
    watch: Option<WatchState>,
}

impl IndexState {
    fn new(roots: Vec<PathBuf>, storage: IndexStorageConfig) -> Self {
        Self {
            entries: HashMap::new(),
            stats: IndexStats::pending(roots),
            storage,
            watch: None,
        }
    }

    fn rebuild_stats(&mut self) {
        self.stats.indexed_files = self
            .entries
            .values()
            .filter(|entry| entry.kind == crate::indexer::types::EntryKind::File)
            .count() as u64;
        self.stats.indexed_directories = self
            .entries
            .values()
            .filter(|entry| entry.kind == crate::indexer::types::EntryKind::Directory)
            .count() as u64;
        self.stats.indexed_symlinks = self
            .entries
            .values()
            .filter(|entry| entry.kind == crate::indexer::types::EntryKind::Symlink)
            .count() as u64;
        self.stats.indexed_bytes = self.entries.values().map(|entry| entry.size_bytes).sum();
    }
}

#[allow(dead_code)]
enum ActiveWatcher {
    Recommended(RecommendedWatcher),
    Poll(PollWatcher),
}

fn install_watchers(
    roots: &[PathBuf],
    event_tx: &mpsc::UnboundedSender<Event>,
) -> Vec<ActiveWatcher> {
    let make_callback = |tx: mpsc::UnboundedSender<Event>| {
        move |result: notify::Result<Event>| {
            if let Ok(event) = result {
                let _ = tx.send(event);
            }
        }
    };

    let mut watchers = Vec::new();

    if let Ok(mut watcher) = notify::recommended_watcher(make_callback(event_tx.clone()))
        && roots
            .iter()
            .all(|root| watcher.watch(root, RecursiveMode::Recursive).is_ok())
    {
        watchers.push(ActiveWatcher::Recommended(watcher));
    }

    let poll_result = PollWatcher::new(
        make_callback(event_tx.clone()),
        NotifyConfig::default()
            .with_poll_interval(Duration::from_millis(250))
            .with_compare_contents(true),
    );
    let Ok(mut poll_watcher) = poll_result else {
        return watchers;
    };

    let watched_roots = roots
        .iter()
        .all(|root| poll_watcher.watch(root, RecursiveMode::Recursive).is_ok());
    if watched_roots {
        watchers.push(ActiveWatcher::Poll(poll_watcher));
    }

    watchers
}

fn detect_watch_backend(watchers: &[ActiveWatcher]) -> WatchBackend {
    if watchers
        .iter()
        .any(|watcher| matches!(watcher, ActiveWatcher::Recommended(_)))
    {
        WatchBackend::Recommended
    } else {
        WatchBackend::Poll
    }
}

async fn run_full_operation(
    config: IndexConfig,
    backend: SearchBackend,
) -> Result<OperationCompletion> {
    let outcome = scan_full(&config).await?;
    let entries = outcome
        .documents
        .iter()
        .map(|(path, document)| (path.clone(), document.entry.clone()))
        .collect::<HashMap<_, _>>();
    backend.replace_all(outcome.documents).await?;
    Ok(OperationCompletion::Full { entries })
}

async fn run_partial_operation(
    config: IndexConfig,
    backend: SearchBackend,
    current_entries: HashMap<PathBuf, IndexedEntry>,
    paths: Vec<PathBuf>,
) -> Result<OperationCompletion> {
    let targets = resolve_scan_targets(&config.roots, &paths);
    let outcome = scan_targets(&config, targets).await?;
    let additions = outcome
        .documents
        .iter()
        .map(|(path, document)| (path.clone(), document.entry.clone()))
        .collect::<HashMap<_, _>>();
    let mut deletions = collect_deletions(&current_entries, &paths);
    deletions.retain(|path| !additions.contains_key(path));
    backend
        .apply_delta(outcome.documents, deletions.clone())
        .await?;

    Ok(OperationCompletion::Partial {
        additions,
        deletions,
    })
}

fn resolve_scan_targets(roots: &[PathBuf], paths: &[PathBuf]) -> Vec<ScanTarget> {
    paths
        .iter()
        .filter_map(|path| {
            owning_root(roots, path).map(|root| ScanTarget {
                root: root.to_path_buf(),
                path: path.clone(),
                explicit_root: path == root,
            })
        })
        .collect()
}

fn collect_deletions(
    current_entries: &HashMap<PathBuf, IndexedEntry>,
    paths: &[PathBuf],
) -> Vec<PathBuf> {
    let mut deletions = Vec::new();
    for path in paths {
        deletions.extend(
            current_entries
                .keys()
                .filter(|candidate| is_same_or_descendant(candidate, path))
                .cloned(),
        );
    }
    deletions.sort();
    deletions.dedup();
    deletions
}

fn owning_root<'a>(roots: &'a [PathBuf], path: &Path) -> Option<&'a Path> {
    roots
        .iter()
        .filter(|root| path.starts_with(root))
        .max_by_key(|root| root.components().count())
        .map(PathBuf::as_path)
}

fn coalesce_paths(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.sort();
    paths.dedup();
    let mut collapsed: Vec<PathBuf> = Vec::new();
    for path in paths {
        if collapsed
            .iter()
            .any(|candidate| is_same_or_descendant(&path, candidate))
        {
            continue;
        }
        collapsed.push(path);
    }
    collapsed
}

fn is_same_or_descendant(path: &Path, prefix: &Path) -> bool {
    path == prefix || path.starts_with(prefix)
}

#[cfg(test)]
mod tests {
    use super::FileIndexHandle;
    use crate::indexer::{
        IndexConfig, IndexStorageConfig,
        storage::{IndexPaths, load_queue, load_stats, prepare_storage},
    };
    use anyhow::{Result, anyhow};
    use std::{path::Path, time::Duration};
    use tempfile::tempdir;
    use tokio::{fs, runtime::Handle, time::sleep};

    fn test_config(root: &Path, storage_root: &Path) -> IndexConfig {
        IndexConfig::with_roots(vec![root.to_path_buf()]).with_storage(IndexStorageConfig::new(
            storage_root.join("data"),
            storage_root.join("state"),
        ))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_builds_index_and_persists_stats() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("workspace");
        fs::create_dir(&root).await?;
        fs::write(root.join("today.md"), "beam").await?;
        let storage_root = temp.path().join("storage");

        let handle = FileIndexHandle::spawn(test_config(&root, &storage_root), Handle::current())?;
        let snapshot = handle.refresh().await?;

        assert!(snapshot.stats.initial_scan_complete);
        assert_eq!(snapshot.stats.indexed_files, 1);

        let stats = load_stats(&IndexPaths::new(&snapshot.storage)).await?;
        assert_eq!(stats.expect("stats should exist").indexed_files, 1);
        handle.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_updates_changed_files() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("workspace");
        fs::create_dir(&root).await?;
        let storage_root = temp.path().join("storage");

        let handle = FileIndexHandle::spawn(test_config(&root, &storage_root), Handle::current())?;
        handle.refresh().await?;

        let fresh_path = root.join("fresh.txt");
        fs::write(&fresh_path, "fresh").await?;
        handle.refresh().await?;
        wait_for_search_hit(&handle, "fresh", &fresh_path).await?;

        fs::remove_file(&fresh_path).await?;
        handle.refresh().await?;
        wait_for_search_miss(&handle, "fresh").await?;

        handle.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watcher_keeps_index_current() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("workspace");
        fs::create_dir(&root).await?;
        let storage_root = temp.path().join("storage");

        let handle = FileIndexHandle::spawn(test_config(&root, &storage_root), Handle::current())?;
        handle.refresh().await?;

        let fresh_path = root.join("watched.txt");
        fs::write(&fresh_path, "watch me").await?;
        wait_for_search_hit(&handle, "watched", &fresh_path).await?;

        fs::remove_file(&fresh_path).await?;
        wait_for_search_miss(&handle, "watched").await?;

        handle.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persisted_index_is_available_after_restart() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("workspace");
        fs::create_dir(&root).await?;
        fs::write(root.join("persisted.md"), "beam").await?;
        let storage_root = temp.path().join("storage");

        let first = FileIndexHandle::spawn(test_config(&root, &storage_root), Handle::current())?;
        first.refresh().await?;
        first.shutdown().await?;

        let second = FileIndexHandle::spawn(test_config(&root, &storage_root), Handle::current())?;
        wait_for_search_hit(&second, "persisted", &root.join("persisted.md")).await?;
        second.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn content_indexing_can_search_file_body() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("workspace");
        fs::create_dir(&root).await?;
        fs::write(root.join("notes.md"), "launcher beam search").await?;
        let storage_root = temp.path().join("storage");

        let mut config = test_config(&root, &storage_root);
        config.index_contents = true;
        let handle = FileIndexHandle::spawn(config, Handle::current())?;
        handle.refresh().await?;

        let hits = handle.search("launcher", 10).await?;
        assert_eq!(
            hits.first().map(|hit| hit.entry.file_name.as_str()),
            Some("notes.md")
        );
        handle.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queue_file_is_written() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("workspace");
        fs::create_dir(&root).await?;
        let storage = IndexStorageConfig::new(
            temp.path().join("storage/data"),
            temp.path().join("storage/state"),
        );
        let paths = IndexPaths::new(&storage);
        prepare_storage(&paths).await?;

        let handle = FileIndexHandle::spawn(
            IndexConfig::with_roots(vec![root]).with_storage(storage),
            Handle::current(),
        )?;
        handle.refresh().await?;

        let queue = load_queue(&paths).await?;
        assert!(queue.is_some());
        handle.shutdown().await?;
        Ok(())
    }

    async fn wait_for_search_hit(
        handle: &FileIndexHandle,
        query: &str,
        expected_path: &Path,
    ) -> Result<()> {
        wait_for(|| async {
            let hits = handle.search(query, 10).await?;
            hits.iter()
                .any(|hit| hit.entry.absolute_path == expected_path)
                .then_some(())
                .ok_or_else(|| anyhow!("expected path not indexed yet"))
        })
        .await
    }

    async fn wait_for_search_miss(handle: &FileIndexHandle, query: &str) -> Result<()> {
        wait_for(|| async {
            let hits = handle.search(query, 10).await?;
            hits.is_empty()
                .then_some(())
                .ok_or_else(|| anyhow!("expected index entry to disappear"))
        })
        .await
    }

    async fn wait_for<F, Fut>(mut check: F) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        for _ in 0..50 {
            if check().await.is_ok() {
                return Ok(());
            }
            sleep(Duration::from_millis(100)).await;
        }
        check().await
    }
}
