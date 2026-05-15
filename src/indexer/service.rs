use crate::indexer::{
    scanner::{ScanOutcome, scan_full, scan_subtree},
    types::{
        EntryKind, IndexConfig, IndexDump, IndexSnapshot, IndexStats, IndexUpdate, IndexUpdateKind,
        IndexedEntry, SearchHit,
    },
};
use anyhow::{Result, anyhow};
use chrono::Utc;
use notify::{Config as NotifyConfig, Event, PollWatcher, RecursiveMode, Watcher};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::{broadcast, mpsc, oneshot},
    time::{Instant, sleep},
};

const WATCH_DEBOUNCE: Duration = Duration::from_millis(150);
const INDEX_UPDATE_BUFFER: usize = 64;

#[derive(Clone)]
pub struct FileIndexHandle {
    command_tx: mpsc::UnboundedSender<Command>,
    update_tx: broadcast::Sender<IndexUpdate>,
}

impl FileIndexHandle {
    pub fn spawn(config: IndexConfig, runtime: Handle) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (update_tx, _) = broadcast::channel(INDEX_UPDATE_BUFFER);
        let actor = IndexActor::new(config.normalized(), command_rx, update_tx.clone());
        runtime.spawn(async move {
            actor.run().await;
        });

        Self {
            command_tx,
            update_tx,
        }
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
    command_rx: mpsc::UnboundedReceiver<Command>,
    update_tx: broadcast::Sender<IndexUpdate>,
    event_rx: mpsc::UnboundedReceiver<Event>,
    _watchers: Vec<PollWatcher>,
    state: IndexState,
}

impl IndexActor {
    fn new(
        config: IndexConfig,
        command_rx: mpsc::UnboundedReceiver<Command>,
        update_tx: broadcast::Sender<IndexUpdate>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let watchers = if config.watch {
            install_watchers(&config.roots, event_tx)
        } else {
            Vec::new()
        };

        Self {
            state: IndexState::new(config.roots.clone()),
            config,
            command_rx,
            update_tx,
            event_rx,
            _watchers: watchers,
        }
    }

    async fn run(mut self) {
        self.publish(IndexUpdateKind::Started);
        if let Err(error) = self.run_full_scan().await {
            self.state.stats.currently_scanning = false;
            self.state.stats.last_error = Some(error.to_string());
            self.publish(IndexUpdateKind::Error);
        }

        let mut pending_paths = HashSet::new();
        let mut debounce = Box::pin(sleep(Duration::from_secs(60 * 60 * 24)));
        let mut debounce_active = false;

        loop {
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
                    debounce_active = true;
                    debounce.as_mut().reset(Instant::now() + WATCH_DEBOUNCE);
                }
                _ = &mut debounce, if debounce_active => {
                    let refresh_targets = coalesce_paths(pending_paths.drain().collect());
                    if let Err(error) = self.refresh_paths(refresh_targets).await {
                        self.state.stats.last_error = Some(error.to_string());
                        self.publish(IndexUpdateKind::Error);
                    }
                    debounce_active = false;
                }
            }
        }
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
                let hits = self.search(&query, limit);
                let _ = response_tx.send(Ok(hits));
            }
            Command::Refresh { response_tx } => {
                let result = self.run_full_scan().await.map(|_| self.snapshot());
                let _ = response_tx.send(result);
            }
            Command::Shutdown { response_tx } => {
                let _ = response_tx.send(Ok(()));
                return true;
            }
        }

        false
    }

    async fn run_full_scan(&mut self) -> Result<()> {
        self.state.stats.currently_scanning = true;
        self.state.stats.last_scan_started_at = Some(Utc::now());
        self.publish(IndexUpdateKind::Started);

        let outcome = scan_full(&self.config).await?;
        self.state.replace_entries(outcome);
        self.state.stats.currently_scanning = false;
        self.state.stats.initial_scan_complete = true;
        self.state.stats.scan_generation += 1;
        self.state.stats.last_scan_finished_at = Some(Utc::now());
        self.state.stats.last_error = None;
        self.publish(IndexUpdateKind::Rebuilt);
        Ok(())
    }

    async fn refresh_paths(&mut self, paths: Vec<PathBuf>) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }

        self.state.stats.currently_scanning = true;
        self.state.stats.last_scan_started_at = Some(Utc::now());
        self.publish(IndexUpdateKind::Started);

        for path in paths {
            self.refresh_path(&path).await?;
        }

        self.state.stats.currently_scanning = false;
        self.state.stats.initial_scan_complete = true;
        self.state.stats.scan_generation += 1;
        self.state.stats.last_scan_finished_at = Some(Utc::now());
        self.state.stats.last_error = None;
        self.publish(IndexUpdateKind::Refreshed);
        Ok(())
    }

    async fn refresh_path(&mut self, path: &Path) -> Result<()> {
        let Some(root) = owning_root(&self.config.roots, path) else {
            return Ok(());
        };
        let metadata = match tokio::fs::symlink_metadata(path).await {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };

        match metadata {
            Some(metadata) if metadata.is_dir() => {
                let outcome = scan_subtree(&self.config, root, path).await?;
                self.state.replace_subtree(path, outcome);
            }
            Some(metadata) => {
                let outcome = scan_subtree(&self.config, root, path).await?;
                if outcome.entries.is_empty() {
                    self.state.remove_path(path);
                } else {
                    self.state
                        .replace_path(path, outcome.entries.into_values().next().unwrap());
                }

                if metadata.file_type().is_symlink() && !self.config.follow_symlinks {
                    self.state.remove_children_of(path);
                }
            }
            None => {
                self.state.remove_path(path);
            }
        }

        Ok(())
    }

    fn snapshot(&self) -> IndexSnapshot {
        IndexSnapshot {
            stats: self.state.stats.clone(),
        }
    }

    fn dump(&self, limit: Option<usize>) -> IndexDump {
        let mut entries = self.sorted_entries();
        if let Some(limit) = limit {
            entries.truncate(limit);
        }

        IndexDump {
            stats: self.state.stats.clone(),
            entries,
        }
    }

    fn search(&self, query: &str, limit: usize) -> Vec<SearchHit> {
        let query = query.trim();
        if query.is_empty() {
            return self
                .sorted_entries()
                .into_iter()
                .take(limit)
                .map(|entry| SearchHit { entry, score: 0 })
                .collect();
        }

        let mut hits = self
            .state
            .entries
            .values()
            .filter_map(|entry| {
                score_entry(query, entry).map(|score| SearchHit {
                    entry: entry.clone(),
                    score,
                })
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.entry.absolute_path.cmp(&right.entry.absolute_path))
        });
        hits.truncate(limit);
        hits
    }

    fn sorted_entries(&self) -> Vec<IndexedEntry> {
        let mut entries = self.state.entries.values().cloned().collect::<Vec<_>>();
        entries.sort_by(|left, right| left.absolute_path.cmp(&right.absolute_path));
        entries
    }

    fn publish(&self, kind: IndexUpdateKind) {
        let _ = self.update_tx.send(IndexUpdate {
            kind,
            stats: self.state.stats.clone(),
        });
    }
}

struct IndexState {
    entries: HashMap<PathBuf, IndexedEntry>,
    stats: IndexStats,
}

impl IndexState {
    fn new(roots: Vec<PathBuf>) -> Self {
        Self {
            entries: HashMap::new(),
            stats: IndexStats::pending(roots),
        }
    }

    fn replace_entries(&mut self, outcome: ScanOutcome) {
        self.entries = outcome.entries;
        self.rebuild_stats();
    }

    fn replace_subtree(&mut self, subtree: &Path, outcome: ScanOutcome) {
        self.entries
            .retain(|path, _| !is_same_or_descendant(path, subtree));
        self.entries.extend(outcome.entries);
        self.rebuild_stats();
    }

    fn replace_path(&mut self, path: &Path, entry: IndexedEntry) {
        self.entries.insert(path.to_path_buf(), entry);
        self.rebuild_stats();
    }

    fn remove_path(&mut self, path: &Path) {
        self.entries
            .retain(|entry_path, _| !is_same_or_descendant(entry_path, path));
        self.rebuild_stats();
    }

    fn remove_children_of(&mut self, path: &Path) {
        self.entries
            .retain(|entry_path, _| entry_path == path || !entry_path.starts_with(path));
        self.rebuild_stats();
    }

    fn rebuild_stats(&mut self) {
        self.stats.indexed_files = self
            .entries
            .values()
            .filter(|entry| entry.kind == EntryKind::File)
            .count();
        self.stats.indexed_directories = self
            .entries
            .values()
            .filter(|entry| entry.kind == EntryKind::Directory)
            .count();
        self.stats.indexed_symlinks = self
            .entries
            .values()
            .filter(|entry| entry.kind == EntryKind::Symlink)
            .count();
    }
}

fn install_watchers(roots: &[PathBuf], event_tx: mpsc::UnboundedSender<Event>) -> Vec<PollWatcher> {
    let mut watchers = Vec::new();

    for root in roots {
        let tx = event_tx.clone();
        let Ok(mut watcher) = PollWatcher::new(
            move |result: notify::Result<Event>| {
                if let Ok(event) = result {
                    let _ = tx.send(event);
                }
            },
            NotifyConfig::default()
                .with_poll_interval(Duration::from_millis(200))
                .with_compare_contents(true),
        ) else {
            continue;
        };

        if watcher.watch(root, RecursiveMode::Recursive).is_ok() {
            watchers.push(watcher);
        }
    }

    watchers
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

fn score_entry(query: &str, entry: &IndexedEntry) -> Option<i64> {
    let file_name_score = fuzzy_score(query, &entry.file_name)?;
    let path_score = fuzzy_score(query, &entry.relative_path.to_string_lossy())?;
    Some(file_name_score * 3 + path_score)
}

fn fuzzy_score(query: &str, candidate: &str) -> Option<i64> {
    let query = query.to_lowercase();
    let candidate = candidate.to_lowercase();

    if let Some(position) = candidate.find(&query) {
        return Some(10_000 - position as i64 - candidate.len() as i64);
    }

    let mut score = 0_i64;
    let mut cursor = 0_usize;
    for ch in query.chars() {
        let remainder = candidate.get(cursor..)?;
        let position = remainder.find(ch)?;
        score += 100 - position as i64;
        cursor += position + ch.len_utf8();
    }
    Some(score - candidate.len() as i64)
}

#[cfg(test)]
mod tests {
    use super::FileIndexHandle;
    use crate::indexer::types::IndexConfig;
    use anyhow::{Result, anyhow};
    use std::{path::Path, time::Duration};
    use tempfile::tempdir;
    use tokio::{fs, runtime::Handle, time::sleep};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn initial_scan_indexes_files_and_exclusions() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path();
        fs::create_dir(root.join("notes")).await?;
        fs::create_dir(root.join("node_modules")).await?;
        fs::create_dir(root.join(".secret")).await?;
        fs::write(root.join("notes/today.md"), "beam").await?;
        fs::write(root.join("node_modules/ignored.js"), "ignored").await?;
        fs::write(root.join(".secret/hidden.txt"), "hidden").await?;

        let handle = FileIndexHandle::spawn(
            IndexConfig::with_roots(vec![root.to_path_buf()]),
            Handle::current(),
        );
        let snapshot = handle.snapshot().await?;
        assert!(snapshot.stats.initial_scan_complete);
        assert_eq!(snapshot.stats.indexed_files, 1);

        let hits = handle.search("today", 10).await?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry.file_name, "today.md");
        handle.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watcher_keeps_index_current() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path();
        let handle = FileIndexHandle::spawn(
            IndexConfig::with_roots(vec![root.to_path_buf()]),
            Handle::current(),
        );
        let _ = handle.snapshot().await?;

        let fresh_path = root.join("fresh.txt");
        fs::write(&fresh_path, "fresh").await?;
        wait_for_search_hit(&handle, "fresh", &fresh_path).await?;

        fs::remove_file(&fresh_path).await?;
        wait_for_search_miss(&handle, "fresh").await?;

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
