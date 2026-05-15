use crate::indexer::types::{EntryKind, IndexConfig, IndexedEntry};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::{
    collections::{HashMap, VecDeque},
    fs::Metadata,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs, task::JoinSet};

#[derive(Debug, Default)]
pub(crate) struct ScanOutcome {
    pub(crate) entries: HashMap<PathBuf, IndexedEntry>,
}

pub(crate) async fn scan_full(config: &IndexConfig) -> Result<ScanOutcome> {
    let matcher = Arc::new(build_exclusions(config)?);
    let mut outcome = ScanOutcome::default();

    for root in &config.roots {
        if let Some((entry, metadata)) = load_existing_entry(root, root, &matcher, config).await? {
            let entry_path = entry.absolute_path.clone();
            let is_directory = metadata.is_dir();
            outcome.entries.insert(entry_path, entry);

            if is_directory {
                let subtree =
                    scan_directory_subtree(root.clone(), root.clone(), matcher.clone(), config)
                        .await?;
                outcome.entries.extend(subtree.entries);
            }
        }
    }

    Ok(outcome)
}

pub(crate) async fn scan_subtree(
    config: &IndexConfig,
    root: &Path,
    subtree: &Path,
) -> Result<ScanOutcome> {
    let matcher = Arc::new(build_exclusions(config)?);
    let mut outcome = ScanOutcome::default();

    if let Some((entry, metadata)) = load_existing_entry(root, subtree, &matcher, config).await? {
        let entry_path = entry.absolute_path.clone();
        let is_directory = metadata.is_dir();
        if subtree != root {
            outcome.entries.insert(entry_path, entry);
        }

        if is_directory {
            let nested =
                scan_directory_subtree(root.to_path_buf(), subtree.to_path_buf(), matcher, config)
                    .await?;
            outcome.entries.extend(nested.entries);
        }
    }

    Ok(outcome)
}

async fn scan_directory_subtree(
    root: PathBuf,
    subtree: PathBuf,
    matcher: Arc<GlobSet>,
    config: &IndexConfig,
) -> Result<ScanOutcome> {
    let mut outcome = ScanOutcome::default();
    let mut queue = VecDeque::from([subtree]);
    let mut join_set = JoinSet::new();
    let concurrency = config.max_concurrency;

    while !queue.is_empty() || !join_set.is_empty() {
        while join_set.len() < concurrency {
            let Some(directory) = queue.pop_front() else {
                break;
            };
            let root = root.clone();
            let matcher = matcher.clone();
            let config = config.clone();
            join_set.spawn(async move { scan_directory(root, directory, matcher, &config).await });
        }

        let Some(result) = join_set.join_next().await else {
            break;
        };
        let scan = result.context("directory scan task panicked")??;
        queue.extend(scan.child_directories);
        outcome.entries.extend(scan.entries);
    }

    Ok(outcome)
}

struct DirectoryScan {
    entries: HashMap<PathBuf, IndexedEntry>,
    child_directories: Vec<PathBuf>,
}

async fn scan_directory(
    root: PathBuf,
    directory: PathBuf,
    matcher: Arc<GlobSet>,
    config: &IndexConfig,
) -> Result<DirectoryScan> {
    let mut reader = fs::read_dir(&directory)
        .await
        .with_context(|| format!("failed to read {}", directory.display()))?;
    let mut entries = HashMap::new();
    let mut child_directories = Vec::new();

    while let Some(dir_entry) = reader.next_entry().await? {
        let path = dir_entry.path();
        let metadata = fs::symlink_metadata(&path).await?;
        if should_skip(&root, &path, &metadata, config, &matcher) {
            continue;
        }

        let entry = build_indexed_entry(&root, &path, &metadata)?;
        let should_descend =
            metadata.is_dir() && (config.follow_symlinks || !metadata.file_type().is_symlink());

        entries.insert(path.clone(), entry);
        if should_descend {
            child_directories.push(path);
        }
    }

    Ok(DirectoryScan {
        entries,
        child_directories,
    })
}

async fn load_existing_entry(
    root: &Path,
    path: &Path,
    matcher: &GlobSet,
    config: &IndexConfig,
) -> Result<Option<(IndexedEntry, Metadata)>> {
    let metadata = match fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    if path != root && should_skip(root, path, &metadata, config, matcher) {
        return Ok(None);
    }

    Ok(Some((
        build_indexed_entry(root, path, &metadata)?,
        metadata,
    )))
}

fn build_indexed_entry(root: &Path, path: &Path, metadata: &Metadata) -> Result<IndexedEntry> {
    let relative_path = path.strip_prefix(root).unwrap_or(path).to_path_buf();
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| path.display().to_string());
    let modified_at = metadata.modified().ok().map(DateTime::<Utc>::from);
    let kind = if metadata.file_type().is_symlink() {
        EntryKind::Symlink
    } else if metadata.is_dir() {
        EntryKind::Directory
    } else {
        EntryKind::File
    };
    let extension = path
        .extension()
        .map(|extension| extension.to_string_lossy().to_string());

    Ok(IndexedEntry {
        root: root.to_path_buf(),
        absolute_path: path.to_path_buf(),
        relative_path,
        file_name,
        extension,
        kind,
        size_bytes: metadata.len(),
        modified_at,
        is_hidden: is_hidden(path, metadata),
    })
}

fn should_skip(
    root: &Path,
    path: &Path,
    metadata: &Metadata,
    config: &IndexConfig,
    matcher: &GlobSet,
) -> bool {
    if config.ignore_hidden && is_hidden(path, metadata) {
        return true;
    }

    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    let mut normalized = relative.to_string_lossy().replace('\\', "/");
    if metadata.is_dir() {
        normalized.push('/');
    }
    matcher.is_match(normalized)
}

fn build_exclusions(config: &IndexConfig) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in &config.exclude_globs {
        builder.add(Glob::new(pattern)?);
    }
    builder.build().context("failed to build exclusion matcher")
}

fn is_hidden(path: &Path, metadata: &Metadata) -> bool {
    path.file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with('.'))
        || windows_hidden(metadata)
}

#[cfg(target_os = "windows")]
fn windows_hidden(metadata: &Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
    metadata.file_attributes() & FILE_ATTRIBUTE_HIDDEN != 0
}

#[cfg(not(target_os = "windows"))]
fn windows_hidden(_metadata: &Metadata) -> bool {
    false
}
