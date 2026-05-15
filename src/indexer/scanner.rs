use crate::indexer::types::{EntryKind, IndexConfig, IndexedEntry};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use globset::{Glob, GlobSet, GlobSetBuilder};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use std::{
    collections::{HashMap, VecDeque},
    fs::Metadata,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs, task::JoinSet};

const IGNORE_FILE_NAMES: &[&str] = &[".gitignore", ".ignore", ".beamignore"];

#[derive(Clone, Debug)]
pub(crate) struct ScannedDocument {
    pub(crate) entry: IndexedEntry,
    pub(crate) content: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ScanOutcome {
    pub(crate) documents: HashMap<PathBuf, ScannedDocument>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScanTarget {
    pub(crate) root: PathBuf,
    pub(crate) path: PathBuf,
    pub(crate) explicit_root: bool,
}

pub(crate) async fn scan_full(config: &IndexConfig) -> Result<ScanOutcome> {
    let targets = config
        .roots
        .iter()
        .cloned()
        .map(|root| ScanTarget {
            path: root.clone(),
            root,
            explicit_root: true,
        })
        .collect::<Vec<_>>();
    scan_targets(config, targets).await
}

pub(crate) async fn scan_targets(
    config: &IndexConfig,
    targets: Vec<ScanTarget>,
) -> Result<ScanOutcome> {
    let matcher = Arc::new(build_exclusions(config)?);
    let mut outcome = ScanOutcome::default();

    for target in targets {
        if let Some(document) = load_existing_document(
            config,
            &target.root,
            &target.path,
            &matcher,
            target.explicit_root,
            &[],
        )
        .await?
        {
            let is_directory = document.entry.kind == EntryKind::Directory;
            let document_path = document.entry.absolute_path.clone();
            outcome.documents.insert(document_path, document);

            if is_directory {
                let subtree = scan_directory_subtree(
                    config.clone(),
                    target.root,
                    target.path,
                    matcher.clone(),
                )
                .await?;
                outcome.documents.extend(subtree.documents);
            }
        }
    }

    Ok(outcome)
}

async fn scan_directory_subtree(
    config: IndexConfig,
    root: PathBuf,
    subtree: PathBuf,
    matcher: Arc<GlobSet>,
) -> Result<ScanOutcome> {
    let mut outcome = ScanOutcome::default();
    let root_matchers = load_ignore_matchers(&subtree).await?;
    let mut queue = VecDeque::from([DirectoryJob {
        root,
        directory: subtree,
        ignore_matchers: root_matchers,
    }]);
    let mut join_set = JoinSet::new();

    while !queue.is_empty() || !join_set.is_empty() {
        while join_set.len() < config.max_concurrency {
            let Some(job) = queue.pop_front() else {
                break;
            };
            let matcher = matcher.clone();
            let config = config.clone();
            join_set.spawn(async move { scan_directory(job, matcher, &config).await });
        }

        let Some(scan_result) = join_set.join_next().await else {
            break;
        };
        let scan = scan_result.context("directory scan task panicked")??;
        queue.extend(scan.child_directories);
        outcome.documents.extend(scan.documents);
    }

    Ok(outcome)
}

#[derive(Clone)]
struct DirectoryJob {
    root: PathBuf,
    directory: PathBuf,
    ignore_matchers: Vec<Arc<Gitignore>>,
}

struct DirectoryScan {
    documents: HashMap<PathBuf, ScannedDocument>,
    child_directories: Vec<DirectoryJob>,
}

async fn scan_directory(
    job: DirectoryJob,
    matcher: Arc<GlobSet>,
    config: &IndexConfig,
) -> Result<DirectoryScan> {
    let local_ignore_matchers = load_ignore_matchers(&job.directory).await?;
    let mut active_matchers = job.ignore_matchers.clone();
    active_matchers.extend(local_ignore_matchers);

    let mut reader = fs::read_dir(&job.directory)
        .await
        .with_context(|| format!("failed to read {}", job.directory.display()))?;
    let mut documents = HashMap::new();
    let mut child_directories = Vec::new();

    while let Some(dir_entry) = reader.next_entry().await? {
        let path = dir_entry.path();
        let metadata = fs::symlink_metadata(&path).await?;

        if should_skip(
            &job.root,
            &path,
            &metadata,
            config,
            &matcher,
            &active_matchers,
            false,
        ) {
            continue;
        }

        let document = build_scanned_document(config, &job.root, &path, &metadata).await?;
        let should_descend =
            metadata.is_dir() && (config.follow_symlinks || !metadata.file_type().is_symlink());
        let document_path = document.entry.absolute_path.clone();

        documents.insert(document_path, document);
        if should_descend {
            child_directories.push(DirectoryJob {
                root: job.root.clone(),
                directory: path,
                ignore_matchers: active_matchers.clone(),
            });
        }
    }

    Ok(DirectoryScan {
        documents,
        child_directories,
    })
}

async fn load_existing_document(
    config: &IndexConfig,
    root: &Path,
    path: &Path,
    matcher: &GlobSet,
    explicit_root: bool,
    ignore_matchers: &[Arc<Gitignore>],
) -> Result<Option<ScannedDocument>> {
    let metadata = match fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    if should_skip(
        root,
        path,
        &metadata,
        config,
        matcher,
        ignore_matchers,
        explicit_root,
    ) {
        return Ok(None);
    }

    build_scanned_document(config, root, path, &metadata)
        .await
        .map(Some)
}

async fn build_scanned_document(
    config: &IndexConfig,
    root: &Path,
    path: &Path,
    metadata: &Metadata,
) -> Result<ScannedDocument> {
    Ok(ScannedDocument {
        content: maybe_read_content(config, path, metadata).await?,
        entry: build_indexed_entry(root, path, metadata)?,
    })
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
    ignore_matchers: &[Arc<Gitignore>],
    explicit_root: bool,
) -> bool {
    if !explicit_root && config.ignore_hidden && is_hidden(path, metadata) {
        return true;
    }

    if !explicit_root {
        let Ok(relative) = path.strip_prefix(root) else {
            return false;
        };
        let mut normalized = relative.to_string_lossy().replace('\\', "/");
        if metadata.is_dir() {
            normalized.push('/');
        }
        if matcher.is_match(&normalized) {
            return true;
        }
    }

    if !config.respect_ignore_files {
        return false;
    }

    match ignore_match_result(ignore_matchers, path, metadata.is_dir()) {
        Some(IgnoreMatch::Ignore) => true,
        Some(IgnoreMatch::Whitelist) | None => false,
    }
}

fn build_exclusions(config: &IndexConfig) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in &config.exclude_globs {
        builder.add(Glob::new(pattern)?);
    }
    builder.build().context("failed to build exclusion matcher")
}

async fn load_ignore_matchers(directory: &Path) -> Result<Vec<Arc<Gitignore>>> {
    let mut matchers = Vec::new();
    for file_name in IGNORE_FILE_NAMES {
        let path = directory.join(file_name);
        let contents = match fs::read_to_string(&path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let mut builder = GitignoreBuilder::new(directory);
        for line in contents.lines() {
            builder.add_line(Some(path.clone()), line)?;
        }
        let matcher = builder
            .build()
            .with_context(|| format!("failed to build ignore matcher for {}", path.display()))?;
        matchers.push(Arc::new(matcher));
    }
    Ok(matchers)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IgnoreMatch {
    Ignore,
    Whitelist,
}

fn ignore_match_result(
    matchers: &[Arc<Gitignore>],
    path: &Path,
    is_directory: bool,
) -> Option<IgnoreMatch> {
    let mut decision = None;
    for matcher in matchers {
        let matched = matcher.matched(path, is_directory);
        if matched.is_ignore() {
            decision = Some(IgnoreMatch::Ignore);
        } else if matched.is_whitelist() {
            decision = Some(IgnoreMatch::Whitelist);
        }
    }
    decision
}

async fn maybe_read_content(
    config: &IndexConfig,
    path: &Path,
    metadata: &Metadata,
) -> Result<Option<String>> {
    if !config.index_contents
        || !metadata.is_file()
        || metadata.len() > config.max_file_size_bytes
        || !is_text_candidate(path)
    {
        return Ok(None);
    }

    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if bytes.contains(&0) {
        return Ok(None);
    }

    let Ok(content) = String::from_utf8(bytes) else {
        return Ok(None);
    };
    let normalized = content.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return Ok(None);
    }

    Ok(Some(normalized))
}

fn is_text_candidate(path: &Path) -> bool {
    let Some(extension) = path.extension() else {
        return false;
    };
    matches!(
        extension.to_string_lossy().to_ascii_lowercase().as_str(),
        "c" | "cc"
            | "cpp"
            | "css"
            | "go"
            | "h"
            | "html"
            | "java"
            | "js"
            | "json"
            | "jsx"
            | "md"
            | "py"
            | "rb"
            | "rs"
            | "scss"
            | "sh"
            | "sql"
            | "swift"
            | "toml"
            | "ts"
            | "tsx"
            | "txt"
            | "xml"
            | "yaml"
            | "yml"
    )
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

#[cfg(test)]
mod tests {
    use super::{ScanTarget, scan_full, scan_targets};
    use crate::indexer::{IndexConfig, IndexStorageConfig};
    use anyhow::Result;
    use tempfile::tempdir;
    use tokio::fs;

    fn test_config(root: &std::path::Path) -> IndexConfig {
        IndexConfig::with_roots(vec![root.to_path_buf()]).with_storage(IndexStorageConfig::new(
            root.join(".beam-data"),
            root.join(".beam-state"),
        ))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_respects_hidden_and_ignore_files() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path();
        fs::create_dir(root.join(".secret")).await?;
        fs::create_dir(root.join("build")).await?;
        fs::write(root.join(".gitignore"), "build/\n").await?;
        fs::write(root.join(".secret/token.txt"), "hidden").await?;
        fs::write(root.join("build/output.log"), "ignored").await?;
        fs::write(root.join("visible.md"), "kept").await?;

        let outcome = scan_full(&test_config(root)).await?;

        assert!(outcome.documents.contains_key(&root.join("visible.md")));
        assert!(
            !outcome
                .documents
                .contains_key(&root.join(".secret/token.txt"))
        );
        assert!(
            !outcome
                .documents
                .contains_key(&root.join("build/output.log"))
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn explicit_root_is_kept_even_if_hidden() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join(".workspace");
        fs::create_dir(&root).await?;
        fs::write(root.join("keep.txt"), "kept").await?;

        let outcome = scan_full(&test_config(&root)).await?;

        assert!(outcome.documents.contains_key(&root));
        assert!(outcome.documents.contains_key(&root.join("keep.txt")));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subtree_scan_can_index_small_text_content() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path();
        fs::write(root.join("notes.md"), "beam launcher").await?;

        let mut config = test_config(root);
        config.index_contents = true;

        let outcome = scan_targets(
            &config,
            vec![ScanTarget {
                root: root.to_path_buf(),
                path: root.join("notes.md"),
                explicit_root: false,
            }],
        )
        .await?;

        let scanned = outcome
            .documents
            .get(&root.join("notes.md"))
            .expect("file should be indexed");
        assert_eq!(scanned.content.as_deref(), Some("beam launcher"));
        Ok(())
    }
}
