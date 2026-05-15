use crate::indexer::{
    scanner::ScannedDocument,
    storage::IndexPaths,
    types::{EntryKind, IndexedEntry, SearchHit},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tantivy::{
    Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term, doc,
    query::{AllQuery, QueryParser},
    schema::{
        FAST, Field, INDEXED, STORED, STRING, Schema, SchemaBuilder, TextFieldIndexing,
        TextOptions, Value,
    },
};
use tokio::task;

const WRITER_HEAP_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct SearchBackend {
    inner: Arc<Mutex<BackendInner>>,
}

impl SearchBackend {
    pub(crate) async fn open(paths: IndexPaths) -> Result<Self> {
        let inner = task::spawn_blocking(move || BackendInner::open(paths))
            .await
            .context("backend open task panicked")??;

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub(crate) async fn stored_entries(&self) -> Result<HashMap<PathBuf, IndexedEntry>> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || {
            let guard = inner.lock().expect("backend mutex poisoned");
            Ok(guard.documents.clone())
        })
        .await
        .context("stored entries task panicked")?
    }

    pub(crate) async fn replace_all(
        &self,
        documents: HashMap<PathBuf, ScannedDocument>,
    ) -> Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || {
            let mut guard = inner.lock().expect("backend mutex poisoned");
            guard.replace_all(documents)
        })
        .await
        .context("replace_all task panicked")?
    }

    pub(crate) async fn apply_delta(
        &self,
        additions: HashMap<PathBuf, ScannedDocument>,
        deletions: Vec<PathBuf>,
    ) -> Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || {
            let mut guard = inner.lock().expect("backend mutex poisoned");
            guard.apply_delta(additions, deletions)
        })
        .await
        .context("apply_delta task panicked")?
    }

    pub(crate) async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let inner = self.inner.clone();
        let query = query.to_string();
        task::spawn_blocking(move || {
            let guard = inner.lock().expect("backend mutex poisoned");
            guard.search(&query, limit)
        })
        .await
        .context("search task panicked")?
    }
}

struct BackendInner {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
    fields: SchemaFields,
    documents: HashMap<PathBuf, IndexedEntry>,
}

impl BackendInner {
    fn open(paths: IndexPaths) -> Result<Self> {
        let schema = build_schema();
        let index = Index::open_or_create(
            tantivy::directory::MmapDirectory::open(&paths.db_dir)?,
            schema.clone(),
        )?;
        let fields = SchemaFields::new(&schema)?;
        let writer = index.writer(WRITER_HEAP_BYTES)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        let documents = load_documents(&reader, &fields)?;

        Ok(Self {
            index,
            reader,
            writer,
            fields,
            documents,
        })
    }

    fn replace_all(&mut self, documents: HashMap<PathBuf, ScannedDocument>) -> Result<()> {
        self.writer.delete_all_documents()?;
        self.documents.clear();

        for scanned in documents.into_values() {
            self.add_document(scanned)?;
        }

        self.commit()
    }

    fn apply_delta(
        &mut self,
        additions: HashMap<PathBuf, ScannedDocument>,
        deletions: Vec<PathBuf>,
    ) -> Result<()> {
        for deletion in deletions {
            self.writer.delete_term(Term::from_field_text(
                self.fields.absolute_path,
                &path_key(&deletion),
            ));
            self.documents.remove(&deletion);
        }

        for scanned in additions.into_values() {
            self.writer.delete_term(Term::from_field_text(
                self.fields.absolute_path,
                &path_key(&scanned.entry.absolute_path),
            ));
            self.add_document(scanned)?;
        }

        self.commit()
    }

    fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(self.default_hits(limit));
        }

        let mut candidates = self.tantivy_candidates(trimmed, limit)?;
        if candidates.is_empty() {
            candidates = self
                .documents
                .values()
                .filter_map(|entry| {
                    fuzzy_score(trimmed, entry).map(|score| SearchHit {
                        entry: entry.clone(),
                        score,
                    })
                })
                .collect();
        }

        candidates.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.entry.absolute_path.cmp(&right.entry.absolute_path))
        });
        candidates.truncate(limit);
        Ok(candidates)
    }

    fn tantivy_candidates(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let searcher = self.reader.searcher();
        let parser = QueryParser::for_index(
            &self.index,
            vec![
                self.fields.file_name,
                self.fields.relative_path,
                self.fields.extension,
                self.fields.content,
            ],
        );
        let parsed_query = match parser.parse_query(query) {
            Ok(parsed_query) => parsed_query,
            Err(_) => return Ok(Vec::new()),
        };
        let top_docs = searcher.search(
            &parsed_query,
            &tantivy::collector::TopDocs::with_limit(limit.saturating_mul(8).max(limit)),
        )?;

        Ok(top_docs
            .into_iter()
            .filter_map(|(backend_score, address)| {
                let document: TantivyDocument = searcher.doc(address).ok()?;
                let entry = document_to_entry(&document, &self.fields).ok()?;
                let fuzzy = fuzzy_score(query, &entry).unwrap_or_default();
                Some(SearchHit {
                    entry,
                    score: fuzzy.saturating_mul(100) + backend_score.round() as i64,
                })
            })
            .collect())
    }

    fn default_hits(&self, limit: usize) -> Vec<SearchHit> {
        let mut entries = self.documents.values().cloned().collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            right
                .modified_at
                .cmp(&left.modified_at)
                .then_with(|| left.absolute_path.cmp(&right.absolute_path))
        });
        entries
            .into_iter()
            .take(limit)
            .map(|entry| SearchHit { entry, score: 0 })
            .collect()
    }

    fn add_document(&mut self, scanned: ScannedDocument) -> Result<()> {
        let mut document = doc!(
            self.fields.absolute_path => path_key(&scanned.entry.absolute_path),
            self.fields.root => path_key(&scanned.entry.root),
            self.fields.relative_path => path_key(&scanned.entry.relative_path),
            self.fields.file_name => scanned.entry.file_name.clone(),
            self.fields.kind => kind_to_u64(scanned.entry.kind),
            self.fields.size_bytes => scanned.entry.size_bytes,
            self.fields.is_hidden => scanned.entry.is_hidden,
        );

        if let Some(extension) = &scanned.entry.extension {
            document.add_text(self.fields.extension, extension);
        }
        if let Some(modified_at) = &scanned.entry.modified_at {
            document.add_i64(self.fields.modified_at, modified_at.timestamp());
        }
        if let Some(content) = &scanned.content {
            document.add_text(self.fields.content, content);
        }

        self.writer.add_document(document)?;
        self.documents
            .insert(scanned.entry.absolute_path.clone(), scanned.entry);
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        self.reader.reload()?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct SchemaFields {
    absolute_path: Field,
    root: Field,
    relative_path: Field,
    file_name: Field,
    extension: Field,
    kind: Field,
    size_bytes: Field,
    modified_at: Field,
    is_hidden: Field,
    content: Field,
}

impl SchemaFields {
    fn new(schema: &Schema) -> Result<Self> {
        Ok(Self {
            absolute_path: schema.get_field("absolute_path")?,
            root: schema.get_field("root")?,
            relative_path: schema.get_field("relative_path")?,
            file_name: schema.get_field("file_name")?,
            extension: schema.get_field("extension")?,
            kind: schema.get_field("kind")?,
            size_bytes: schema.get_field("size_bytes")?,
            modified_at: schema.get_field("modified_at")?,
            is_hidden: schema.get_field("is_hidden")?,
            content: schema.get_field("content")?,
        })
    }
}

fn build_schema() -> Schema {
    let mut builder = SchemaBuilder::default();
    let text_options = TextOptions::default().set_stored().set_indexing_options(
        TextFieldIndexing::default()
            .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
    );

    builder.add_text_field("absolute_path", STRING | STORED);
    builder.add_text_field("root", STRING | STORED);
    builder.add_text_field("relative_path", text_options.clone());
    builder.add_text_field("file_name", text_options);
    builder.add_text_field("extension", STRING | STORED);
    builder.add_u64_field("kind", INDEXED | FAST | STORED);
    builder.add_u64_field("size_bytes", FAST | STORED);
    builder.add_i64_field("modified_at", FAST | STORED);
    builder.add_bool_field("is_hidden", FAST | STORED);
    builder.add_text_field("content", tantivy::schema::TEXT);
    builder.build()
}

fn load_documents(
    reader: &IndexReader,
    fields: &SchemaFields,
) -> Result<HashMap<PathBuf, IndexedEntry>> {
    let searcher = reader.searcher();
    let total = searcher.num_docs() as usize;
    if total == 0 {
        return Ok(HashMap::new());
    }

    let top_docs = searcher.search(&AllQuery, &tantivy::collector::TopDocs::with_limit(total))?;
    let mut documents = HashMap::with_capacity(top_docs.len());

    for (_, address) in top_docs {
        let document: TantivyDocument = searcher.doc(address)?;
        let entry = document_to_entry(&document, fields)?;
        documents.insert(entry.absolute_path.clone(), entry);
    }

    Ok(documents)
}

fn document_to_entry(document: &TantivyDocument, fields: &SchemaFields) -> Result<IndexedEntry> {
    let root = PathBuf::from(require_str(document, fields.root)?);
    let absolute_path = PathBuf::from(require_str(document, fields.absolute_path)?);
    let relative_path = PathBuf::from(require_str(document, fields.relative_path)?);
    let file_name = require_str(document, fields.file_name)?.to_string();
    let extension = document
        .get_first(fields.extension)
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned);
    let modified_at = document
        .get_first(fields.modified_at)
        .and_then(|value| value.as_i64())
        .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0));
    let kind = kind_from_u64(require_u64(document, fields.kind)?)?;
    let size_bytes = require_u64(document, fields.size_bytes)?;
    let is_hidden = document
        .get_first(fields.is_hidden)
        .and_then(|value| value.as_bool())
        .unwrap_or(false);

    Ok(IndexedEntry {
        root,
        absolute_path,
        relative_path,
        file_name,
        extension,
        kind,
        size_bytes,
        modified_at,
        is_hidden,
    })
}

fn require_str(document: &TantivyDocument, field: Field) -> Result<&str> {
    document
        .get_first(field)
        .and_then(|value| value.as_str())
        .context("missing string field")
}

fn require_u64(document: &TantivyDocument, field: Field) -> Result<u64> {
    document
        .get_first(field)
        .and_then(|value| value.as_u64())
        .context("missing u64 field")
}

fn kind_to_u64(kind: EntryKind) -> u64 {
    match kind {
        EntryKind::File => 1,
        EntryKind::Directory => 2,
        EntryKind::Symlink => 3,
    }
}

fn kind_from_u64(value: u64) -> Result<EntryKind> {
    match value {
        1 => Ok(EntryKind::File),
        2 => Ok(EntryKind::Directory),
        3 => Ok(EntryKind::Symlink),
        _ => anyhow::bail!("unknown entry kind {value}"),
    }
}

fn path_key(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

fn fuzzy_score(query: &str, entry: &IndexedEntry) -> Option<i64> {
    let file_name_score = fuzzy_match(query, &entry.file_name)?;
    let path_score = fuzzy_match(query, &entry.relative_path.to_string_lossy())?;
    Some(file_name_score.saturating_mul(3) + path_score)
}

fn fuzzy_match(query: &str, candidate: &str) -> Option<i64> {
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
    use super::SearchBackend;
    use crate::indexer::{
        IndexStorageConfig,
        scanner::ScannedDocument,
        storage::{IndexPaths, prepare_storage},
        types::{EntryKind, IndexedEntry},
    };
    use anyhow::Result;
    use chrono::Utc;
    use std::{collections::HashMap, path::PathBuf};
    use tempfile::tempdir;

    fn entry(root: &std::path::Path, relative: &str) -> ScannedDocument {
        let absolute_path = root.join(relative);
        ScannedDocument {
            entry: IndexedEntry {
                root: root.to_path_buf(),
                absolute_path: absolute_path.clone(),
                relative_path: PathBuf::from(relative),
                file_name: absolute_path
                    .file_name()
                    .expect("file should have name")
                    .to_string_lossy()
                    .to_string(),
                extension: absolute_path
                    .extension()
                    .map(|extension| extension.to_string_lossy().to_string()),
                kind: EntryKind::File,
                size_bytes: 42,
                modified_at: Some(Utc::now()),
                is_hidden: false,
            },
            content: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persisted_index_survives_restart() -> Result<()> {
        let temp = tempdir()?;
        let storage = IndexStorageConfig::new(temp.path().join("data"), temp.path().join("state"));
        let paths = IndexPaths::new(&storage);
        prepare_storage(&paths).await?;

        let root = temp.path().join("root");
        let mut documents = HashMap::new();
        documents.insert(root.join("notes.md"), entry(&root, "notes.md"));

        let backend = SearchBackend::open(paths.clone()).await?;
        backend.replace_all(documents).await?;
        drop(backend);

        let reopened = SearchBackend::open(paths).await?;
        let stored = reopened.stored_entries().await?;
        assert!(stored.contains_key(&root.join("notes.md")));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_prefers_name_matches() -> Result<()> {
        let temp = tempdir()?;
        let storage = IndexStorageConfig::new(temp.path().join("data"), temp.path().join("state"));
        let paths = IndexPaths::new(&storage);
        prepare_storage(&paths).await?;
        let root = temp.path().join("root");

        let mut documents = HashMap::new();
        documents.insert(root.join("docs/today.md"), entry(&root, "docs/today.md"));
        documents.insert(
            root.join("logs/archive.txt"),
            entry(&root, "logs/archive.txt"),
        );

        let backend = SearchBackend::open(paths).await?;
        backend.replace_all(documents).await?;

        let hits = backend.search("today", 5).await?;
        assert_eq!(
            hits.first().map(|hit| hit.entry.file_name.as_str()),
            Some("today.md")
        );
        Ok(())
    }
}
