use crate::indexer::{
    scanner::ScannedDocument,
    storage::IndexPaths,
    types::{EntryContentType, EntryKind, IndexedEntry, SearchHit},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
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
const DEFAULT_CANDIDATE_LIMIT: usize = 128;

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
            Ok(guard
                .documents
                .iter()
                .map(|(path, document)| (path.clone(), document.entry.clone()))
                .collect())
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
    documents: HashMap<PathBuf, SearchDocument>,
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

        let normalized_query = normalize_for_search(trimmed);
        let mut candidates = self.tantivy_candidates(trimmed, &normalized_query, limit)?;
        if candidates.is_empty() {
            candidates = self.fallback_candidates(trimmed, &normalized_query);
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

    fn tantivy_candidates(
        &self,
        raw_query: &str,
        normalized_query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        let searcher = self.reader.searcher();
        let parser = QueryParser::for_index(
            &self.index,
            vec![self.fields.search_text, self.fields.content],
        );
        let parsed_query = match parser
            .parse_query(normalized_query)
            .or_else(|_| parser.parse_query(raw_query))
        {
            Ok(parsed_query) => parsed_query,
            Err(_) => return Ok(Vec::new()),
        };

        let candidate_limit = limit
            .saturating_mul(16)
            .max(limit)
            .max(DEFAULT_CANDIDATE_LIMIT);
        let top_docs = searcher.search(
            &parsed_query,
            &tantivy::collector::TopDocs::with_limit(candidate_limit),
        )?;

        let mut hits = Vec::with_capacity(top_docs.len());
        for (rank, (backend_score, address)) in top_docs.into_iter().enumerate() {
            let document: TantivyDocument = searcher.doc(address)?;
            let absolute_path = PathBuf::from(require_str(&document, self.fields.absolute_path)?);
            let Some(candidate) = self.documents.get(&absolute_path) else {
                continue;
            };
            if let Some(score) = score_document(
                raw_query,
                normalized_query,
                candidate,
                Some(CandidateSignal {
                    rank,
                    backend_score,
                }),
                Utc::now(),
            ) {
                hits.push(SearchHit {
                    entry: candidate.entry.clone(),
                    score,
                });
            }
        }

        Ok(hits)
    }

    fn fallback_candidates(&self, raw_query: &str, normalized_query: &str) -> Vec<SearchHit> {
        let now = Utc::now();
        self.documents
            .values()
            .filter_map(|candidate| {
                let score = score_document(raw_query, normalized_query, candidate, None, now)?;
                Some(SearchHit {
                    entry: candidate.entry.clone(),
                    score,
                })
            })
            .collect()
    }

    fn default_hits(&self, limit: usize) -> Vec<SearchHit> {
        let mut entries = self.documents.values().collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            right
                .entry
                .last_accessed_at
                .or(right.entry.modified_at)
                .cmp(&left.entry.last_accessed_at.or(left.entry.modified_at))
                .then_with(|| left.entry.absolute_path.cmp(&right.entry.absolute_path))
        });
        entries
            .into_iter()
            .take(limit)
            .map(|document| SearchHit {
                entry: document.entry.clone(),
                score: 0,
            })
            .collect()
    }

    fn add_document(&mut self, scanned: ScannedDocument) -> Result<()> {
        let search_text = search_text_for_entry(&scanned.entry);
        let mut document = doc!(
            self.fields.absolute_path => path_key(&scanned.entry.absolute_path),
            self.fields.root => path_key(&scanned.entry.root),
            self.fields.relative_path => path_key(&scanned.entry.relative_path),
            self.fields.file_name => scanned.entry.file_name.clone(),
            self.fields.search_text => search_text,
            self.fields.kind => kind_to_u64(scanned.entry.kind),
            self.fields.content_type => content_type_to_u64(scanned.entry.content_type),
            self.fields.size_bytes => scanned.entry.size_bytes,
            self.fields.is_hidden => scanned.entry.is_hidden,
        );

        if let Some(extension) = &scanned.entry.extension {
            document.add_text(self.fields.extension, extension);
        }
        if let Some(modified_at) = &scanned.entry.modified_at {
            document.add_i64(self.fields.modified_at, modified_at.timestamp());
        }
        if let Some(last_accessed_at) = &scanned.entry.last_accessed_at {
            document.add_i64(self.fields.last_accessed_at, last_accessed_at.timestamp());
        }
        if let Some(content) = &scanned.content {
            document.add_text(self.fields.content, content);
        }

        let cached = SearchDocument::new(scanned.entry);
        self.writer.add_document(document)?;
        self.documents
            .insert(cached.entry.absolute_path.clone(), cached);
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        self.reader.reload()?;
        Ok(())
    }
}

#[derive(Clone)]
struct SearchDocument {
    entry: IndexedEntry,
    normalized_name: String,
    normalized_stem: String,
    normalized_path: String,
    compact_name: String,
    compact_path: String,
    component_terms: Vec<String>,
    depth: usize,
    root_priority: i64,
}

impl SearchDocument {
    fn new(entry: IndexedEntry) -> Self {
        let normalized_name = normalize_for_search(&entry.file_name);
        let stem = Path::new(&entry.file_name)
            .file_stem()
            .map(|stem| stem.to_string_lossy().to_string())
            .unwrap_or_else(|| entry.file_name.clone());
        let normalized_stem = normalize_for_search(&stem);
        let normalized_path = normalize_for_search(&entry.relative_path.to_string_lossy());
        let component_terms = entry
            .relative_path
            .components()
            .filter_map(|component| {
                let normalized = normalize_for_search(&component.as_os_str().to_string_lossy());
                (!normalized.is_empty()).then_some(normalized)
            })
            .collect::<Vec<_>>();

        Self {
            compact_name: compact_for_search(&normalized_name),
            compact_path: compact_for_search(&normalized_path),
            depth: entry.relative_path.components().count(),
            root_priority: root_priority(&entry),
            entry,
            normalized_name,
            normalized_stem,
            normalized_path,
            component_terms,
        }
    }
}

#[derive(Clone, Copy)]
struct SchemaFields {
    absolute_path: Field,
    root: Field,
    relative_path: Field,
    file_name: Field,
    search_text: Field,
    extension: Field,
    kind: Field,
    content_type: Field,
    size_bytes: Field,
    modified_at: Field,
    last_accessed_at: Field,
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
            search_text: schema.get_field("search_text")?,
            extension: schema.get_field("extension")?,
            kind: schema.get_field("kind")?,
            content_type: schema.get_field("content_type")?,
            size_bytes: schema.get_field("size_bytes")?,
            modified_at: schema.get_field("modified_at")?,
            last_accessed_at: schema.get_field("last_accessed_at")?,
            is_hidden: schema.get_field("is_hidden")?,
            content: schema.get_field("content")?,
        })
    }
}

fn build_schema() -> Schema {
    let stored_text = TextOptions::default().set_stored().set_indexing_options(
        TextFieldIndexing::default()
            .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
    );
    let indexed_text = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
    );

    let mut builder = SchemaBuilder::default();
    builder.add_text_field("absolute_path", STRING | STORED);
    builder.add_text_field("root", STRING | STORED);
    builder.add_text_field("relative_path", stored_text.clone());
    builder.add_text_field("file_name", stored_text);
    builder.add_text_field("search_text", indexed_text);
    builder.add_text_field("extension", STRING | STORED);
    builder.add_u64_field("kind", INDEXED | FAST | STORED);
    builder.add_u64_field("content_type", INDEXED | FAST | STORED);
    builder.add_u64_field("size_bytes", FAST | STORED);
    builder.add_i64_field("modified_at", FAST | STORED);
    builder.add_i64_field("last_accessed_at", FAST | STORED);
    builder.add_bool_field("is_hidden", FAST | STORED);
    builder.add_text_field("content", tantivy::schema::TEXT);
    builder.build()
}

fn load_documents(
    reader: &IndexReader,
    fields: &SchemaFields,
) -> Result<HashMap<PathBuf, SearchDocument>> {
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
        documents.insert(entry.absolute_path.clone(), SearchDocument::new(entry));
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
    let last_accessed_at = document
        .get_first(fields.last_accessed_at)
        .and_then(|value| value.as_i64())
        .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0));
    let kind = kind_from_u64(require_u64(document, fields.kind)?)?;
    let content_type = content_type_from_u64(require_u64(document, fields.content_type)?)?;
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
        content_type,
        size_bytes,
        modified_at,
        last_accessed_at,
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

fn content_type_to_u64(content_type: EntryContentType) -> u64 {
    match content_type {
        EntryContentType::Application => 1,
        EntryContentType::Archive => 2,
        EntryContentType::Audio => 3,
        EntryContentType::Code => 4,
        EntryContentType::Configuration => 5,
        EntryContentType::Directory => 6,
        EntryContentType::Document => 7,
        EntryContentType::Image => 8,
        EntryContentType::Other => 9,
        EntryContentType::Package => 10,
        EntryContentType::Shortcut => 11,
        EntryContentType::Symlink => 12,
        EntryContentType::Video => 13,
    }
}

fn content_type_from_u64(value: u64) -> Result<EntryContentType> {
    match value {
        1 => Ok(EntryContentType::Application),
        2 => Ok(EntryContentType::Archive),
        3 => Ok(EntryContentType::Audio),
        4 => Ok(EntryContentType::Code),
        5 => Ok(EntryContentType::Configuration),
        6 => Ok(EntryContentType::Directory),
        7 => Ok(EntryContentType::Document),
        8 => Ok(EntryContentType::Image),
        9 => Ok(EntryContentType::Other),
        10 => Ok(EntryContentType::Package),
        11 => Ok(EntryContentType::Shortcut),
        12 => Ok(EntryContentType::Symlink),
        13 => Ok(EntryContentType::Video),
        _ => anyhow::bail!("unknown entry content type {value}"),
    }
}

fn path_key(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

#[derive(Clone, Copy)]
struct MatchWeights {
    exact: i64,
    prefix: i64,
    token_prefix: i64,
    substring: i64,
    fuzzy: i64,
}

#[derive(Clone, Copy)]
struct CandidateSignal {
    rank: usize,
    backend_score: f32,
}

fn score_document(
    raw_query: &str,
    normalized_query: &str,
    document: &SearchDocument,
    signal: Option<CandidateSignal>,
    now: DateTime<Utc>,
) -> Option<i64> {
    let compact_query = compact_for_search(normalized_query);
    let query_tokens = normalized_query.split_whitespace().collect::<Vec<_>>();

    let stem_score = lexical_match_score(
        normalized_query,
        &compact_query,
        &query_tokens,
        &document.normalized_stem,
        &document.compact_name,
        MatchWeights {
            exact: 1_350_000,
            prefix: 1_200_000,
            token_prefix: 1_080_000,
            substring: 960_000,
            fuzzy: 260_000,
        },
    );
    let name_score = lexical_match_score(
        normalized_query,
        &compact_query,
        &query_tokens,
        &document.normalized_name,
        &document.compact_name,
        MatchWeights {
            exact: 1_250_000,
            prefix: 1_120_000,
            token_prefix: 1_000_000,
            substring: 820_000,
            fuzzy: 220_000,
        },
    );
    let path_score = lexical_match_score(
        normalized_query,
        &compact_query,
        &query_tokens,
        &document.normalized_path,
        &document.compact_path,
        MatchWeights {
            exact: 860_000,
            prefix: 720_000,
            token_prefix: 640_000,
            substring: 520_000,
            fuzzy: 160_000,
        },
    );
    let component_score = document
        .component_terms
        .iter()
        .filter_map(|component| {
            lexical_match_score(
                normalized_query,
                &compact_query,
                &query_tokens,
                component,
                &compact_for_search(component),
                MatchWeights {
                    exact: 1_180_000,
                    prefix: 1_040_000,
                    token_prefix: 940_000,
                    substring: 760_000,
                    fuzzy: 200_000,
                },
            )
        })
        .max();

    let primary_match = stem_score
        .into_iter()
        .chain(name_score)
        .chain(component_score)
        .max();
    let secondary_match = path_score.or(component_score).unwrap_or_default() / 8;
    let mut score = primary_match.or_else(|| signal.map(|_| 0))?;
    if primary_match.is_some() {
        score += secondary_match;
    }

    if let Some(extension) = &document.entry.extension
        && normalize_for_search(extension) == normalized_query
    {
        score += 90_000;
    }

    if document.entry.content_type.is_launchable() {
        score += 12_000;
    }
    if document.entry.content_type == EntryContentType::Package {
        score += 4_000;
    }
    if document.entry.kind == EntryKind::Directory {
        score -= 5_000;
    }
    if document.entry.is_hidden {
        score -= 35_000;
    }

    score += document.root_priority;
    score += recency_boost(&document.entry, now);
    score -= document.depth as i64 * 350;

    if let Some(signal) = signal {
        score += backend_rank_bonus(signal.rank);
        score += backend_score_bonus(signal.backend_score);
    } else if raw_query.contains(std::path::MAIN_SEPARATOR) {
        score += 3_000;
    }

    Some(score)
}

fn lexical_match_score(
    normalized_query: &str,
    compact_query: &str,
    query_tokens: &[&str],
    normalized_candidate: &str,
    compact_candidate: &str,
    weights: MatchWeights,
) -> Option<i64> {
    if normalized_query.is_empty() || normalized_candidate.is_empty() {
        return None;
    }

    let mut best = None;

    if normalized_candidate == normalized_query {
        best = Some(weights.exact - normalized_candidate.len() as i64);
    }

    if normalized_candidate.starts_with(normalized_query) {
        let score = weights.prefix - normalized_candidate.len() as i64;
        best = Some(best.map_or(score, |current| current.max(score)));
    }

    if let Some((position, span)) = token_prefix_sequence(normalized_candidate, query_tokens) {
        let score = weights.token_prefix - position as i64 * 40 - span as i64 * 5;
        best = Some(best.map_or(score, |current| current.max(score)));
    }

    if let Some(position) = normalized_candidate.find(normalized_query) {
        let score = weights.substring - position as i64 * 25 - normalized_candidate.len() as i64;
        best = Some(best.map_or(score, |current| current.max(score)));
    }

    if let Some(fuzzy) = fuzzy_match(compact_query, compact_candidate) {
        let score = weights.fuzzy + fuzzy * 20;
        best = Some(best.map_or(score, |current| current.max(score)));
    }

    best
}

fn token_prefix_sequence(candidate: &str, query_tokens: &[&str]) -> Option<(usize, usize)> {
    if query_tokens.is_empty() {
        return None;
    }

    let candidate_tokens = candidate.split_whitespace().collect::<Vec<_>>();
    let mut first_match = None;
    let mut candidate_index = 0;

    for query_token in query_tokens {
        let mut matched = false;
        while let Some(candidate_token) = candidate_tokens.get(candidate_index) {
            if candidate_token.starts_with(query_token) {
                if first_match.is_none() {
                    first_match = Some(candidate_index);
                }
                candidate_index += 1;
                matched = true;
                break;
            }
            candidate_index += 1;
        }
        if !matched {
            return None;
        }
    }

    Some((first_match.unwrap_or_default(), candidate_index))
}

fn backend_rank_bonus(rank: usize) -> i64 {
    24_000_i64.saturating_sub(rank as i64 * 120)
}

fn backend_score_bonus(score: f32) -> i64 {
    (score.max(0.0) * 256.0).round() as i64
}

fn root_priority(entry: &IndexedEntry) -> i64 {
    let root = entry.root.to_string_lossy().to_ascii_lowercase();

    if entry.content_type.is_launchable() {
        if root.contains("/applications") || root.contains("\\applications") {
            return 16_000;
        }
        if root.contains("/program files") || root.contains("\\program files") {
            return 14_000;
        }
        if root.ends_with("/bin") || root.ends_with("\\bin") {
            return 10_000;
        }
    }

    0
}

fn recency_boost(entry: &IndexedEntry, now: DateTime<Utc>) -> i64 {
    let Some(reference) = entry.last_accessed_at.or(entry.modified_at) else {
        return 0;
    };
    let age = now.signed_duration_since(reference);
    if age <= Duration::hours(24) {
        return 36_000;
    }
    if age <= Duration::days(7) {
        return 24_000;
    }
    if age <= Duration::days(30) {
        return 12_000;
    }
    if age <= Duration::days(180) {
        return 4_000;
    }
    0
}

fn search_text_for_entry(entry: &IndexedEntry) -> String {
    let mut parts = Vec::with_capacity(8);
    let normalized_name = normalize_for_search(&entry.file_name);
    if !normalized_name.is_empty() {
        parts.push(normalized_name);
    }

    let stem = Path::new(&entry.file_name)
        .file_stem()
        .map(|stem| stem.to_string_lossy().to_string())
        .unwrap_or_else(|| entry.file_name.clone());
    let normalized_stem = normalize_for_search(&stem);
    if !normalized_stem.is_empty() {
        parts.push(normalized_stem);
    }

    let normalized_path = normalize_for_search(&entry.relative_path.to_string_lossy());
    if !normalized_path.is_empty() {
        parts.push(normalized_path);
    }

    if let Some(extension) = &entry.extension {
        let normalized_extension = normalize_for_search(extension);
        if !normalized_extension.is_empty() {
            parts.push(normalized_extension);
        }
    }

    parts.push(content_type_tags(entry.content_type).to_string());
    parts.join(" ")
}

fn content_type_tags(content_type: EntryContentType) -> &'static str {
    match content_type {
        EntryContentType::Application => "application app launchable executable",
        EntryContentType::Archive => "archive compressed bundle",
        EntryContentType::Audio => "audio music sound",
        EntryContentType::Code => "code source",
        EntryContentType::Configuration => "config configuration settings",
        EntryContentType::Directory => "directory folder",
        EntryContentType::Document => "document notes text",
        EntryContentType::Image => "image photo picture",
        EntryContentType::Other => "file",
        EntryContentType::Package => "package bundle container",
        EntryContentType::Shortcut => "shortcut alias link launchable",
        EntryContentType::Symlink => "symlink alias link",
        EntryContentType::Video => "video movie media",
    }
}

fn normalize_for_search(input: &str) -> String {
    let mut normalized = String::new();
    let mut previous = CharClass::Boundary;

    for character in input.chars() {
        let class = classify_char(character);
        match class {
            CharClass::Boundary => {
                push_space(&mut normalized);
            }
            CharClass::Cjk => {
                push_space(&mut normalized);
                normalized.push(character);
                normalized.push(' ');
            }
            CharClass::Lower | CharClass::Upper | CharClass::Digit => {
                let boundary = matches!(
                    (previous, class),
                    (CharClass::Lower, CharClass::Upper)
                        | (CharClass::Digit, CharClass::Lower | CharClass::Upper)
                        | (CharClass::Lower | CharClass::Upper, CharClass::Digit)
                );
                if boundary {
                    push_space(&mut normalized);
                }
                for folded in character.to_lowercase() {
                    normalized.push(folded);
                }
            }
        }
        previous = class;
    }

    normalized.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn compact_for_search(input: &str) -> String {
    input.split_whitespace().collect::<String>()
}

#[derive(Clone, Copy)]
enum CharClass {
    Boundary,
    Lower,
    Upper,
    Digit,
    Cjk,
}

fn classify_char(character: char) -> CharClass {
    if is_cjk(character) {
        return CharClass::Cjk;
    }
    if character.is_ascii_digit() {
        return CharClass::Digit;
    }
    if character.is_uppercase() {
        return CharClass::Upper;
    }
    if character.is_lowercase() || character.is_alphanumeric() {
        return CharClass::Lower;
    }
    CharClass::Boundary
}

fn is_cjk(character: char) -> bool {
    matches!(
        character as u32,
        0x3040..=0x30ff
            | 0x3400..=0x4dbf
            | 0x4e00..=0x9fff
            | 0xac00..=0xd7af
            | 0xf900..=0xfaff
            | 0xff66..=0xff9d
    )
}

fn push_space(buffer: &mut String) {
    if !buffer.ends_with(' ') && !buffer.is_empty() {
        buffer.push(' ');
    }
}

fn fuzzy_match(query: &str, candidate: &str) -> Option<i64> {
    if query.is_empty() || candidate.is_empty() {
        return None;
    }

    if let Some(position) = candidate.find(query) {
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
    use super::{
        SearchBackend, SearchDocument, compact_for_search, normalize_for_search, score_document,
        search_text_for_entry,
    };
    use crate::indexer::{
        EntryContentType, IndexStorageConfig,
        scanner::ScannedDocument,
        storage::{IndexPaths, prepare_storage},
        types::{EntryKind, IndexedEntry},
    };
    use anyhow::Result;
    use chrono::{Duration, Utc};
    use std::{collections::HashMap, path::PathBuf};
    use tempfile::tempdir;

    fn entry(root: &std::path::Path, relative: &str) -> ScannedDocument {
        entry_with(
            root,
            relative,
            EntryContentType::Document,
            Utc::now() - Duration::days(14),
            None,
        )
    }

    fn entry_with(
        root: &std::path::Path,
        relative: &str,
        content_type: EntryContentType,
        modified_at: chrono::DateTime<Utc>,
        last_accessed_at: Option<chrono::DateTime<Utc>>,
    ) -> ScannedDocument {
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
                content_type,
                size_bytes: 42,
                modified_at: Some(modified_at),
                last_accessed_at,
                is_hidden: false,
            },
            content: None,
        }
    }

    fn score_for(query: &str, entry: IndexedEntry) -> i64 {
        let normalized = normalize_for_search(query);
        score_document(
            query,
            &normalized,
            &SearchDocument::new(entry),
            None,
            Utc::now(),
        )
        .expect("entry should match")
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
        assert_eq!(
            stored
                .get(&root.join("notes.md"))
                .map(|entry| entry.content_type),
            Some(EntryContentType::Document)
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_prefers_launchables_with_same_name() -> Result<()> {
        let temp = tempdir()?;
        let storage = IndexStorageConfig::new(temp.path().join("data"), temp.path().join("state"));
        let paths = IndexPaths::new(&storage);
        prepare_storage(&paths).await?;
        let root = temp.path().join("Applications");

        let mut documents = HashMap::new();
        documents.insert(
            root.join("Beam.app"),
            entry_with(
                &root,
                "Beam.app",
                EntryContentType::Application,
                Utc::now() - Duration::days(1),
                Some(Utc::now() - Duration::hours(2)),
            ),
        );
        documents.insert(
            root.join("Beam.md"),
            entry_with(
                &root,
                "Beam.md",
                EntryContentType::Document,
                Utc::now() - Duration::days(1),
                Some(Utc::now() - Duration::days(10)),
            ),
        );

        let backend = SearchBackend::open(paths).await?;
        backend.replace_all(documents).await?;

        let hits = backend.search("beam", 5).await?;
        assert_eq!(
            hits.first().map(|hit| hit.entry.content_type),
            Some(EntryContentType::Application)
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_matches_normalized_names() -> Result<()> {
        let temp = tempdir()?;
        let storage = IndexStorageConfig::new(temp.path().join("data"), temp.path().join("state"));
        let paths = IndexPaths::new(&storage);
        prepare_storage(&paths).await?;
        let root = temp.path().join("root");

        let mut documents = HashMap::new();
        documents.insert(
            root.join("docs/BeamLauncherGuide.md"),
            entry(&root, "docs/BeamLauncherGuide.md"),
        );

        let backend = SearchBackend::open(paths).await?;
        backend.replace_all(documents).await?;

        let spaced = backend.search("beam launcher", 5).await?;
        assert_eq!(
            spaced.first().map(|hit| hit.entry.file_name.as_str()),
            Some("BeamLauncherGuide.md")
        );

        let dashed = backend.search("launcher guide", 5).await?;
        assert_eq!(
            dashed.first().map(|hit| hit.entry.file_name.as_str()),
            Some("BeamLauncherGuide.md")
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recency_breaks_ties_for_identical_names() -> Result<()> {
        let temp = tempdir()?;
        let storage = IndexStorageConfig::new(temp.path().join("data"), temp.path().join("state"));
        let paths = IndexPaths::new(&storage);
        prepare_storage(&paths).await?;
        let root = temp.path().join("root");

        let mut documents = HashMap::new();
        documents.insert(
            root.join("recent/Beam.md"),
            entry_with(
                &root,
                "recent/Beam.md",
                EntryContentType::Document,
                Utc::now() - Duration::days(2),
                Some(Utc::now() - Duration::hours(1)),
            ),
        );
        documents.insert(
            root.join("archive/Beam.md"),
            entry_with(
                &root,
                "archive/Beam.md",
                EntryContentType::Document,
                Utc::now() - Duration::days(2),
                Some(Utc::now() - Duration::days(45)),
            ),
        );

        let backend = SearchBackend::open(paths).await?;
        backend.replace_all(documents).await?;

        let hits = backend.search("beam", 5).await?;
        assert_eq!(
            hits.first().map(|hit| hit.entry.absolute_path.clone()),
            Some(root.join("recent/Beam.md"))
        );
        Ok(())
    }

    #[test]
    fn normalized_search_text_includes_type_tags() {
        let root = PathBuf::from("/Applications");
        let entry = IndexedEntry {
            root: root.clone(),
            absolute_path: root.join("Beam.app"),
            relative_path: PathBuf::from("Beam.app"),
            file_name: "Beam.app".to_string(),
            extension: Some("app".to_string()),
            kind: EntryKind::Directory,
            content_type: EntryContentType::Application,
            size_bytes: 0,
            modified_at: None,
            last_accessed_at: None,
            is_hidden: false,
        };

        let search_text = search_text_for_entry(&entry);
        assert!(search_text.contains("beam"));
        assert!(search_text.contains("application"));
        assert!(search_text.contains("launchable"));
    }

    #[test]
    fn normalize_for_search_splits_camel_case_and_separators() {
        assert_eq!(
            normalize_for_search("BeamLauncher_Guide-v2"),
            "beam launcher guide v 2"
        );
        assert_eq!(compact_for_search("beam launcher"), "beamlauncher");
    }

    #[test]
    fn score_prefers_exact_stem_match() {
        let root = PathBuf::from("/tmp");
        let exact = IndexedEntry {
            root: root.clone(),
            absolute_path: root.join("Beam.md"),
            relative_path: PathBuf::from("Beam.md"),
            file_name: "Beam.md".to_string(),
            extension: Some("md".to_string()),
            kind: EntryKind::File,
            content_type: EntryContentType::Document,
            size_bytes: 0,
            modified_at: None,
            last_accessed_at: None,
            is_hidden: false,
        };
        let partial = IndexedEntry {
            root: root.clone(),
            absolute_path: root.join("BeamLauncher.md"),
            relative_path: PathBuf::from("BeamLauncher.md"),
            file_name: "BeamLauncher.md".to_string(),
            extension: Some("md".to_string()),
            kind: EntryKind::File,
            content_type: EntryContentType::Document,
            size_bytes: 0,
            modified_at: None,
            last_accessed_at: None,
            is_hidden: false,
        };

        assert!(score_for("beam", exact) > score_for("beam", partial));
    }
}
