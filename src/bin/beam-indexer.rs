use anyhow::{Result, anyhow};
use beam::indexer::{
    FileIndexHandle, IndexConfig, IndexRequest, IndexResponse, IndexStats, IndexStorageConfig,
    default_index_roots,
};
use clap::{Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::{fs, io, runtime::Handle, time::sleep};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser)]
#[command(author, version, about = "Beam's async cross-platform file indexer.")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Args, Clone, Debug, Default)]
struct StorageArgs {
    #[arg(long, value_name = "PATH")]
    data_dir: Option<PathBuf>,
    #[arg(long, value_name = "PATH")]
    state_dir: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Command {
    Scan {
        #[arg(long = "root", value_name = "PATH")]
        roots: Vec<PathBuf>,
        #[command(flatten)]
        storage: StorageArgs,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        json: bool,
    },
    Serve {
        #[arg(long = "root", value_name = "PATH")]
        roots: Vec<PathBuf>,
        #[command(flatten)]
        storage: StorageArgs,
    },
    Bench {
        #[arg(long = "root", value_name = "PATH")]
        roots: Vec<PathBuf>,
        #[command(flatten)]
        storage: StorageArgs,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    init_tracing();

    match Cli::parse().command {
        Command::Scan {
            roots,
            storage,
            query,
            limit,
            json,
        } => scan_command(roots, storage, query, limit, json).await,
        Command::Serve { roots, storage } => serve_command(roots, storage).await,
        Command::Bench {
            roots,
            storage,
            query,
            limit,
            json,
        } => bench_command(roots, storage, query, limit, json).await,
    }
}

async fn scan_command(
    roots: Vec<PathBuf>,
    storage: StorageArgs,
    query: Option<String>,
    limit: usize,
    json: bool,
) -> Result<()> {
    let mut config = build_config(roots, storage)?;
    config.watch = false;
    config.refresh_on_start = false;
    let handle = FileIndexHandle::spawn(config, Handle::current())?;
    let snapshot = handle.refresh().await?;

    if let Some(query) = query {
        let hits = handle.search(query, limit).await?;
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&IndexResponse::SearchResults { hits })?
            );
        } else {
            for hit in hits {
                println!("{:>6} {}", hit.score, hit.entry.absolute_path.display());
            }
        }
    } else {
        let dump = handle.dump(Some(limit)).await?;
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&IndexResponse::Dump { dump })?
            );
        } else {
            println!(
                "Indexed {} files, {} directories, {} symlinks across {} roots in {}.",
                snapshot.stats.indexed_files,
                snapshot.stats.indexed_directories,
                snapshot.stats.indexed_symlinks,
                snapshot.stats.roots.len(),
                snapshot.storage.data_dir.display()
            );
            for entry in dump.entries {
                println!("{}", entry.absolute_path.display());
            }
        }
    }

    handle.shutdown().await?;
    Ok(())
}

async fn serve_command(roots: Vec<PathBuf>, storage: StorageArgs) -> Result<()> {
    let config = build_config(roots, storage)?;
    let handle = FileIndexHandle::spawn(config, Handle::current())?;

    let mut stdin = FramedRead::new(io::stdin(), LinesCodec::new());
    let mut stdout = FramedWrite::new(io::stdout(), LinesCodec::new());
    stdout
        .send(serde_json::to_string(&IndexResponse::Ready)?)
        .await?;

    while let Some(line) = stdin.next().await {
        let line = line?;
        let request = match serde_json::from_str::<IndexRequest>(&line) {
            Ok(request) => request,
            Err(error) => {
                stdout
                    .send(serde_json::to_string(&IndexResponse::Error {
                        message: error.to_string(),
                    })?)
                    .await?;
                continue;
            }
        };

        let response = match request {
            IndexRequest::Snapshot => IndexResponse::Snapshot {
                snapshot: handle.snapshot().await?,
            },
            IndexRequest::Dump { limit } => IndexResponse::Dump {
                dump: handle.dump(limit).await?,
            },
            IndexRequest::Search { query, limit } => IndexResponse::SearchResults {
                hits: handle.search(query, limit).await?,
            },
            IndexRequest::Refresh => {
                handle.refresh().await?;
                IndexResponse::Ack
            }
            IndexRequest::Shutdown => {
                handle.shutdown().await?;
                stdout
                    .send(serde_json::to_string(&IndexResponse::Ack)?)
                    .await?;
                break;
            }
        };

        stdout.send(serde_json::to_string(&response)?).await?;
    }

    Ok(())
}

async fn bench_command(
    roots: Vec<PathBuf>,
    storage: StorageArgs,
    query: Option<String>,
    limit: usize,
    json: bool,
) -> Result<()> {
    let mut config = build_config(roots, storage)?;
    config.watch = false;
    config.refresh_on_start = false;

    let refresh_start = Instant::now();
    let handle = FileIndexHandle::spawn(config.clone(), Handle::current())?;
    let refreshed = handle.refresh().await?;
    let refresh_duration_ms = refresh_start.elapsed().as_secs_f64() * 1_000.0;

    let search = if let Some(query) = query {
        let started = Instant::now();
        let hits = handle.search(query.clone(), limit).await?;
        Some(BenchmarkSearch {
            query,
            duration_ms: started.elapsed().as_secs_f64() * 1_000.0,
            hits: hits.len(),
        })
    } else {
        None
    };

    handle.shutdown().await?;

    let cold_start_begin = Instant::now();
    let (reopened, cold_snapshot) = reopen_for_benchmark(config).await?;
    let cold_start_ms = cold_start_begin.elapsed().as_secs_f64() * 1_000.0;
    reopened.shutdown().await?;

    let benchmark = BenchmarkResult {
        beam: BenchmarkMetrics {
            full_refresh_ms: refresh_duration_ms,
            cold_start_ms,
            throughput: ThroughputMetrics::new(&refreshed.stats, refresh_duration_ms),
            stats: refreshed.stats,
            storage: refreshed.storage,
        },
        search,
        raycast: read_raycast_baseline().await?,
        reopened_stats: cold_snapshot.stats,
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&benchmark)?);
    } else {
        println!(
            "Beam full refresh: {:.2} ms, cold start: {:.2} ms, entries: {}, {:.6} ms/entry, {:.0} entries/s",
            benchmark.beam.full_refresh_ms,
            benchmark.beam.cold_start_ms,
            benchmark.beam.throughput.entries,
            benchmark.beam.throughput.ms_per_entry,
            benchmark.beam.throughput.entries_per_second,
        );
        if let Some(search) = &benchmark.search {
            println!(
                "Search \"{}\": {:.2} ms ({} hits)",
                search.query, search.duration_ms, search.hits
            );
        }
        if let Some(raycast) = &benchmark.raycast {
            println!(
                "Raycast recorded full index duration: {:.2} ms for {} entries, {:.6} ms/entry, {:.0} entries/s",
                raycast.full_refresh_ms,
                raycast.throughput.entries,
                raycast.throughput.ms_per_entry,
                raycast.throughput.entries_per_second,
            );
            println!(
                "Beam vs Raycast throughput: {:.2}x entries/s, {:.2}x ms/entry",
                benchmark.beam.throughput.entries_per_second
                    / raycast.throughput.entries_per_second,
                benchmark.beam.throughput.ms_per_entry / raycast.throughput.ms_per_entry,
            );
        }
    }

    Ok(())
}

fn resolve_roots(roots: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    let resolved = if roots.is_empty() {
        default_index_roots()
    } else {
        roots
    };

    if resolved.is_empty() {
        return Err(anyhow!(
            "no index roots were provided and no home directory was found"
        ));
    }

    Ok(resolved)
}

fn build_config(roots: Vec<PathBuf>, storage: StorageArgs) -> Result<IndexConfig> {
    let mut config = IndexConfig::with_roots(resolve_roots(roots)?);
    if let Some(storage) = resolve_storage(storage)? {
        config = config.with_storage(storage);
    }
    Ok(config)
}

fn resolve_storage(storage: StorageArgs) -> Result<Option<IndexStorageConfig>> {
    match (storage.data_dir, storage.state_dir) {
        (Some(data_dir), Some(state_dir)) => Ok(Some(IndexStorageConfig::new(data_dir, state_dir))),
        (None, None) => Ok(None),
        _ => Err(anyhow!(
            "both --data-dir and --state-dir must be provided together"
        )),
    }
}

#[derive(Debug, Serialize)]
struct BenchmarkResult {
    beam: BenchmarkMetrics,
    search: Option<BenchmarkSearch>,
    raycast: Option<RaycastBaseline>,
    reopened_stats: IndexStats,
}

#[derive(Debug, Serialize)]
struct BenchmarkMetrics {
    full_refresh_ms: f64,
    cold_start_ms: f64,
    throughput: ThroughputMetrics,
    stats: IndexStats,
    storage: IndexStorageConfig,
}

#[derive(Debug, Serialize)]
struct ThroughputMetrics {
    entries: u64,
    ms_per_entry: f64,
    entries_per_second: f64,
}

impl ThroughputMetrics {
    fn new(stats: &IndexStats, duration_ms: f64) -> Self {
        let entries = total_entries(stats);
        let entries_f64 = entries as f64;
        let ms_per_entry = if entries == 0 {
            0.0
        } else {
            duration_ms / entries_f64
        };
        let entries_per_second = if duration_ms == 0.0 {
            0.0
        } else {
            entries_f64 / (duration_ms / 1_000.0)
        };

        Self {
            entries,
            ms_per_entry,
            entries_per_second,
        }
    }
}

#[derive(Debug, Serialize)]
struct BenchmarkSearch {
    query: String,
    duration_ms: f64,
    hits: usize,
}

#[derive(Debug, Serialize)]
struct RaycastBaseline {
    full_refresh_ms: f64,
    entries: u64,
    files: u64,
    directories: u64,
    symlinks: u64,
    throughput: ThroughputMetrics,
}

async fn read_raycast_baseline() -> Result<Option<RaycastBaseline>> {
    let path = PathBuf::from(
        "~/Library/Application Support/com.raycast-x.macos/index/stats.json"
            .replace('~', &std::env::var("HOME").unwrap_or_default()),
    );
    let contents = match fs::read_to_string(&path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let stats: serde_json::Value = serde_json::from_str(&contents)?;
    let files = stats["num_files"].as_u64().unwrap_or_default();
    let directories = stats["num_directories"].as_u64().unwrap_or_default();
    let symlinks = stats["num_symlinks"].as_u64().unwrap_or_default();
    let entries = stats["num_entries"]
        .as_u64()
        .unwrap_or_else(|| files + directories + symlinks);
    let full_refresh_ms = stats["duration"].as_f64().unwrap_or_default() * 1_000.0;
    Ok(Some(RaycastBaseline {
        full_refresh_ms,
        entries,
        files,
        directories,
        symlinks,
        throughput: ThroughputMetrics {
            entries,
            ms_per_entry: if entries == 0 {
                0.0
            } else {
                full_refresh_ms / entries as f64
            },
            entries_per_second: if full_refresh_ms == 0.0 {
                0.0
            } else {
                entries as f64 / (full_refresh_ms / 1_000.0)
            },
        },
    }))
}

async fn reopen_for_benchmark(
    config: IndexConfig,
) -> Result<(FileIndexHandle, beam::indexer::IndexSnapshot)> {
    const RETRIES: usize = 8;
    const RETRY_DELAY: Duration = Duration::from_millis(50);

    let mut last_error = None;
    for attempt in 0..RETRIES {
        let handle = FileIndexHandle::spawn(config.clone(), Handle::current())?;
        match handle.snapshot().await {
            Ok(snapshot) => return Ok((handle, snapshot)),
            Err(error) if error.to_string().contains("indexer actor dropped") => {
                last_error = Some(error);
                if attempt + 1 < RETRIES {
                    sleep(RETRY_DELAY).await;
                    continue;
                }
            }
            Err(error) => return Err(error),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("failed to reopen benchmark indexer")))
}

fn total_entries(stats: &IndexStats) -> u64 {
    stats.indexed_files + stats.indexed_directories + stats.indexed_symlinks
}

fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("beam=info")),
        )
        .with_target(false)
        .try_init();
}
