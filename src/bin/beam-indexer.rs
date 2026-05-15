use anyhow::{Result, anyhow};
use beam::indexer::{
    FileIndexHandle, IndexConfig, IndexRequest, IndexResponse, IndexStats, IndexStorageConfig,
    default_index_roots,
};
use clap::{Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use std::{path::PathBuf, time::Instant};
use tokio::{fs, io, runtime::Handle};
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
    let reopened = FileIndexHandle::spawn(config, Handle::current())?;
    let cold_snapshot = reopened.snapshot().await?;
    let cold_start_ms = cold_start_begin.elapsed().as_secs_f64() * 1_000.0;
    reopened.shutdown().await?;

    let benchmark = BenchmarkResult {
        beam: BenchmarkMetrics {
            full_refresh_ms: refresh_duration_ms,
            cold_start_ms,
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
            "Beam full refresh: {:.2} ms, cold start: {:.2} ms, entries: {}",
            benchmark.beam.full_refresh_ms,
            benchmark.beam.cold_start_ms,
            benchmark.beam.stats.indexed_files
                + benchmark.beam.stats.indexed_directories
                + benchmark.beam.stats.indexed_symlinks
        );
        if let Some(search) = &benchmark.search {
            println!(
                "Search \"{}\": {:.2} ms ({} hits)",
                search.query, search.duration_ms, search.hits
            );
        }
        if let Some(raycast) = &benchmark.raycast {
            println!(
                "Raycast recorded full index duration: {:.2} ms for {} entries",
                raycast.full_refresh_ms, raycast.entries
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
    stats: IndexStats,
    storage: IndexStorageConfig,
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
    Ok(Some(RaycastBaseline {
        full_refresh_ms: stats["duration"].as_f64().unwrap_or_default() * 1_000.0,
        entries: stats["num_entries"].as_u64().unwrap_or_default(),
        files: stats["num_files"].as_u64().unwrap_or_default(),
        directories: stats["num_directories"].as_u64().unwrap_or_default(),
        symlinks: stats["num_symlinks"].as_u64().unwrap_or_default(),
    }))
}

fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("beam=info")),
        )
        .with_target(false)
        .try_init();
}
