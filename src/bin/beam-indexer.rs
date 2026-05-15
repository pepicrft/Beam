use anyhow::{Result, anyhow};
use beam::indexer::{
    FileIndexHandle, IndexConfig, IndexRequest, IndexResponse, default_index_roots,
};
use clap::{Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use tokio::{io, runtime::Handle};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser)]
#[command(author, version, about = "Beam's async cross-platform file indexer.")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Scan {
        #[arg(long = "root", value_name = "PATH")]
        roots: Vec<std::path::PathBuf>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        json: bool,
    },
    Serve {
        #[arg(long = "root", value_name = "PATH")]
        roots: Vec<std::path::PathBuf>,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    init_tracing();

    match Cli::parse().command {
        Command::Scan {
            roots,
            query,
            limit,
            json,
        } => scan_command(roots, query, limit, json).await,
        Command::Serve { roots } => serve_command(roots).await,
    }
}

async fn scan_command(
    roots: Vec<std::path::PathBuf>,
    query: Option<String>,
    limit: usize,
    json: bool,
) -> Result<()> {
    let config = IndexConfig::with_roots(resolve_roots(roots)?);
    let handle = FileIndexHandle::spawn(config, Handle::current());

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
                "Indexed {} files, {} directories, {} symlinks across {} roots.",
                dump.stats.indexed_files,
                dump.stats.indexed_directories,
                dump.stats.indexed_symlinks,
                dump.stats.roots.len()
            );
            for entry in dump.entries {
                println!("{}", entry.absolute_path.display());
            }
        }
    }

    handle.shutdown().await?;
    Ok(())
}

async fn serve_command(roots: Vec<std::path::PathBuf>) -> Result<()> {
    let config = IndexConfig::with_roots(resolve_roots(roots)?);
    let handle = FileIndexHandle::spawn(config, Handle::current());

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

fn resolve_roots(roots: Vec<std::path::PathBuf>) -> Result<Vec<std::path::PathBuf>> {
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

fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("beam=info")),
        )
        .with_target(false)
        .try_init();
}
