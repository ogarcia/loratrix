//! meshtrix — a minimal relay between LoRa mesh networks and Matrix rooms.

#[cfg(not(any(feature = "meshtastic", feature = "meshcore")))]
compile_error!(
    "enable at least one mesh backend: the `meshtastic` feature, the `meshcore` feature, or both"
);

mod config;
mod dedupe;
mod matrix;
mod mesh;
mod prefix;
mod router;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::{Config, NetworkKind};
use crate::matrix::{MatrixSink, http::HttpSink};
use crate::mesh::MeshBackend;
use crate::router::Router;

const DEFAULT_CONFIG_PATH: &str = "~/.config/meshtrix/config.toml";
/// Environment variable naming the config file. This is how the container image
/// points at a mounted path without every invocation having to repeat
/// `--config`, which an overridden CMD would otherwise drop.
const CONFIG_ENV: &str = "MESHTRIX_CONFIG";
/// Bounded so a burst on one side cannot grow memory without limit.
const CHANNEL_CAPACITY: usize = 64;

const USAGE: &str = "\
meshtrix — relay between Meshtastic/MeshCore meshes and Matrix rooms

Usage:
  meshtrix [--config PATH]
  meshtrix --check [--config PATH]
  meshtrix --help | --version

Options:
  -c, --config PATH  Config file (default: $MESHTRIX_CONFIG, or
                     ~/.config/meshtrix/config.toml)
      --check        Validate the config, print the routing plan and exit
  -h, --help         Show this help
  -V, --version      Show the version
";

struct Args {
    config: PathBuf,
    check: bool,
}

/// Hand-rolled because three flags do not justify an argument parser.
fn parse_args() -> Result<Option<Args>> {
    let mut config = std::env::var_os(CONFIG_ENV)
        .filter(|value| !value.is_empty())
        .map_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH), PathBuf::from);
    let mut check = false;
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                return Ok(None);
            }
            "-V" | "--version" => {
                println!("meshtrix {}", env!("CARGO_PKG_VERSION"));
                return Ok(None);
            }
            "--check" => check = true,
            "-c" | "--config" => {
                config = args
                    .next()
                    .map(PathBuf::from)
                    .context("--config needs a path")?;
            }
            other => bail!("unexpected argument {other:?}\n\n{USAGE}"),
        }
    }

    Ok(Some(Args { config, check }))
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = match parse_args() {
        Ok(Some(args)) => args,
        Ok(None) => return ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };

    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error!("{error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<()> {
    let config = Config::load(&args.config)?;
    let backends = build_backends(&config)?;
    let matrix: Arc<dyn MatrixSink> = Arc::new(HttpSink::new(&config.matrix)?);
    let router = Arc::new(Router::build(&config, backends.clone(), matrix.clone())?);

    if args.check {
        println!("{}", router.plan());
        return Ok(());
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the async runtime")?
        .block_on(relay(router, backends, matrix))
}

fn build_backends(config: &Config) -> Result<Vec<Arc<dyn MeshBackend>>> {
    let mut backends: Vec<Arc<dyn MeshBackend>> = Vec::with_capacity(config.networks.len());

    for network in &config.networks {
        let backend: Arc<dyn MeshBackend> = match network.kind {
            #[cfg(feature = "meshtastic")]
            NetworkKind::Meshtastic => Arc::new(mesh::meshtastic::MeshtasticBackend::new(network)?),
            #[cfg(feature = "meshcore")]
            NetworkKind::Meshcore => Arc::new(mesh::meshcore::MeshCoreBackend::new(network)?),
            #[allow(unreachable_patterns)]
            kind => bail!(
                "network {:?} is {kind:?}, but this build has no backend for it; \
                 rebuild with the matching cargo feature enabled",
                network.name
            ),
        };
        backends.push(backend);
    }

    Ok(backends)
}

async fn relay(
    router: Arc<Router>,
    backends: Vec<Arc<dyn MeshBackend>>,
    matrix: Arc<dyn MatrixSink>,
) -> Result<()> {
    matrix.join_rooms(&router.rooms()).await?;

    let (mesh_tx, mut mesh_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (matrix_tx, mut matrix_rx) = mpsc::channel(CHANNEL_CAPACITY);

    let mut runners = JoinSet::new();
    for backend in backends {
        let tx = mesh_tx.clone();
        runners.spawn(async move {
            let id = backend.id().to_owned();
            backend
                .run(tx)
                .await
                .with_context(|| format!("mesh network {id}"))
        });
    }
    runners.spawn(async move { matrix.run(matrix_tx).await.context("matrix") });
    // Both senders are now owned by the tasks; drop ours so the receivers close
    // when the last runner stops.
    drop(mesh_tx);

    info!("relay started");

    loop {
        tokio::select! {
            Some(message) = mesh_rx.recv() => {
                router.handle_mesh(message, Instant::now()).await;
            }
            Some(message) = matrix_rx.recv() => {
                router.handle_matrix(message, Instant::now()).await;
            }
            Some(finished) = runners.join_next() => {
                // A runner is expected to retry internally, so its return means
                // the link is unrecoverable and the relay cannot do its job.
                match finished {
                    Ok(Ok(())) => warn!("a connection task stopped"),
                    Ok(Err(error)) => return Err(error),
                    Err(error) => return Err(error).context("a connection task panicked"),
                }
            }
            result = tokio::signal::ctrl_c() => {
                result.context("waiting for ctrl-c")?;
                info!("shutting down");
                return Ok(());
            }
            else => {
                warn!("every connection closed, shutting down");
                return Ok(());
            }
        }
    }
}
