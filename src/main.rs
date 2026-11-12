mod acceptance;
mod acceptance_api;
mod acceptance_approval;
mod acceptance_context;
#[cfg(test)]
mod acceptance_test_support;
mod acp_client;
mod adapter;
mod ai_approval;
mod api;
mod app_state;
mod approval_policy;
mod bridge_settings;
mod claude_hook;
mod claude_store;
mod client_auth_store;
mod desktop_gateway;
mod device_store;
mod environment_probe;
mod execution_health;
mod logging;
mod mcp;
mod mcp_api;
mod message_projection;
mod metadata_persistence;
mod models;
mod pi_plugin_store;
mod project_detection;
mod project_environment;
mod push;
mod relay;
mod runtime_plugin;
mod secret_store;
mod session_domain;
mod session_domain_store;
mod session_store;
mod session_trace;

use std::{future::IntoFuture, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use app_state::AppState;
use axum::Router;
use clap::{Parser, Subcommand};
use tower_http::cors::{Any, CorsLayer};

#[derive(Parser)]
#[command(name = "omni-code-bridge", about = "Omni Code bridge CLI and server")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Configure the embedded Relay before starting the server
    Relay {
        #[arg(trailing_var_arg = true)]
        arguments: Vec<String>,
    },
    /// Manage Git worktrees, dependency sharing, and merge cleanup
    Worktree(omni_worktree::cli::WorktreeArgs),
    /// Start the HTTP bridge server (default)
    Serve {
        #[arg(long, default_value = "8787")]
        port: u16,
        /// Enable verbose diagnostic logging
        #[arg(long)]
        debug: bool,
        /// Pi provider HTTP request timeout in seconds; 0 disables the timeout
        #[arg(
            long = "pi-request-timeout",
            env = "PI_HTTP_REQUEST_TIMEOUT_SECS",
            default_value_t = 300
        )]
        pi_request_timeout: u64,
    },
    /// Validate a bridge settings file without starting the server
    SettingsValidate {
        /// Settings file path. Defaults to the resolved bridge settings path.
        #[arg(long)]
        path: Option<PathBuf>,
    },
    /// Claude Code permission hook (invoked by Claude Code, not by users)
    ClaudePermissionHook {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        project_root: PathBuf,
    },
    /// Manage client authorization
    #[command(subcommand)]
    ClientAuth(ClientAuthCommand),
    /// Show recent agent command/response trace for a session
    SessionTrace {
        /// Session id, exact title, or partial title
        #[arg(long)]
        session: String,
        /// Number of recent command/response pairs to show
        #[arg(long, default_value_t = 5)]
        limit: usize,
    },
}

#[derive(Subcommand)]
enum ClientAuthCommand {
    /// List all client auth requests
    List {
        /// Show only pending requests
        #[arg(long)]
        pending: bool,
    },
    /// Approve pending client auth requests and generate tokens
    Approve {
        /// The request ID to approve (omit to approve all pending)
        #[arg(long)]
        request_id: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error:?}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    load_bridge_env();

    let cli = Cli::parse();

    match cli.command {
        Some(Command::Relay { arguments }) => {
            omni_relay_connector::host_cli(relay::host(8787)?.as_ref(), &arguments).await
        }
        Some(Command::Worktree(args)) => omni_worktree::cli::run(args),
        Some(Command::ClaudePermissionHook {
            state_dir,
            session_id,
            run_id,
            project_root,
        }) => claude_hook::run_permission_hook(state_dir, session_id, run_id, project_root).await,
        Some(Command::SettingsValidate { path }) => validate_settings_cli(path).await,
        Some(Command::ClientAuth(sub)) => handle_client_auth(sub).await,
        Some(Command::SessionTrace { session, limit }) => {
            session_trace::print_session_trace(&session, limit)
        }
        Some(Command::Serve {
            port,
            debug,
            pi_request_timeout,
        }) => serve(port, debug, pi_request_timeout).await,
        None => serve(8787, false, pi_request_timeout_from_env()).await,
    }
}

fn pi_request_timeout_from_env() -> u64 {
    std::env::var("PI_HTTP_REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(300)
}

async fn serve(port: u16, debug: bool, pi_request_timeout: u64) -> Result<()> {
    pi::http::client::set_request_timeout_override(pi_request_timeout);
    logging::init(debug);
    let relay = relay::host(port)?;
    // Reserve the port before startup recovery can modify persisted turns.
    // A duplicate server must fail without interrupting the running server.
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let mut state = AppState::new_strict().await?;
    state.relay = Some(relay.clone());
    let state = Arc::new(state);
    state.start_metadata_writer();
    state.acceptance.start_maintenance();
    state
        .acceptance
        .resume_automations(Arc::clone(&state))
        .await;
    let app = router(Arc::clone(&state));

    relay.resume().await?;

    println!("Omni Code bridge listening on http://{addr}");

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_received = Arc::clone(&shutdown);
    let stopping_relay = relay.clone();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            stopping_relay.shutdown().await;
            shutdown_received.notify_one();
        })
        .into_future();
    tokio::pin!(server);
    let result = tokio::select! {
        result = &mut server => result,
        _ = shutdown.notified() => {
            if let Err(error) = state.flush_pending_session_metadata().await {
                eprintln!("failed to flush session metadata during shutdown: {error}");
            }
            // SSE streams can remain open indefinitely; allow requests to
            // drain briefly before completing the final metadata flush.
            tokio::time::timeout(std::time::Duration::from_secs(5), &mut server)
                .await
                .unwrap_or(Ok(()))
        }
    };
    relay.shutdown().await;
    state
        .flush_pending_session_metadata()
        .await
        .map_err(anyhow::Error::msg)?;
    result?;
    Ok(())
}

async fn shutdown_signal() {
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            eprintln!("failed to listen for interrupt signal: {error}");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                eprintln!("failed to listen for termination signal: {error}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = interrupt => {},
        _ = terminate => {},
    }
}

async fn validate_settings_cli(path: Option<PathBuf>) -> Result<()> {
    let path = path.unwrap_or_else(bridge_settings::settings_path);
    let settings = bridge_settings::load_settings_from_path(&path).await?;
    bridge_settings::validate_bridge_settings(&settings).map_err(anyhow::Error::msg)?;

    println!("Bridge settings are valid: {}", path.display());
    println!("  model_providers: {}", settings.model_providers.len());
    println!("  acp_servers: {}", settings.acp_servers.len());
    Ok(())
}

async fn handle_client_auth(cmd: ClientAuthCommand) -> Result<()> {
    match cmd {
        ClientAuthCommand::List { pending } => {
            let store = client_auth_store::ClientAuthStore::load().await;
            let records = store.list().await;

            if records.is_empty() {
                println!("No client auth requests found.");
                return Ok(());
            }

            for record in &records {
                if pending && record.status != models::ClientAuthStatus::Pending {
                    continue;
                }
                let status = match record.status {
                    models::ClientAuthStatus::Pending => "pending",
                    models::ClientAuthStatus::Approved => "approved",
                };
                let token_display = record.token.as_deref().unwrap_or("(none)");
                let device = record.device_name.as_deref().unwrap_or("(unknown)");
                println!(
                    "  request_id: {}\n  client_id:  {}\n  device:     {}\n  status:     {}\n  token:      {}\n  created_at: {}\n  updated_at: {}\n",
                    record.request_id,
                    record.client_id,
                    device,
                    status,
                    token_display,
                    record.created_at.to_rfc3339(),
                    record.updated_at.to_rfc3339(),
                );
            }
            Ok(())
        }
        ClientAuthCommand::Approve { request_id } => {
            let store = client_auth_store::ClientAuthStore::load().await;
            match request_id {
                Some(id) => {
                    let record = store.approve(&id).await?;
                    println!("Approved.");
                    println!("  request_id: {}", record.request_id);
                    println!("  client_id:  {}", record.client_id);
                    println!(
                        "  token:      {}",
                        record.token.as_deref().unwrap_or("(none)")
                    );
                }
                None => {
                    let records = store.approve_all_pending().await?;
                    if records.is_empty() {
                        println!("No pending requests to approve.");
                    } else {
                        println!("Approved {} request(s):", records.len());
                        for record in &records {
                            println!(
                                "  request_id: {}  client_id: {}  token: {}",
                                record.request_id,
                                record.client_id,
                                record.token.as_deref().unwrap_or("(none)"),
                            );
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

fn load_bridge_env() {
    let manifest_env = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".env");
    if manifest_env.is_file() {
        let _ = dotenvy::from_path_override(&manifest_env);
        return;
    }

    let _ = dotenvy::dotenv_override();
}

fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .merge(api::router())
        .merge(relay::router())
        .layer(axum::middleware::from_fn(relay::guard))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
}
