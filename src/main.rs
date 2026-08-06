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
mod device_store;
mod message_projection;
mod models;
mod push;
mod secret_store;
mod session_store;
mod session_trace;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

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
    /// Start the HTTP bridge server (default)
    Serve {
        #[arg(long, default_value = "8787")]
        port: u16,
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
        Some(Command::Serve { port }) => serve(port).await,
        None => serve(8787).await,
    }
}

async fn serve(port: u16) -> Result<()> {
    let state = Arc::new(AppState::new_strict().await?);
    let app = router(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;

    println!("Omni Code bridge listening on http://{addr}");

    axum::serve(listener, app).await?;
    Ok(())
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
    Router::new().merge(api::router()).with_state(state).layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    )
}
