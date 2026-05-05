mod adapter;
mod ai_approval;
mod api;
mod app_state;
mod approval_policy;
mod asr;
mod bridge_settings;
mod claude_hook;
mod claude_store;
mod device_store;
mod models;
mod push;
mod session_store;
mod tts;

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use app_state::AppState;
use axum::Router;
use tower_http::cors::{Any, CorsLayer};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error:?}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    load_bridge_env();

    if let Some(args) = ClaudePermissionHookArgs::parse(std::env::args().skip(1))? {
        return claude_hook::run_permission_hook(
            args.state_dir,
            args.session_id,
            args.run_id,
            args.project_root,
        )
        .await;
    }

    let state = Arc::new(AppState::new().await);
    let app = router(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8787));
    let listener = tokio::net::TcpListener::bind(addr).await?;

    println!("Omni Code desktop bridge listening on http://{addr}");

    axum::serve(listener, app).await?;
    Ok(())
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

struct ClaudePermissionHookArgs {
    state_dir: std::path::PathBuf,
    session_id: String,
    run_id: String,
    project_root: std::path::PathBuf,
}

impl ClaudePermissionHookArgs {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Option<Self>> {
        let Some(command) = args.next() else {
            return Ok(None);
        };
        if command != "claude-permission-hook" {
            return Ok(None);
        }

        let mut state_dir = None;
        let mut session_id = None;
        let mut run_id = None;
        let mut project_root = None;

        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--state-dir" => state_dir = args.next().map(Into::into),
                "--session-id" => session_id = args.next(),
                "--run-id" => run_id = args.next(),
                "--project-root" => project_root = args.next().map(Into::into),
                other => anyhow::bail!("unknown claude hook arg: {other}"),
            }
        }

        Ok(Some(Self {
            state_dir: state_dir.context("missing --state-dir")?,
            session_id: session_id.context("missing --session-id")?,
            run_id: run_id.context("missing --run-id")?,
            project_root: project_root.context("missing --project-root")?,
        }))
    }
}
