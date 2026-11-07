#![cfg(feature = "cli")]
use std::{
    fs,
    path::Path,
    process::{Command, Output},
    time::{Duration, Instant},
};
fn cli(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_omni-worktree"))
        .arg("--repo")
        .arg(root)
        .args(args)
        .output()
        .unwrap()
}
fn git(root: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
#[test]
fn cli_create_watch_detach_merge_and_skill() {
    let dir = std::env::temp_dir().join(format!("omni-cli-{}", uuid::Uuid::new_v4()));
    let root = dir.join("repo");
    fs::create_dir_all(&root).unwrap();
    git(&root, &["init", "-b", "main"]);
    git(&root, &["config", "user.email", "test@example.test"]);
    git(&root, &["config", "user.name", "test"]);
    fs::write(root.join(".gitignore"), "node_modules/\n").unwrap();
    fs::write(root.join("package.json"), "{}").unwrap();
    git(&root, &["add", "."]);
    git(&root, &["commit", "-m", "initial"]);
    fs::create_dir_all(root.join("node_modules/demo")).unwrap();
    fs::write(root.join("node_modules/demo/data"), "installed").unwrap();
    let out = cli(&root, &["create", "feature"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let path = Path::new(value["path"].as_str().unwrap());
    assert!(path.join("node_modules/demo/data").exists());
    let mut watcher = Command::new(env!("CARGO_BIN_EXE_omni-worktree"))
        .arg("--repo")
        .arg(&root)
        .args(["watch", "--interval", "1"])
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    fs::write(path.join("package.json"), "{\"version\":\"2\"}").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while path.join("node_modules").exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    watcher.kill().unwrap();
    watcher.wait().unwrap();
    assert!(!path.join("node_modules").exists());
    git(path, &["add", "."]);
    git(path, &["commit", "-m", "changed"]);
    let out = cli(&root, &["merge", "feature"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!path.exists());
    let out = cli(&root, &["list"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&out.stdout)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(!cli(&root, &["remove", "nonexistent"]).status.success());
    let out = cli(Path::new("/nonexistent"), &["skill"]);
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).starts_with("---\nname: omni-worktree"));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn shared_parser_embeds_as_a_host_subcommand() {
    use clap::{Parser, Subcommand};
    #[derive(Parser)]
    struct Host {
        #[command(subcommand)]
        command: HostCommand,
    }
    #[derive(Subcommand)]
    enum HostCommand {
        Worktree(omni_worktree::cli::WorktreeArgs),
    }
    let host = Host::try_parse_from([
        "omni-code-bridge",
        "worktree",
        "--repo",
        "/repo",
        "create",
        "feature",
        "--keep",
    ])
    .unwrap();
    let HostCommand::Worktree(args) = host.command;
    assert_eq!(args.repo, Path::new("/repo"));
    assert!(matches!(
        args.command,
        omni_worktree::cli::WorktreeCommand::Create { keep: true, .. }
    ));
}
