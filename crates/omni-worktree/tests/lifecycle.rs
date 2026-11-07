use omni_worktree::{CreateOptions, Manager};
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    process::Command,
};
struct Repo {
    dir: PathBuf,
    root: PathBuf,
}
impl Repo {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("omni-worktree-test-{}", uuid::Uuid::new_v4()));
        let root = dir.join("repo with spaces");
        fs::create_dir_all(&root).unwrap();
        git(&root, &["init", "-b", "main"]);
        git(&root, &["config", "user.email", "worktree@example.test"]);
        git(&root, &["config", "user.name", "Worktree Test"]);
        write(
            &root,
            ".gitignore",
            "node_modules/\nvendor/\n.dart_tool/\ntarget/\n.env\n",
        );
        write(&root, "README", "initial\n");
        commit(&root, "initial");
        Self { dir, root }
    }
    fn manager(&self) -> Manager {
        Manager::open(&self.root).unwrap()
    }
    fn node(&self) {
        write(
            &self.root,
            "package.json",
            r#"{"dependencies":{"demo":"1"}}"#,
        );
        write(&self.root, "package-lock.json", "lock v1");
        commit(&self.root, "dependencies");
        write(
            &self.root,
            "node_modules/demo/index.js",
            "module.exports = 42;",
        );
    }
}
impl Drop for Repo {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}
fn git(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}
fn write(root: &Path, file: &str, value: &str) {
    let path = root.join(file);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, value).unwrap();
}
fn commit(root: &Path, message: &str) {
    git(root, &["add", "."]);
    git(root, &["commit", "-m", message]);
}
#[test]
fn shares_then_detaches_on_manifest_edit_and_preserves_source() {
    let repo = Repo::new();
    repo.node();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature/test"))
        .unwrap();
    assert!(w.last_error.is_none(), "{:?}", w.last_error);
    assert!(
        fs::symlink_metadata(w.path.join("node_modules/demo"))
            .unwrap()
            .is_symlink()
    );
    write(&w.path, "package.json", r#"{"dependencies":{"demo":"2"}}"#);
    let report = Manager::open(&w.path)
        .unwrap()
        .sync(false, &HashSet::new())
        .unwrap();
    assert_eq!(report.detached, vec![w.id]);
    assert!(!w.path.join("node_modules").exists());
    assert!(repo.root.join("node_modules/demo/index.js").exists());
}
#[test]
fn source_lock_change_detaches_even_after_restart() {
    let repo = Repo::new();
    repo.node();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    write(&repo.root, "package-lock.json", "lock v2");
    assert_eq!(
        repo.manager()
            .sync(false, &HashSet::new())
            .unwrap()
            .detached
            .len(),
        1
    );
    assert!(!w.path.join("node_modules").exists());
}
#[test]
fn changed_dependencies_at_creation_are_not_shared() {
    let repo = Repo::new();
    repo.node();
    write(&repo.root, "package-lock.json", "uncommitted source change");
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    assert!(w.dependencies.artifacts.is_empty());
    assert!(!w.path.join("node_modules").exists());
}
#[test]
fn new_worktree_survives_but_merged_commit_is_removed() {
    let repo = Repo::new();
    repo.node();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    assert!(
        repo.manager()
            .sync(true, &HashSet::new())
            .unwrap()
            .removed
            .is_empty()
    );
    write(&w.path, "feature.txt", "done");
    commit(&w.path, "feature");
    git(&repo.root, &["merge", "--no-edit", "feature"]);
    let mut busy = HashSet::new();
    busy.insert(w.path.clone());
    assert!(repo.manager().sync(true, &busy).unwrap().removed.is_empty());
    let report = repo.manager().sync(true, &HashSet::new()).unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.removed, vec![w.id]);
    assert!(!w.path.exists());
    assert!(repo.root.join("node_modules/demo/index.js").exists());
    assert!(repo.manager().managed().unwrap().is_empty());
}
#[test]
fn dirty_untracked_and_ignored_files_prevent_cleanup() {
    let repo = Repo::new();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    for (name, content) in [
        ("README", "edited"),
        ("untracked", "data"),
        (".env", "secret"),
    ] {
        write(&w.path, name, content);
        assert!(repo.manager().remove(&w.id).is_err());
        if name == "README" {
            git(&w.path, &["restore", "README"]);
        } else {
            fs::remove_file(w.path.join(name)).unwrap();
        }
    }
    repo.manager().remove(&w.id).unwrap();
    assert_eq!(git(&repo.root, &["branch", "--list", "feature"]), "feature");
}
#[test]
fn manual_install_replacing_alias_is_preserved() {
    let repo = Repo::new();
    repo.node();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    fs::remove_file(w.path.join("node_modules/demo")).unwrap();
    write(&w.path, "node_modules/local.txt", "owned by user");
    write(&w.path, "package-lock.json", "lock v2");
    repo.manager().sync(false, &HashSet::new()).unwrap();
    assert!(w.path.join("node_modules/local.txt").exists());
    assert!(repo.manager().remove(&w.id).is_err());
}
#[test]
fn flutter_config_reuses_cache_and_remaps_local_packages() {
    let repo = Repo::new();
    write(
        &repo.root,
        "pubspec.yaml",
        "name: app\nenvironment:\n  sdk: ^3.0.0\n",
    );
    write(&repo.root, "pubspec.lock", "packages: {}\n");
    commit(&repo.root, "flutter");
    let cache = repo.dir.join("pub cache/demo");
    fs::create_dir_all(&cache).unwrap();
    let cache_uri = url::Url::from_directory_path(&cache).unwrap();
    write(
        &repo.root,
        ".dart_tool/package_config.json",
        &serde_json::json!({"configVersion":2,"packages":[
            {"name":"app", "rootUri":"../", "packageUri":"lib/"},
            {"name":"demo", "rootUri":cache_uri.to_string(), "packageUri":"lib/"}
        ]})
        .to_string(),
    );
    let w = repo
        .manager()
        .create(CreateOptions::new("flutter-feature"))
        .unwrap();
    assert!(w.last_error.is_none(), "{:?}", w.last_error);
    let config: serde_json::Value =
        serde_json::from_slice(&fs::read(w.path.join(".dart_tool/package_config.json")).unwrap())
            .unwrap();
    assert_eq!(
        config["packages"][0]["rootUri"],
        url::Url::from_directory_path(&w.path).unwrap().to_string()
    );
    assert_eq!(config["packages"][1]["rootUri"], cache_uri.to_string());
    write(
        &w.path,
        "pubspec_overrides.yaml",
        "dependency_overrides: {}\n",
    );
    repo.manager().sync(false, &HashSet::new()).unwrap();
    assert!(!w.path.join(".dart_tool/package_config.json").exists());
    assert!(cache.exists());
    assert!(repo.root.join(".dart_tool/package_config.json").exists());
}
#[test]
fn rust_vendor_detaches_on_cargo_config_change() {
    let repo = Repo::new();
    write(
        &repo.root,
        "Cargo.toml",
        "[package]\nname='example'\nversion='0.1.0'\n",
    );
    write(&repo.root, "Cargo.lock", "version = 4\n");
    write(
        &repo.root,
        ".cargo/config.toml",
        "[source.vendored-sources]\ndirectory='vendor'\n",
    );
    commit(&repo.root, "rust");
    write(&repo.root, "vendor/demo/Cargo.toml", "[package]");
    let w = repo
        .manager()
        .create(CreateOptions::new("rust-feature"))
        .unwrap();
    assert!(w.path.join("vendor/demo/Cargo.toml").exists());
    assert!(!w.path.join("target").exists());
    write(
        &w.path,
        ".cargo/config.toml",
        "[source.vendored-sources]\ndirectory='elsewhere'\n",
    );
    repo.manager().sync(false, &HashSet::new()).unwrap();
    assert!(!w.path.join("vendor").exists());
    assert!(repo.root.join("vendor/demo/Cargo.toml").exists());
}
#[cfg(unix)]
#[test]
fn node_workspace_links_resolve_to_the_new_checkout() {
    let repo = Repo::new();
    repo.node();
    write(
        &repo.root,
        "packages/local/package.json",
        r#"{"name":"local"}"#,
    );
    commit(&repo.root, "workspace");
    std::os::unix::fs::symlink("../packages/local", repo.root.join("node_modules/local")).unwrap();
    let w = repo
        .manager()
        .create(CreateOptions::new("workspace-feature"))
        .unwrap();
    assert!(w.last_error.is_none(), "{:?}", w.last_error);
    assert_eq!(
        w.path.join("node_modules/local").canonicalize().unwrap(),
        w.path.join("packages/local")
    );
    assert_eq!(
        w.path.join("node_modules/demo").canonicalize().unwrap(),
        repo.root.join("node_modules/demo")
    );
    write(
        &w.path,
        "packages/local/package.json",
        r#"{"name":"local","version":"2"}"#,
    );
    repo.manager().sync(false, &HashSet::new()).unwrap();
    assert!(!w.path.join("node_modules").exists());
}
#[test]
fn merge_command_and_keep_configuration() {
    let repo = Repo::new();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    repo.manager().configure(&w.id, false).unwrap();
    write(&w.path, "feature", "done");
    commit(&w.path, "feature");
    repo.manager().merge(&w.id).unwrap();
    assert!(w.path.exists());
    assert!(repo.root.join("feature").exists());
    repo.manager().configure(&w.id, true).unwrap();
    repo.manager().sync(true, &HashSet::new()).unwrap();
    assert!(!w.path.exists());
}
#[test]
fn locked_and_conflicted_worktrees_are_preserved() {
    let repo = Repo::new();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    git(&repo.root, &["worktree", "lock", w.path.to_str().unwrap()]);
    assert!(repo.manager().remove(&w.id).is_err());
    git(
        &repo.root,
        &["worktree", "unlock", w.path.to_str().unwrap()],
    );
    write(&repo.root, "README", "main\n");
    commit(&repo.root, "main change");
    write(&w.path, "README", "feature\n");
    commit(&w.path, "feature change");
    assert!(repo.manager().merge(&w.id).is_err());
    assert!(w.path.exists());
    assert!(!repo.manager().managed().unwrap().is_empty());
    git(&repo.root, &["merge", "--abort"]);
}
#[test]
fn prune_forgets_external_removal_and_corrupt_registry_is_not_overwritten() {
    let repo = Repo::new();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    git(
        &repo.root,
        &["worktree", "remove", w.path.to_str().unwrap()],
    );
    assert_eq!(repo.manager().prune().unwrap(), vec![w.id]);
    let registry = repo.root.join(".git/omni-worktree/registry.json");
    fs::write(&registry, "broken").unwrap();
    assert!(repo.manager().create(CreateOptions::new("other")).is_err());
    assert_eq!(fs::read_to_string(registry).unwrap(), "broken");
}

#[test]
fn existing_unmerged_branch_is_cleaned_after_external_merge() {
    let repo = Repo::new();
    git(&repo.root, &["checkout", "-b", "existing"]);
    write(&repo.root, "feature", "done");
    commit(&repo.root, "feature");
    git(&repo.root, &["checkout", "main"]);
    let mut options = CreateOptions::new("existing");
    options.existing = true;
    let w = repo.manager().create(options).unwrap();
    assert!(
        repo.manager()
            .sync(true, &HashSet::new())
            .unwrap()
            .removed
            .is_empty()
    );
    git(&repo.root, &["merge", "--no-edit", "existing"]);
    assert_eq!(
        repo.manager().sync(true, &HashSet::new()).unwrap().removed,
        vec![w.id]
    );
}
#[test]
fn already_merged_existing_branch_is_not_removed_on_creation() {
    let repo = Repo::new();
    git(&repo.root, &["branch", "existing"]);
    write(&repo.root, "new", "main has advanced");
    commit(&repo.root, "main advanced");
    let mut options = CreateOptions::new("existing");
    options.existing = true;
    let w = repo.manager().create(options).unwrap();
    assert!(
        repo.manager()
            .sync(true, &HashSet::new())
            .unwrap()
            .removed
            .is_empty()
    );
    assert!(w.path.exists());
}
#[test]
fn branch_switch_still_detaches_dependencies_but_blocks_cleanup() {
    let repo = Repo::new();
    repo.node();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    git(&w.path, &["checkout", "-b", "other"]);
    write(&w.path, "package-lock.json", "other dependencies");
    assert_eq!(
        repo.manager()
            .sync(false, &HashSet::new())
            .unwrap()
            .detached,
        vec![w.id.clone()]
    );
    assert!(repo.manager().remove(&w.id).is_err());
}
#[cfg(unix)]
#[test]
fn replaced_parent_symlink_cannot_delete_source_files() {
    let repo = Repo::new();
    repo.node();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    fs::remove_dir_all(w.path.join("node_modules")).unwrap();
    std::os::unix::fs::symlink(repo.root.join("node_modules"), w.path.join("node_modules"))
        .unwrap();
    write(&w.path, "package-lock.json", "changed");
    assert!(
        !repo
            .manager()
            .sync(false, &HashSet::new())
            .unwrap()
            .errors
            .is_empty()
    );
    assert!(repo.root.join("node_modules/demo/index.js").exists());
}
#[test]
fn concurrent_creates_keep_both_registry_entries() {
    let repo = Repo::new();
    let a = repo.manager();
    let b = repo.manager();
    let first = std::thread::spawn(move || a.create(CreateOptions::new("a")).unwrap());
    let second = std::thread::spawn(move || b.create(CreateOptions::new("b")).unwrap());
    first.join().unwrap();
    second.join().unwrap();
    assert_eq!(repo.manager().managed().unwrap().len(), 2);
}
#[test]
fn no_share_and_explicit_detach_leave_no_aliases() {
    let repo = Repo::new();
    repo.node();
    let mut options = CreateOptions::new("private");
    options.share_dependencies = false;
    let private = repo.manager().create(options).unwrap();
    assert!(private.dependencies.artifacts.is_empty());
    let w = repo.manager().create(CreateOptions::new("shared")).unwrap();
    repo.manager().detach(&w.id).unwrap();
    assert!(!w.path.join("node_modules").exists());
    assert!(
        repo.manager()
            .sync(false, &HashSet::new())
            .unwrap()
            .detached
            .is_empty()
    );
}
#[test]
fn composer_vendor_and_manifest_deletion() {
    let repo = Repo::new();
    write(
        &repo.root,
        "composer.json",
        r#"{"require":{"vendor/demo":"1.0"}}"#,
    );
    write(&repo.root, "composer.lock", "{}");
    commit(&repo.root, "composer");
    write(&repo.root, "vendor/autoload.php", "<?php");
    let w = repo.manager().create(CreateOptions::new("php")).unwrap();
    assert!(w.path.join("vendor/autoload.php").exists());
    fs::remove_file(w.path.join("composer.lock")).unwrap();
    repo.manager().sync(false, &HashSet::new()).unwrap();
    assert!(!w.path.join("vendor").exists());
}

#[test]
fn generated_rust_and_flutter_build_outputs_do_not_block_auto_cleanup() {
    let repo = Repo::new();
    write(
        &repo.root,
        "Cargo.toml",
        "[package]\nname='app'\nversion='0.1.0'\n",
    );
    write(&repo.root, "pubspec.yaml", "name: app\n");
    write(
        &repo.root,
        ".gitignore",
        "target/\nbuild/\n.dart_tool/\n.env\n",
    );
    commit(&repo.root, "apps");
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    write(&w.path, "target/debug/app", "compiled");
    write(&w.path, "build/linux/app", "compiled");
    write(
        &w.path,
        ".dart_tool/package_config.json",
        "new local install",
    );
    write(&w.path, "README", "feature");
    commit(&w.path, "feature");
    git(&repo.root, &["merge", "--no-edit", "feature"]);
    let report = repo.manager().sync(true, &HashSet::new()).unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.removed, vec![w.id]);
}

#[test]
fn removing_a_dependency_source_requires_detaching_dependents() {
    let repo = Repo::new();
    repo.node();
    let parent = repo.manager().create(CreateOptions::new("parent")).unwrap();
    let child = Manager::open(&parent.path)
        .unwrap()
        .create(CreateOptions::new("child"))
        .unwrap();
    assert!(repo.manager().remove(&parent.id).is_err());
    repo.manager().detach(&child.id).unwrap();
    repo.manager().remove(&parent.id).unwrap();
    assert!(child.path.exists());
    assert!(!child.path.join("node_modules").exists());
}

#[test]
fn source_code_edits_keep_shared_dependencies() {
    let repo = Repo::new();
    repo.node();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    write(&w.path, "README", "normal development");
    let report = repo.manager().sync(false, &HashSet::new()).unwrap();
    assert!(report.detached.is_empty());
    assert!(w.path.join("node_modules/demo/index.js").exists());
}

#[test]
fn merge_does_not_overwrite_ignored_target_data() {
    let repo = Repo::new();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    write(&w.path, ".env", "tracked feature config");
    git(&w.path, &["add", "-f", ".env"]);
    git(&w.path, &["commit", "-m", "config"]);
    write(&repo.root, ".env", "private target config");
    assert!(repo.manager().merge(&w.id).is_err());
    assert_eq!(
        fs::read_to_string(repo.root.join(".env")).unwrap(),
        "private target config"
    );
    assert!(w.path.exists());
}
#[test]
fn successful_merge_reports_blocked_cleanup_separately() {
    let repo = Repo::new();
    let w = repo
        .manager()
        .create(CreateOptions::new("feature"))
        .unwrap();
    write(&w.path, "feature", "done");
    commit(&w.path, "feature");
    write(&w.path, ".env", "private");
    let error = repo.manager().merge(&w.id).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("merge succeeded, but cleanup was blocked")
    );
    assert!(repo.root.join("feature").exists());
    assert!(w.path.join(".env").exists());
    assert!(repo.manager().managed().unwrap()[0].last_error.is_some());
}
