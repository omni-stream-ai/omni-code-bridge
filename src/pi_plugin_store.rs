use std::{
    collections::HashSet,
    fs,
    path::{Component, Path, PathBuf},
    sync::Mutex,
};

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use pi::extensions::{ExtensionLoadSpec, load_extension_manifest, resolve_extension_load_spec};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokio::{
    io::AsyncWriteExt,
    process::Command,
    time::{Duration, timeout},
};
use uuid::Uuid;

const MAX_PLUGIN_BYTES: usize = 25 * 1024 * 1024;
const PIJS_BUNDLE_NAME: &str = ".omni-pijs-bundle.mjs";
const MAX_PLUGIN_FILES: usize = 2_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PiPluginSourceKind {
    Npm,
    Url,
    Git,
    Upload,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiPluginSource {
    pub kind: PiPluginSourceKind,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiPlugin {
    pub id: String,
    pub name: String,
    pub version: Option<String>,
    pub source: PiPluginSource,
    pub entry_path: String,
    pub sha256: String,
    pub enabled: bool,
    #[serde(default)]
    pub project_ids: Vec<String>,
    #[serde(default)]
    pub config: Map<String, Value>,
    pub installed_at: DateTime<Utc>,
    pub validation_error: Option<String>,
    #[serde(default)]
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PiPluginCommand {
    pub name: String,
    pub source: String,
}

#[derive(Debug, Deserialize)]
pub struct InstallPiPluginInput {
    pub source: PiPluginSource,
    pub id: Option<String>,
    pub sha256: Option<String>,
    pub content_base64: Option<String>,
    pub file_name: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub project_ids: Vec<String>,
    #[serde(default)]
    pub config: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePiPluginInput {
    pub enabled: Option<bool>,
    pub project_ids: Option<Vec<String>>,
    pub config: Option<Map<String, Value>>,
}

fn default_true() -> bool {
    true
}

#[derive(Default, Serialize, Deserialize)]
struct Registry {
    #[serde(default)]
    plugins: Vec<PiPlugin>,
}

pub struct PiPluginStore {
    root: PathBuf,
    lock: Mutex<()>,
}

impl PiPluginStore {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            lock: Mutex::new(()),
        }
    }

    pub fn list(&self) -> Result<Vec<PiPlugin>> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| anyhow!("Pi plugin store lock poisoned"))?;
        let mut plugins = self.read_registry()?.plugins;
        for plugin in &mut plugins {
            plugin.validation_error = self.validation_error(plugin);
        }
        Ok(plugins)
    }

    pub fn enabled_paths(&self, project_id: &str) -> Result<Vec<PathBuf>> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| anyhow!("Pi plugin store lock poisoned"))?;
        let mut paths = Vec::new();
        for plugin in self.read_registry()?.plugins {
            if !plugin.enabled
                || (!plugin.project_ids.is_empty()
                    && !plugin.project_ids.iter().any(|id| id == project_id))
            {
                continue;
            }
            if let Some(error) = self.validation_error(&plugin) {
                bail!("Pi plugin {} is invalid: {error}", plugin.id);
            }
            let path = self.safe_installed_path(&plugin.entry_path)?;
            paths.push(path);
        }
        Ok(paths)
    }

    /// Extract literal command registrations without booting extension code.
    pub fn declared_commands(&self, project_id: &str) -> Result<Vec<PiPluginCommand>> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| anyhow!("Pi plugin store lock poisoned"))?;
        let mut commands = Vec::new();
        for plugin in self.read_registry()?.plugins {
            if !plugin.enabled
                || (!plugin.project_ids.is_empty()
                    && !plugin.project_ids.iter().any(|id| id == project_id))
            {
                continue;
            }
            let entry = self.safe_installed_path(&plugin.entry_path)?;
            let root = entry
                .parent()
                .ok_or_else(|| anyhow!("Pi plugin entry has no parent"))?;
            let mut files = Vec::new();
            collect_source_files(root, &mut files)?;
            for file in files {
                let Ok(source) = fs::read_to_string(&file) else {
                    continue;
                };
                for name in literal_registered_commands(&source) {
                    commands.push(PiPluginCommand {
                        name,
                        source: plugin.name.clone(),
                    });
                }
            }
        }
        commands.sort_by(|a, b| a.name.cmp(&b.name).then(a.source.cmp(&b.source)));
        commands.dedup_by(|a, b| a.name == b.name && a.source == b.source);
        Ok(commands)
    }

    pub async fn install(&self, input: InstallPiPluginInput) -> Result<PiPlugin> {
        validate_scope(&input.project_ids)?;
        let id = input
            .id
            .as_deref()
            .map(validate_id)
            .transpose()?
            .map(str::to_owned)
            .unwrap_or_else(|| format!("plugin-{}", &Uuid::new_v4().simple().to_string()[..12]));
        let staging = self.root.join(format!(".install-{id}-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&staging)
            .await
            .context("create Pi plugin staging directory")?;
        let result = self.populate(&staging, &input).await;
        if let Err(error) = result {
            let _ = tokio::fs::remove_dir_all(&staging).await;
            return Err(error);
        }
        scan_tree(&staging)?;
        let (entry, name, version, permissions) = plugin_metadata(&staging)?;
        let digest = digest_tree(&staging)?;
        if let Some(expected) = input.sha256.as_deref() {
            if !expected.eq_ignore_ascii_case(&digest) {
                let _ = tokio::fs::remove_dir_all(&staging).await;
                bail!("plugin checksum mismatch");
            }
        }
        let installed = self.root.join("installed").join(&id);
        tokio::fs::create_dir_all(installed.parent().unwrap()).await?;
        if tokio::fs::try_exists(&installed).await? {
            let _ = tokio::fs::remove_dir_all(&staging).await;
            bail!("Pi plugin id already exists: {id}");
        }
        tokio::fs::rename(&staging, &installed)
            .await
            .context("commit Pi plugin installation")?;
        let entry_rel = PathBuf::from("installed")
            .join(&id)
            .join(entry.strip_prefix(&staging).unwrap());
        let plugin = PiPlugin {
            id,
            name,
            version,
            source: input.source,
            entry_path: path_string(&entry_rel)?,
            sha256: digest,
            enabled: input.enabled,
            project_ids: input.project_ids,
            config: input.config,
            installed_at: Utc::now(),
            validation_error: None,
            permissions,
        };
        let _guard = self
            .lock
            .lock()
            .map_err(|_| anyhow!("Pi plugin store lock poisoned"))?;
        let mut registry = self.read_registry()?;
        registry.plugins.push(plugin.clone());
        if let Err(error) = self.write_registry(&registry) {
            let _ = fs::remove_dir_all(&installed);
            return Err(error);
        }
        Ok(plugin)
    }

    async fn populate(&self, staging: &Path, input: &InstallPiPluginInput) -> Result<()> {
        match input.source.kind {
            PiPluginSourceKind::Npm => {
                let spec = input.source.value.trim();
                if !spec.starts_with("npm:")
                    || spec.len() <= 4
                    || spec.contains('\n')
                    || spec.contains('\r')
                {
                    bail!("npm plugin source must look like npm:package[@version]");
                }
                let package_spec = &spec[4..];
                if package_spec.starts_with('-') {
                    bail!("invalid npm plugin source");
                }
                let output = timeout(
                    Duration::from_secs(120),
                    Command::new("npm")
                        .args([
                            "pack",
                            "--ignore-scripts",
                            "--no-audit",
                            "--no-fund",
                            "--pack-destination",
                        ])
                        .arg(staging)
                        .arg("--")
                        .arg(package_spec)
                        .env("npm_config_ignore_scripts", "true")
                        .output(),
                )
                .await
                .map_err(|_| anyhow!("npm pack timed out after 120 seconds"))??;
                if !output.status.success() {
                    bail!(
                        "npm pack failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                }
                let archive = fs::read_dir(staging)?
                    .filter_map(|entry| entry.ok().map(|value| value.path()))
                    .find(|path| path.extension().is_some_and(|ext| ext == "tgz"))
                    .ok_or_else(|| anyhow!("npm pack did not produce a package archive"))?;
                unpack_npm_archive(&archive, staging)?;
                fs::remove_file(archive)?;
                install_npm_dependencies(staging).await?;
                bundle_npm_plugin(staging).await?;
            }
            PiPluginSourceKind::Upload => {
                let encoded = input
                    .content_base64
                    .as_deref()
                    .ok_or_else(|| anyhow!("content_base64 is required for upload source"))?;
                let bytes = BASE64
                    .decode(encoded)
                    .context("invalid base64 plugin content")?;
                ensure_size(bytes.len())?;
                let name = safe_file_name(input.file_name.as_deref().unwrap_or("index.js"))?;
                tokio::fs::write(staging.join(name), bytes).await?;
            }
            PiPluginSourceKind::Url => {
                let url =
                    reqwest::Url::parse(input.source.value.trim()).context("invalid plugin URL")?;
                if !matches!(url.scheme(), "https" | "http") {
                    bail!("plugin URL must use http or https");
                }
                let response = reqwest::Client::builder()
                    .timeout(Duration::from_secs(60))
                    .build()?
                    .get(url.clone())
                    .send()
                    .await?
                    .error_for_status()?;
                if response
                    .content_length()
                    .is_some_and(|n| n > MAX_PLUGIN_BYTES as u64)
                {
                    bail!("plugin exceeds 25 MiB limit");
                }
                let guessed = url
                    .path_segments()
                    .and_then(Iterator::last)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("index.js");
                let mut output =
                    tokio::fs::File::create(staging.join(safe_file_name(guessed)?)).await?;
                let mut stream = response.bytes_stream();
                let mut total = 0usize;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    total = total
                        .checked_add(chunk.len())
                        .ok_or_else(|| anyhow!("plugin size overflow"))?;
                    ensure_size(total)?;
                    output.write_all(&chunk).await?;
                }
                output.flush().await?;
            }
            PiPluginSourceKind::Local => {
                let source = fs::canonicalize(input.source.value.trim())
                    .context("resolve local plugin path")?;
                copy_local(&source, staging)?;
            }
            PiPluginSourceKind::Git => {
                let value = input.source.value.trim();
                if value.starts_with('-') || value.contains('\n') || value.contains('\r') {
                    bail!("invalid Git source");
                }
                let mut command = Command::new("git");
                command
                    .args(["clone", "--depth", "1", "--", value])
                    .arg(staging);
                let status = timeout(Duration::from_secs(120), command.status())
                    .await
                    .map_err(|_| anyhow!("git clone timed out after 120 seconds"))?
                    .context("start git clone")?;
                if !status.success() {
                    bail!("git clone failed with status {status}");
                }
                let git = staging.join(".git");
                if git.exists() {
                    tokio::fs::remove_dir_all(git).await?;
                }
            }
        }
        Ok(())
    }

    pub fn update(&self, id: &str, input: UpdatePiPluginInput) -> Result<PiPlugin> {
        validate_id(id)?;
        if let Some(ids) = &input.project_ids {
            validate_scope(ids)?;
        }
        let _guard = self
            .lock
            .lock()
            .map_err(|_| anyhow!("Pi plugin store lock poisoned"))?;
        let mut registry = self.read_registry()?;
        let plugin = registry
            .plugins
            .iter_mut()
            .find(|p| p.id == id)
            .ok_or_else(|| anyhow!("Pi plugin not found: {id}"))?;
        if let Some(value) = input.enabled {
            plugin.enabled = value;
        }
        if let Some(value) = input.project_ids {
            plugin.project_ids = value;
        }
        if let Some(value) = input.config {
            plugin.config = value;
        }
        let result = plugin.clone();
        self.write_registry(&registry)?;
        Ok(result)
    }

    pub fn validate(&self, id: &str) -> Result<PiPlugin> {
        validate_id(id)?;
        let _guard = self
            .lock
            .lock()
            .map_err(|_| anyhow!("Pi plugin store lock poisoned"))?;
        let mut registry = self.read_registry()?;
        let index = registry
            .plugins
            .iter()
            .position(|p| p.id == id)
            .ok_or_else(|| anyhow!("Pi plugin not found: {id}"))?;
        let error = self.validation_error(&registry.plugins[index]);
        registry.plugins[index].validation_error = error;
        let result = registry.plugins[index].clone();
        self.write_registry(&registry)?;
        Ok(result)
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        validate_id(id)?;
        let _guard = self
            .lock
            .lock()
            .map_err(|_| anyhow!("Pi plugin store lock poisoned"))?;
        let mut registry = self.read_registry()?;
        let before = registry.plugins.len();
        registry.plugins.retain(|p| p.id != id);
        if before == registry.plugins.len() {
            bail!("Pi plugin not found: {id}");
        }
        self.write_registry(&registry)?;
        let path = self.root.join("installed").join(id);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    fn read_registry(&self) -> Result<Registry> {
        let path = self.root.join("registry.json");
        if !path.exists() {
            return Ok(Registry::default());
        }
        serde_json::from_slice(&fs::read(&path)?).context("parse Pi plugin registry")
    }
    fn write_registry(&self, registry: &Registry) -> Result<()> {
        fs::create_dir_all(&self.root)?;
        let path = self.root.join("registry.json");
        let temp = self.root.join("registry.json.tmp");
        fs::write(&temp, serde_json::to_vec_pretty(registry)?)?;
        fs::rename(temp, path)?;
        Ok(())
    }
    fn safe_installed_path(&self, relative: &str) -> Result<PathBuf> {
        let relative = Path::new(relative);
        if relative.is_absolute()
            || relative
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
        {
            bail!("invalid plugin entry path");
        }
        let installed =
            fs::canonicalize(self.root.join("installed")).context("resolve plugin directory")?;
        let path = fs::canonicalize(self.root.join(relative)).context("resolve plugin entry")?;
        if !path.starts_with(&installed) {
            bail!("plugin entry escapes managed directory");
        }
        Ok(path)
    }

    fn validation_error(&self, plugin: &PiPlugin) -> Option<String> {
        (|| -> Result<()> {
            let plugin_root = self.root.join("installed").join(&plugin.id);
            scan_tree(&plugin_root)?;
            let digest = digest_tree(&plugin_root)?;
            if !digest.eq_ignore_ascii_case(&plugin.sha256) {
                bail!("plugin files changed after installation (checksum mismatch)");
            }
            let entry = self.safe_installed_path(&plugin.entry_path)?;
            resolve_extension_load_spec(&entry).map_err(|e| anyhow!(e.to_string()))?;
            Ok(())
        })()
        .err()
        .map(|error| error.to_string())
    }
}

fn validate_id(id: &str) -> Result<&str> {
    if id.is_empty()
        || id.len() > 80
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        bail!("invalid plugin id");
    }
    Ok(id)
}
fn validate_scope(ids: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for id in ids {
        validate_id(id)?;
        if !seen.insert(id) {
            bail!("duplicate project id: {id}");
        }
    }
    Ok(())
}
fn safe_file_name(name: &str) -> Result<&str> {
    if name.is_empty()
        || name.len() > 200
        || Path::new(name).file_name().and_then(|v| v.to_str()) != Some(name)
    {
        bail!("invalid plugin file name");
    }
    Ok(name)
}
fn ensure_size(size: usize) -> Result<()> {
    if size > MAX_PLUGIN_BYTES {
        bail!("plugin exceeds 25 MiB limit");
    }
    Ok(())
}
fn path_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("plugin path is not UTF-8"))
}

fn copy_local(source: &Path, target: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        bail!("symlinked plugin sources are not allowed");
    }
    if metadata.is_file() {
        ensure_size(metadata.len() as usize)?;
        fs::copy(
            source,
            target.join(safe_file_name(
                source
                    .file_name()
                    .and_then(|v| v.to_str())
                    .unwrap_or("index.js"),
            )?),
        )?;
        return Ok(());
    }
    if !metadata.is_dir() {
        bail!("local plugin source must be a file or directory");
    }
    fn walk(src: &Path, dst: &Path, count: &mut usize, bytes: &mut usize) -> Result<()> {
        fs::create_dir_all(dst)?;
        for entry in fs::read_dir(src)? {
            let entry = entry?;
            *count += 1;
            if *count > MAX_PLUGIN_FILES {
                bail!("plugin exceeds file count limit");
            }
            let meta = fs::symlink_metadata(entry.path())?;
            if meta.file_type().is_symlink() {
                bail!("plugin contains a symlink");
            }
            let out = dst.join(entry.file_name());
            if meta.is_dir() {
                walk(&entry.path(), &out, count, bytes)?;
            } else if meta.is_file() {
                *bytes += meta.len() as usize;
                ensure_size(*bytes)?;
                fs::copy(entry.path(), out)?;
            }
        }
        Ok(())
    }
    walk(source, target, &mut 0, &mut 0)
}

fn unpack_npm_archive(archive: &Path, target: &Path) -> Result<()> {
    let file = fs::File::open(archive).context("open npm package archive")?;
    let decoder = GzDecoder::new(file);
    let mut tar = tar::Archive::new(decoder);
    for entry in tar.entries().context("read npm package archive")? {
        let mut entry = entry.context("read npm package entry")?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            bail!("npm package contains a symlink or hard link");
        }
        let path = entry.path().context("read npm package path")?.into_owned();
        let mut components = path.components();
        if !matches!(components.next(), Some(Component::Normal(prefix)) if prefix == "package") {
            bail!("npm package entry must be under package/");
        }
        let relative = components.collect::<PathBuf>();
        if relative.as_os_str().is_empty()
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            bail!("npm package contains an unsafe path");
        }
        let destination = target.join(&relative);
        if entry_type.is_dir() {
            fs::create_dir_all(&destination)?;
        } else if entry_type.is_file() {
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }
            entry
                .unpack(&destination)
                .context("extract npm package file")?;
        } else {
            bail!("npm package contains unsupported archive entry");
        }
    }
    Ok(())
}

async fn install_npm_dependencies(root: &Path) -> Result<()> {
    let output = timeout(
        Duration::from_secs(300),
        Command::new("npm")
            .args([
                "install",
                "--omit=dev",
                "--ignore-scripts",
                "--no-audit",
                "--no-fund",
            ])
            .env("npm_config_ignore_scripts", "true")
            .current_dir(root)
            .output(),
    )
    .await
    .map_err(|_| anyhow!("npm dependency installation timed out after 300 seconds"))??;
    if !output.status.success() {
        bail!(
            "npm dependency installation failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

async fn bundle_npm_plugin(root: &Path) -> Result<()> {
    let (entry, _, _, _) = plugin_metadata(root)?;
    let output = timeout(
        Duration::from_secs(300),
        Command::new("npm")
            .args([
                "exec",
                "--yes",
                "--package=esbuild@0.28.2",
                "--",
                "esbuild",
            ])
            .arg(&entry)
            .args([
                "--bundle",
                "--platform=node",
                "--format=esm",
                "--minify-identifiers",
                "--banner:js=import { createRequire as __omniCreateRequire } from \"node:module\"; const require = __omniCreateRequire(import.meta.url);",
                "--external:@earendil-works/pi-coding-agent",
                "--external:@earendil-works/pi-ai",
                "--external:@earendil-works/pi-ai/compat",
                "--external:@earendil-works/pi-tui",
                "--external:typebox",
                "--external:node:*",
            ])
            .arg(format!("--outfile={PIJS_BUNDLE_NAME}"))
            .env("npm_config_ignore_scripts", "true")
            .current_dir(root)
            .output(),
    )
    .await
    .map_err(|_| anyhow!("npm plugin bundling timed out after 300 seconds"))??;
    if !output.status.success() {
        bail!(
            "npm plugin bundling failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    if !root.join(PIJS_BUNDLE_NAME).is_file() {
        bail!("npm plugin bundling did not produce {PIJS_BUNDLE_NAME}");
    }
    let dependencies = root.join("node_modules");
    if dependencies.exists() {
        tokio::fs::remove_dir_all(dependencies)
            .await
            .context("remove bundled npm dependencies")?;
    }
    Ok(())
}

fn scan_tree(root: &Path) -> Result<()> {
    let temp = tempfile_scan(root)?;
    ensure_size(temp.1)?;
    Ok(())
}
fn tempfile_scan(root: &Path) -> Result<(usize, usize)> {
    fn walk(p: &Path, n: &mut usize, b: &mut usize) -> Result<()> {
        for e in fs::read_dir(p)? {
            let e = e?;
            *n += 1;
            if *n > MAX_PLUGIN_FILES {
                bail!("plugin exceeds file count limit");
            }
            let m = fs::symlink_metadata(e.path())?;
            if m.file_type().is_symlink() {
                bail!("plugin contains a symlink");
            }
            if m.is_dir() {
                walk(&e.path(), n, b)?;
            } else if m.is_file() {
                *b += m.len() as usize;
            }
        }
        Ok(())
    }
    let (mut n, mut b) = (0, 0);
    walk(root, &mut n, &mut b)?;
    Ok((n, b))
}
fn digest_tree(root: &Path) -> Result<String> {
    let mut files = Vec::new();
    fn collect(p: &Path, v: &mut Vec<PathBuf>) -> Result<()> {
        for e in fs::read_dir(p)? {
            let e = e?;
            if e.file_type()?.is_dir() {
                collect(&e.path(), v)?
            } else {
                v.push(e.path())
            }
        }
        Ok(())
    }
    collect(root, &mut files)?;
    files.sort();
    let mut h = Sha256::new();
    for p in files {
        h.update(p.strip_prefix(root)?.to_string_lossy().as_bytes());
        h.update([0]);
        h.update(fs::read(p)?);
    }
    Ok(format!("{:x}", h.finalize()))
}
fn plugin_metadata(root: &Path) -> Result<(PathBuf, String, Option<String>, Vec<String>)> {
    let spec = resolve_extension_load_spec(root).map_err(|e| anyhow!(e.to_string()))?;
    let (mut entry, fallback_name, fallback_version) = match spec {
        ExtensionLoadSpec::Js(s) => (s.entry_path, s.name, s.version),
        ExtensionLoadSpec::NativeRust(s) => (s.entry_path, s.name, s.version),
    };
    let bundled_entry = root.join(PIJS_BUNDLE_NAME);
    if bundled_entry.is_file() {
        entry = bundled_entry;
    }
    let manifest = load_extension_manifest(root).map_err(|e| anyhow!(e.to_string()))?;
    let name = manifest
        .as_ref()
        .map(|m| m.manifest.name.clone())
        .filter(|v| !v.trim().is_empty())
        .unwrap_or(fallback_name);
    let version = manifest
        .as_ref()
        .map(|m| m.manifest.version.clone())
        .filter(|v| !v.trim().is_empty())
        .or_else(|| (!fallback_version.trim().is_empty()).then_some(fallback_version));
    let permissions = manifest
        .map(|m| m.manifest.capabilities)
        .unwrap_or_default();
    Ok((entry, name, version, permissions))
}

fn collect_source_files(root: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        fs::read_dir(root).with_context(|| format!("read plugin directory {}", root.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() {
            collect_source_files(&path, files)?;
        } else if metadata.is_file()
            && matches!(
                path.extension().and_then(|value| value.to_str()),
                Some("js" | "ts" | "mjs" | "cjs")
            )
        {
            files.push(path);
        }
    }
    Ok(())
}

fn literal_registered_commands(source: &str) -> Vec<String> {
    let mut constants = std::collections::HashMap::new();
    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("const ") {
            if let Some((name, value)) = rest.split_once('=') {
                let name = name.trim();
                let value = value.trim().trim_end_matches(';').trim();
                if value.len() >= 2
                    && matches!(value.as_bytes()[0], b'\'' | b'"')
                    && value.as_bytes().last() == value.as_bytes().first()
                {
                    constants.insert(name.to_string(), value[1..value.len() - 1].to_string());
                }
            }
        }
    }
    let needle = "registerCommand";
    let mut commands = Vec::new();
    let mut offset = 0;
    while let Some(relative) = source[offset..].find(needle) {
        let start = offset + relative + needle.len();
        let rest = source[start..].trim_start();
        let Some(rest) = rest.strip_prefix('(') else {
            offset = start;
            continue;
        };
        let rest = rest.trim_start();
        let first = rest.chars().next();
        let resolved = if let Some(quote) = first.filter(|ch| matches!(ch, '\'' | '"')) {
            let body = &rest[quote.len_utf8()..];
            let Some(end) = body.find(quote) else { break };
            body[..end].trim().to_string()
        } else {
            let identifier = rest
                .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                .next()
                .unwrap_or("");
            constants.get(identifier).cloned().unwrap_or_default()
        };
        let name = resolved.trim();
        if !name.is_empty()
            && name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '/'))
        {
            commands.push(format!("/{}", name.trim_start_matches('/')));
        }
        offset = start + needle.len();
    }
    commands
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (PathBuf, PiPluginStore) {
        let root = std::env::temp_dir().join(format!("omni-pi-plugin-test-{}", Uuid::new_v4()));
        (root.clone(), PiPluginStore::new(root))
    }

    fn upload(project_ids: Vec<String>) -> InstallPiPluginInput {
        InstallPiPluginInput {
            source: PiPluginSource {
                kind: PiPluginSourceKind::Upload,
                value: "index.js".into(),
            },
            id: Some("test-plugin".into()),
            sha256: None,
            content_base64: Some(BASE64.encode("export default function(pi) {}")),
            file_name: Some("index.js".into()),
            enabled: true,
            project_ids,
            config: Map::new(),
        }
    }

    #[tokio::test]
    async fn install_scope_validate_tamper_and_remove() {
        let (root, store) = store();
        let plugin = store
            .install(upload(vec!["project-a".into()]))
            .await
            .unwrap();
        assert_eq!(plugin.id, "test-plugin");
        assert!(store.enabled_paths("project-b").unwrap().is_empty());
        assert_eq!(store.enabled_paths("project-a").unwrap().len(), 1);
        fs::write(root.join("installed/test-plugin/index.js"), "changed").unwrap();
        let listed = store.list().unwrap();
        assert!(
            listed[0]
                .validation_error
                .as_deref()
                .unwrap()
                .contains("checksum")
        );
        assert!(store.enabled_paths("project-a").is_err());
        store.remove("test-plugin").unwrap();
        assert!(store.list().unwrap().is_empty());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn rejects_bad_ids_and_oversized_uploads() {
        let (root, store) = store();
        let mut bad = upload(Vec::new());
        bad.id = Some("../escape".into());
        assert!(store.install(bad).await.is_err());
        let mut large = upload(Vec::new());
        large.content_base64 = Some(BASE64.encode(vec![0; MAX_PLUGIN_BYTES + 1]));
        assert!(store.install(large).await.is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn npm_archive_extraction_rejects_links_and_preserves_package_files() {
        let root = std::env::temp_dir().join(format!("omni-pi-npm-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let archive = root.join("package.tgz");
        let file = fs::File::create(&archive).unwrap();
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut builder = tar::Builder::new(encoder);
        let content = b"export default function(pi) {}";
        let mut header = tar::Header::new_gnu();
        header.set_path("package/index.js").unwrap();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, &content[..]).unwrap();
        builder.into_inner().unwrap().finish().unwrap();
        let target = root.join("extracted");
        unpack_npm_archive(&archive, &target).unwrap();
        assert_eq!(fs::read(target.join("index.js")).unwrap(), content);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scans_literal_and_constant_command_names() {
        let source = r#"
            const COMMAND_NAME = "figma-remote-auth";
            pi.registerCommand("mcp", {});
            pi.registerCommand(COMMAND_NAME, {});
        "#;
        assert_eq!(
            literal_registered_commands(source),
            vec!["/mcp".to_string(), "/figma-remote-auth".to_string()]
        );
    }

    #[tokio::test]
    #[ignore = "requires npm registry access"]
    async fn installs_npm_plugin_as_pijs_bundle() {
        let (root, store) = store();
        let plugin = store
            .install(InstallPiPluginInput {
                source: PiPluginSource {
                    kind: PiPluginSourceKind::Npm,
                    value: "npm:pi-mcp-adapter@2.26.1".into(),
                },
                id: Some("mcp-adapter".into()),
                sha256: None,
                content_base64: None,
                file_name: None,
                enabled: true,
                project_ids: Vec::new(),
                config: Map::new(),
            })
            .await
            .expect("npm plugin should install");
        assert!(plugin.entry_path.ends_with(PIJS_BUNDLE_NAME));
        assert!(root.join(&plugin.entry_path).is_file());
        assert!(!root.join("installed/mcp-adapter/node_modules").exists());
        let commands = store.declared_commands("").expect("scan commands");
        assert!(commands.iter().any(|command| command.name == "/mcp"));
    }
}
