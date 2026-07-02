use std::{collections::HashMap, path::PathBuf};

use anyhow::{Context, Result};
use tokio::sync::RwLock;

pub struct SecretStore {
    path: PathBuf,
    records: RwLock<HashMap<String, String>>,
}

impl SecretStore {
    pub async fn load() -> Self {
        let path = storage_path();
        let records = read_records(&path).await;
        Self {
            path,
            records: RwLock::new(records),
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.reload().await;
        self.records.read().await.get(key).cloned()
    }

    pub async fn set(&self, key: String, value: String) -> Result<()> {
        self.reload().await;
        {
            let mut records = self.records.write().await;
            records.insert(key, value);
        }
        self.save().await
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        self.reload().await;
        {
            let mut records = self.records.write().await;
            records.remove(key);
        }
        self.save().await
    }

    async fn save(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "failed to create secret store directory: {}",
                    parent.display()
                )
            })?;
        }

        let body = {
            let records = self.records.read().await;
            serde_json::to_string_pretty(&*records).context("failed to encode secret store")?
        };
        tokio::fs::write(&self.path, body)
            .await
            .with_context(|| format!("failed to write secret store: {}", self.path.display()))
    }

    async fn reload(&self) {
        let records = read_records(&self.path).await;
        *self.records.write().await = records;
    }
}

async fn read_records(path: &PathBuf) -> HashMap<String, String> {
    match tokio::fs::read_to_string(path).await {
        Ok(body) => serde_json::from_str::<HashMap<String, String>>(&body).unwrap_or_default(),
        Err(_) => HashMap::new(),
    }
}

fn storage_path() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".omni-code")
        .join("acp-secrets.json")
}
