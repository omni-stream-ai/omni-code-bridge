use std::{collections::HashMap, path::PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::models::{ClientAuthRecord, ClientAuthStatus};

pub struct ClientAuthStore {
    path: PathBuf,
    records: RwLock<HashMap<String, ClientAuthRecord>>,
}

impl ClientAuthStore {
    pub async fn load() -> Self {
        let path = storage_path();
        let records = read_records(&path).await;
        Self {
            path,
            records: RwLock::new(records),
        }
    }

    pub async fn list(&self) -> Vec<ClientAuthRecord> {
        self.reload().await;
        let mut items = self
            .records
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        items.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        items
    }

    pub async fn get(&self, request_id: &str) -> Option<ClientAuthRecord> {
        self.reload().await;
        self.records.read().await.get(request_id).cloned()
    }

    pub async fn find_approved_by_client_id(&self, client_id: &str) -> Option<ClientAuthRecord> {
        self.reload().await;
        self.records
            .read()
            .await
            .values()
            .find(|record| record.client_id == client_id && record.token.is_some())
            .cloned()
    }

    pub async fn has_approved_client_id(&self, client_id: &str) -> bool {
        self.find_approved_by_client_id(client_id).await.is_some()
    }

    pub async fn token_matches(&self, client_id: &str, token: &str) -> bool {
        self.reload().await;
        self.records.read().await.values().any(|record| {
            record.client_id == client_id && record.token.as_deref() == Some(token)
        })
    }

    pub async fn upsert(&self, record: ClientAuthRecord) -> Result<ClientAuthRecord> {
        self.reload().await;
        {
            let mut records = self.records.write().await;
            records.insert(record.request_id.clone(), record.clone());
        }
        self.save().await?;
        Ok(record)
    }

    pub async fn approve(&self, request_id: &str) -> Result<ClientAuthRecord> {
        self.reload().await;
        let mut records = self.records.write().await;
        let record = records
            .get_mut(request_id)
            .context("request not found")?;

        if record.status == ClientAuthStatus::Approved {
            anyhow::bail!("request {} is already approved", request_id);
        }

        let token = Uuid::new_v4().to_string().replace('-', "");
        record.status = ClientAuthStatus::Approved;
        record.token = Some(token);
        record.updated_at = Utc::now();

        let record = record.clone();
        drop(records);
        self.save().await?;
        Ok(record)
    }

    pub async fn approve_all_pending(&self) -> Result<Vec<ClientAuthRecord>> {
        self.reload().await;
        let mut records = self.records.write().await;
        let mut approved = Vec::new();

        for record in records.values_mut() {
            if record.status == ClientAuthStatus::Pending {
                let token = Uuid::new_v4().to_string().replace('-', "");
                record.status = ClientAuthStatus::Approved;
                record.token = Some(token);
                record.updated_at = Utc::now();
                approved.push(record.clone());
            }
        }

        drop(records);
        if !approved.is_empty() {
            self.save().await?;
        }
        Ok(approved)
    }

    async fn save(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "failed to create client auth directory: {}",
                    parent.display()
                )
            })?;
        }

        let body = {
            let records = self.records.read().await;
            serde_json::to_string_pretty(&*records).context("failed to encode client auth store")?
        };
        tokio::fs::write(&self.path, body)
            .await
            .with_context(|| format!("failed to write client auth store: {}", self.path.display()))
    }

    async fn reload(&self) {
        let records = read_records(&self.path).await;
        *self.records.write().await = records;
    }
}

async fn read_records(path: &PathBuf) -> HashMap<String, ClientAuthRecord> {
    match tokio::fs::read_to_string(path).await {
        Ok(body) => {
            serde_json::from_str::<HashMap<String, ClientAuthRecord>>(&body).unwrap_or_default()
        }
        Err(_) => HashMap::new(),
    }
}

fn storage_path() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".omni-code")
        .join("client-auth.json")
}
