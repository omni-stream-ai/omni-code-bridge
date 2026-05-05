use std::{collections::HashMap, fs, path::PathBuf};

use crate::models::PushDeviceRegistration;

pub fn load_device_registrations() -> HashMap<String, PushDeviceRegistration> {
    let path = storage_path();
    let Ok(body) = fs::read_to_string(path) else {
        return HashMap::new();
    };
    serde_json::from_str(&body).unwrap_or_default()
}

pub fn save_device_registrations(devices: &HashMap<String, PushDeviceRegistration>) {
    let path = storage_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let Ok(body) = serde_json::to_string_pretty(devices) else {
        return;
    };
    let _ = fs::write(path, body);
}

fn storage_path() -> PathBuf {
    let mut path = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    path.push(".omni-code");
    path.push("device-registrations.json");
    path
}
