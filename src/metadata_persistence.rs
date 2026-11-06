use std::{
    fs::{self, OpenOptions},
    io::{self, BufWriter, Write},
    path::Path,
};

use serde::Serialize;
use uuid::Uuid;

/// Serialize without allocating a complete JSON string. Readers see either
/// the previous complete document or its complete replacement.
pub(crate) fn write_atomic_json(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if let Some(parent) = parent {
        fs::create_dir_all(parent)?;
    }
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "metadata path needs a file name",
        )
    })?;
    let temporary = path.with_file_name(format!(
        ".{}.{}.tmp",
        name.to_string_lossy(),
        Uuid::new_v4()
    ));
    let result = (|| {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        if let Ok(previous) = fs::metadata(path) {
            file.set_permissions(previous.permissions())?;
        }
        let mut writer = BufWriter::with_capacity(64 * 1024, file);
        serde_json::to_writer(&mut writer, value).map_err(io::Error::other)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::SerializeSeq;

    struct FailedSnapshot;

    impl Serialize for FailedSnapshot {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut sequence = serializer.serialize_seq(None)?;
            sequence.serialize_element(&"partial".repeat(20_000))?;
            Err(serde::ser::Error::custom("snapshot serialization failed"))
        }
    }

    #[test]
    fn failed_serialization_preserves_previous_document_and_removes_temporary_file() {
        let root = std::env::temp_dir().join(format!("omni-metadata-atomic-{}", Uuid::new_v4()));
        let path = root.join("metadata.json");
        let previous = serde_json::json!({"title": "原有会话"});
        write_atomic_json(&path, &previous).unwrap();

        let error = write_atomic_json(&path, &FailedSnapshot).unwrap_err();
        assert!(error.to_string().contains("snapshot serialization failed"));
        let restored: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(restored, previous);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);

        let replacement = serde_json::json!({"title": "新会话", "messages": ["已保存"]});
        write_atomic_json(&path, &replacement).unwrap();
        let restored: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(restored, replacement);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn replacing_metadata_preserves_existing_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("omni-metadata-mode-{}", Uuid::new_v4()));
        let path = root.join("metadata.json");
        write_atomic_json(&path, &serde_json::json!({})).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        write_atomic_json(&path, &serde_json::json!({"updated": true})).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::remove_dir_all(root).unwrap();
    }
}
