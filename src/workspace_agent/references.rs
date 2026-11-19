//! Explicitly selected files and skills supplied by the workspace composer.
use super::*;
use crate::models::{AgentKind, ProjectSummary};
use std::path::{Component, Path};
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum ComposerReference {
    File { project_id: String, path: String },
    Skill { project_id: String, name: String },
}
impl ComposerReference {
    pub(super) fn project_id(&self) -> &str {
        match self {
            Self::File { project_id, .. } | Self::Skill { project_id, .. } => project_id,
        }
    }
}

pub(super) async fn resolve(
    projects: &[ProjectSummary],
    selected_project: Option<&str>,
    agent: AgentKind,
    references: &[ComposerReference],
) -> Result<Vec<Value>, String> {
    if references.len() > 16 {
        return Err("Select at most 16 references".into());
    }
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    let mut bytes = 0;
    for reference in references {
        let id = reference.project_id();
        if selected_project.is_some_and(|selected| selected != id) {
            return Err("Reference is outside the selected project".into());
        }
        let project = projects
            .iter()
            .find(|project| project.id == id)
            .ok_or_else(|| "Unknown reference project".to_string())?;
        let key = serde_json::to_string(reference).map_err(|error| error.to_string())?;
        if !seen.insert(key) {
            continue;
        }
        match reference {
            ComposerReference::File { path, .. } => {
                let relative = Path::new(path);
                if path.is_empty()
                    || path.len() > 4096
                    || relative.is_absolute()
                    || relative.components().any(|part| {
                        matches!(
                            part,
                            Component::ParentDir | Component::RootDir | Component::Prefix(_)
                        )
                    })
                {
                    return Err("File references must use a project-relative path".into());
                }
                let root = tokio::fs::canonicalize(&project.root_path)
                    .await
                    .map_err(|_| "Reference project is not a local directory".to_string())?;
                let file = tokio::fs::canonicalize(root.join(relative))
                    .await
                    .map_err(|_| format!("Referenced file is unavailable: {path}"))?;
                if !file.starts_with(&root) {
                    return Err("Referenced file is outside its project".into());
                }
                let source = tokio::fs::File::open(&file)
                    .await
                    .map_err(|error| error.to_string())?;
                if !source
                    .metadata()
                    .await
                    .map_err(|error| error.to_string())?
                    .is_file()
                {
                    return Err("Select a file rather than a directory".into());
                }
                let mut content = Vec::new();
                source
                    .take(16 * 1024 + 1)
                    .read_to_end(&mut content)
                    .await
                    .map_err(|error| error.to_string())?;
                let truncated = content.len() > 16 * 1024;
                content.truncate(16 * 1024);
                bytes += content.len();
                if bytes > 64 * 1024 {
                    return Err("Referenced files exceed 64 KB of context".into());
                }
                if content.contains(&0) {
                    return Err("Attach binary files instead of referencing them".into());
                }
                result.push(json!({"kind":"file", "project_id":id, "project_name":project.name,
                    "path":path, "content":String::from_utf8_lossy(&content), "truncated":truncated}));
            }
            ComposerReference::Skill { name, .. } => {
                if agent != AgentKind::Codex
                    || name.is_empty()
                    || name.len() > 200
                    || !name
                        .chars()
                        .all(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | '.'))
                {
                    return Err("Invalid skill for the selected execution agent".into());
                }
                result.push(json!({"kind":"skill", "project_id":id, "project_name":project.name, "name":name}));
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn project(root: &Path) -> ProjectSummary {
        ProjectSummary {
            id: "project".into(),
            name: "Example".into(),
            root_path: root.to_string_lossy().into(),
            updated_at: Utc::now(),
            session_count: 0,
            icon_data_uri: None,
            last_session_preview: None,
            git_branch: None,
            git_status: None,
        }
    }
    fn reference(path: &str) -> ComposerReference {
        ComposerReference::File {
            project_id: "project".into(),
            path: path.into(),
        }
    }

    #[tokio::test]
    async fn selected_files_supply_real_content_and_respect_project_boundaries() {
        let root =
            std::env::temp_dir().join(format!("workspace-references-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("read me.txt"), "actual project content").unwrap();
        let projects = [project(&root)];
        let result = resolve(
            &projects,
            None,
            AgentKind::Pi,
            &[reference("read me.txt"), reference("read me.txt")],
        )
        .await
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["content"], "actual project content");
        for path in ["../outside", "/etc/passwd", ".", "missing"] {
            assert!(
                resolve(&projects, None, AgentKind::Pi, &[reference(path)])
                    .await
                    .is_err()
            );
        }
        assert!(
            resolve(
                &projects,
                Some("other"),
                AgentKind::Pi,
                &[reference("read me.txt")]
            )
            .await
            .is_err()
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(std::env::temp_dir(), root.join("outside")).unwrap();
            assert!(
                resolve(&projects, None, AgentKind::Pi, &[reference("outside")])
                    .await
                    .is_err()
            );
        }
        std::fs::write(root.join("large"), vec![b'x'; 20 * 1024]).unwrap();
        let large = resolve(&projects, None, AgentKind::Pi, &[reference("large")])
            .await
            .unwrap();
        assert_eq!(large[0]["truncated"], true);
        assert_eq!(large[0]["content"].as_str().unwrap().len(), 16 * 1024);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn skills_require_the_matching_execution_agent_and_project() {
        let projects = [project(Path::new("."))];
        let skill = ComposerReference::Skill {
            project_id: "project".into(),
            name: "review".into(),
        };
        let result = resolve(
            &projects,
            Some("project"),
            AgentKind::Codex,
            &[skill.clone()],
        )
        .await
        .unwrap();
        assert_eq!(result[0]["name"], "review");
        assert!(
            resolve(&projects, None, AgentKind::Pi, &[skill])
                .await
                .is_err()
        );
        let invalid = ComposerReference::Skill {
            project_id: "project".into(),
            name: "../skill".into(),
        };
        assert!(
            resolve(&projects, None, AgentKind::Codex, &[invalid])
                .await
                .is_err()
        );
    }

    #[test]
    fn dispatch_preserves_selected_skill_and_files_only_in_their_project() {
        let mut plan = serde_json::from_value::<Vec<dispatch::PlannedTask>>(json!([
            {"project_id":"project","title":"Review","instruction":"Review the implementation"},
            {"project_id":"other","title":"Other","instruction":"Do other work"}
        ]))
        .unwrap();
        let references = [
            reference("src/main.rs"),
            ComposerReference::Skill {
                project_id: "project".into(),
                name: "review".into(),
            },
        ];
        dispatch::preserve_references(&mut plan, &references);
        dispatch::preserve_references(&mut plan, &references);
        let result = serde_json::to_value(plan).unwrap();
        let instruction = result[0]["instruction"].as_str().unwrap();
        assert!(instruction.starts_with("$review "));
        assert_eq!(instruction.matches("$review").count(), 1);
        assert_eq!(instruction.matches("src/main.rs").count(), 1);
        assert_eq!(result[1]["instruction"], "Do other work");
    }
}
