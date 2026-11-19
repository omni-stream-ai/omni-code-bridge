//! Read-only project facts associated with the current assistant conversation.
use super::*;
use crate::models::{GitStatusDetail, ProjectGitStatus};
use std::time::Instant;

pub(super) struct CachedGit {
    root: String,
    checked_at: Instant,
    status: Option<GitStatusDetail>,
    commit: Option<GitCommit>,
}

fn unique_project<'a>(ids: impl Iterator<Item = &'a str>) -> Option<String> {
    let ids = ids.collect::<HashSet<_>>();
    (ids.len() == 1).then(|| ids.into_iter().next().unwrap().to_string())
}

pub(super) fn chat_project(
    query: &ScopeQuery,
    proposed: Option<&str>,
    plans: &[dispatch::PlannedTask],
    references: &[references::ComposerReference],
    available: &[Value],
) -> Result<Option<String>, String> {
    if let Some(id) = proposed {
        if !available.iter().any(|project| project["id"] == id)
            || query
                .project_id
                .as_ref()
                .is_some_and(|selected| selected != id)
        {
            return Err("Conversation project is outside the selected project scope".into());
        }
    }
    Ok(query
        .project_id
        .clone()
        .or_else(|| proposed.map(str::to_owned))
        .or_else(|| unique_project(plans.iter().map(|task| task.project_id.as_str())))
        .or_else(|| unique_project(references.iter().map(|reference| reference.project_id()))))
}

fn project_id(memory: &Memory, query: &ScopeQuery, data: &Value) -> Option<String> {
    query.project_id.clone().or_else(|| {
        let id = query.conversation_id.as_ref()?;
        memory
            .conversations
            .get(id)
            .and_then(|conversation| conversation.active_project_id.clone())
            .or_else(|| {
                // Older saved conversations can recover their own latest project.
                // Never borrow the project from another main conversation.
                let tasks = data["dispatch_tasks"].as_array()?;
                let latest_request = tasks
                    .iter()
                    .rev()
                    .find(|task| task["conversation_id"] == *id)?["request_id"]
                    .as_str()?;
                unique_project(
                    tasks
                        .iter()
                        .filter(|task| {
                            task["conversation_id"] == *id && task["request_id"] == latest_request
                        })
                        .filter_map(|task| task["project_id"].as_str()),
                )
            })
    })
}

pub(super) async fn enrich(state: &AppState, query: &ScopeQuery, data: &mut Value) {
    let id = {
        let memory = state.workspace_agent.memory.lock().await;
        project_id(&memory, query, data)
    };
    let Some(id) = id else {
        return;
    };
    let Some(mut project) = state
        .list_projects()
        .await
        .into_iter()
        .find(|project| project.id == id)
    else {
        return;
    };
    // Share the bounded status read across chat and task-panel polling. Git facts
    // stay in memory, are refreshed periodically, and are never treated as task acceptance.
    let (status, commit) = {
        let mut cache = state.workspace_agent.project_git_cache.lock().await;
        let fresh = cache.get(&id).is_some_and(|cached| {
            cached.root == project.root_path && cached.checked_at.elapsed() < Duration::from_secs(3)
        });
        if !fresh {
            let (status, commit) = tokio::join!(
                crate::app_state::workspace_project_git_status(&project.root_path),
                project_git_commit(&project.root_path),
            );
            cache.insert(
                id.clone(),
                CachedGit {
                    root: project.root_path.clone(),
                    checked_at: Instant::now(),
                    status,
                    commit,
                },
            );
        }
        let cached = &cache[&id];
        (cached.status.clone(), cached.commit.clone())
    };
    if let Some(status) = &status {
        project.git_branch = status.branch.clone();
        project.git_status = Some(if status.dirty {
            ProjectGitStatus::Dirty
        } else {
            ProjectGitStatus::Clean
        });
    }
    let participated = state.workspace_agent_participated_sessions().await;
    let task_sessions = data["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|task| task["project_id"] == id)
        .filter_map(|task| task["session_id"].as_str())
        .chain(
            data["dispatch_tasks"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|task| task["project_id"] == id)
                .filter_map(|task| task["session_id"].as_str()),
        )
        .collect::<HashSet<_>>();
    let mut sessions = state
        .list_sessions()
        .await
        .into_iter()
        .filter(|session| {
            session.project_id == id
                && !crate::models::is_internal_acceptance_session_id(&session.id)
                && (participated.contains(&session.id)
                    || task_sessions.contains(session.id.as_str()))
        })
        .collect::<Vec<_>>();
    sessions.sort_by(|a, b| b.updated_at.cmp(&a.updated_at).then(a.id.cmp(&b.id)));
    let total = sessions.len();
    let sessions = sessions.into_iter().take(12).map(|session| json!({
        "id":session.id,"project_id":session.project_id,"title":session.title,
        "status":session.status,"updated_at":session.updated_at,
        "preview":session.last_message_preview.map(|text| text.chars().take(300).collect::<String>()),
    })).collect::<Vec<_>>();
    data["project_context"] = json!({"project":project,"git_status":status,"commit":commit,"sessions":sessions,"total_sessions":total});
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_context_uses_only_the_current_conversation_and_explicit_scope() {
        let memory = Memory::default();
        let mut query = ScopeQuery {
            conversation_id: Some("a".into()),
            ..Default::default()
        };
        let data = json!({"dispatch_tasks":[
            {"conversation_id":"a","request_id":"a1","project_id":"alpha"},
            {"conversation_id":"b","request_id":"b1","project_id":"beta"},
        ]});
        assert_eq!(project_id(&memory, &query, &data).as_deref(), Some("alpha"));
        query.conversation_id = Some("new".into());
        assert_eq!(project_id(&memory, &query, &data), None);
        query.project_id = Some("beta".into());
        assert_eq!(project_id(&memory, &query, &data).as_deref(), Some("beta"));
        query.project_id = None;
        query.conversation_id = Some("a".into());
        let ambiguous = json!({"dispatch_tasks":[
            {"conversation_id":"a","request_id":"a1","project_id":"alpha"},
            {"conversation_id":"a","request_id":"a1","project_id":"beta"},
        ]});
        assert_eq!(project_id(&memory, &query, &ambiguous), None);
    }

    #[test]
    fn discussed_projects_must_be_real_and_obey_the_explicit_project_scope() {
        let available = vec![json!({"id":"alpha"}), json!({"id":"beta"})];
        let mut query = ScopeQuery::default();
        assert!(chat_project(&query, Some("missing"), &[], &[], &available).is_err());
        assert_eq!(
            chat_project(&query, Some("beta"), &[], &[], &available)
                .unwrap()
                .as_deref(),
            Some("beta")
        );
        let plans: Vec<dispatch::PlannedTask> = serde_json::from_value(json!([
            {"project_id":"alpha","title":"A","instruction":"Build A"},
            {"project_id":"beta","title":"B","instruction":"Build B"},
        ]))
        .unwrap();
        assert_eq!(
            chat_project(&query, None, &plans, &[], &available).unwrap(),
            None
        );
        assert_eq!(
            chat_project(&query, None, &plans[..1], &[], &available)
                .unwrap()
                .as_deref(),
            Some("alpha")
        );
        let references = vec![references::ComposerReference::File {
            project_id: "beta".into(),
            path: "src/main.rs".into(),
        }];
        assert_eq!(
            chat_project(&query, None, &[], &references, &available)
                .unwrap()
                .as_deref(),
            Some("beta")
        );
        query.project_id = Some("alpha".into());
        assert!(chat_project(&query, Some("beta"), &[], &[], &available).is_err());
    }

    #[tokio::test]
    async fn discussion_project_survives_restart_without_becoming_another_conversations_default() {
        let path = std::env::temp_dir().join(format!(
            "workspace-project-context-{}.json",
            uuid::Uuid::new_v4()
        ));
        let store = WorkspaceAgent::open(path.clone()).unwrap();
        let memory: Memory = serde_json::from_value(json!({
            "scopes":{},"analyses":{},"chats":{},
            "conversations":{"a":{"id":"a","title":"Discuss alpha","created_at":Utc::now(),
                "updated_at":Utc::now(),"configuration":{"selection":null,"agent":"pi"},"active_project_id":"alpha"}},
        })).unwrap();
        store.save(&memory).await.unwrap();
        let restored = WorkspaceAgent::open(path.clone()).unwrap();
        let memory = restored.memory.lock().await;
        let mut query = ScopeQuery {
            conversation_id: Some("a".into()),
            ..Default::default()
        };
        assert_eq!(
            project_id(&memory, &query, &json!({})).as_deref(),
            Some("alpha")
        );
        query.conversation_id = Some("b".into());
        assert_eq!(project_id(&memory, &query, &json!({})), None);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn git_context_reports_real_working_tree_counts_and_unknown_is_not_clean() {
        let root =
            std::env::temp_dir().join(format!("workspace-context-git-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(&root)
                .env("GIT_AUTHOR_NAME", "Test")
                .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
                .env("GIT_COMMITTER_NAME", "Test")
                .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            output.stdout
        };
        git(&["init", "-b", "main"]);
        std::fs::write(root.join("tracked.txt"), "initial").unwrap();
        git(&["add", "tracked.txt"]);
        git(&[
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "Initial delivery",
        ]);
        assert!(
            !crate::app_state::workspace_project_git_status(root.to_str().unwrap())
                .await
                .unwrap()
                .dirty
        );
        std::fs::write(root.join("tracked.txt"), "changed").unwrap();
        std::fs::write(root.join("staged.txt"), "staged").unwrap();
        git(&["add", "staged.txt"]);
        std::fs::write(root.join("untracked.txt"), "untracked").unwrap();
        let before = git(&["status", "--porcelain=v1"]);
        let status = crate::app_state::workspace_project_git_status(root.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(status.branch.as_deref(), Some("main"));
        assert!(status.dirty);
        assert_eq!(
            (
                status.staged_count,
                status.unstaged_count,
                status.untracked_count
            ),
            (1, 1, 1)
        );
        assert_eq!(before, git(&["status", "--porcelain=v1"]));
        assert_eq!(
            project_git_commit(root.to_str().unwrap())
                .await
                .unwrap()
                .subject,
            "Initial delivery"
        );
        assert!(
            crate::app_state::workspace_project_git_status(root.join("missing").to_str().unwrap())
                .await
                .is_none()
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
