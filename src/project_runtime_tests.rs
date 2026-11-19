use super::*;
use crate::{
    acceptance_test_support::Fixture, models::CreateProjectInput,
    project_detection::BuildConfiguration, project_environment::EnvironmentConfig,
};
use serde_json::{Value, json};

async fn setup(command: &str) -> (Fixture, ProjectSummary) {
    let fixture = Fixture::new().await;
    let root = fixture.root.join("project");
    std::fs::create_dir_all(root.join("api")).unwrap();
    std::fs::create_dir_all(root.join("client")).unwrap();
    std::fs::write(root.join("api/runtime-fixture"), "frozen fixture").unwrap();
    let project = fixture
        .state
        .create_project(CreateProjectInput {
            name: "Runtime fixture".into(),
            root_path: root.to_string_lossy().into_owned(),
        })
        .await;
    fixture
        .state
        .acceptance
        .environments
        .save_config(
            &project.id,
            EnvironmentConfig {
                build_targets: vec![
                    BuildTarget {
                        directory: "api".into(),
                        configuration: BuildConfiguration {
                            start_command: command.into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    BuildTarget {
                        directory: "client".into(),
                        configuration: BuildConfiguration {
                            start_command: command.into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            false,
        )
        .unwrap();
    (fixture, project)
}

async fn wait_for(fixture: &Fixture, project: &ProjectSummary, phase: Phase) -> Runtime {
    for _ in 0..100 {
        if let Some(record) = fixture
            .state
            .project_runtimes
            .overview(&fixture.state, project)
            .await
            .unwrap()
            .runtimes
            .into_iter()
            .find(|runtime| runtime.directory == "api")
        {
            if record.phase == phase {
                return record;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("Runtime did not reach {phase:?}");
}

#[tokio::test]
async fn manual_target_crud_preserves_source_and_excludes_deleted_profiles_from_detection() {
    let (fixture, project) = setup("sleep 60").await;
    let root = Path::new(&project.root_path);
    std::fs::create_dir_all(root.join("manual")).unwrap();
    std::fs::write(root.join("manual/source.txt"), "keep source").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = api::router()
        .merge(crate::acceptance_api::router())
        .with_state(fixture.state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();
    let url = format!("{origin}/v2/projects/{}/runtime", project.id);
    let input = json!({"create": true, "name": "Manual service", "target": {
        "directory": "manual", "configuration": {"start_command": "printf manual"}
    }});
    for (endpoint, body) in [
        ("targets", input.clone()),
        (
            "targets/rename",
            json!({"directory": "manual", "name": "Name"}),
        ),
        ("targets/delete", json!({"directory": "manual"})),
    ] {
        assert_eq!(
            client
                .post(format!("{url}/{endpoint}"))
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
    }
    assert_eq!(
        client
            .post(format!("{url}/targets"))
            .headers(fixture.headers.clone())
            .json(&input)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let store = &fixture.state.project_runtimes;
    store
        .save_location(&project.id, "manual", Location::Docker)
        .await
        .unwrap();
    assert_eq!(store.targets(&fixture.state, &project).unwrap().len(), 3);
    let detected: Value = client
        .post(format!(
            "{origin}/v2/projects/{}/environment/detect",
            project.id
        ))
        .headers(fixture.headers.clone())
        .json(&json!({"all_targets": true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        detected["data"]["targets"]
            .as_array()
            .unwrap()
            .iter()
            .any(|target| target["directory"] == "manual")
    );
    let renamed = client
        .post(format!("{url}/targets/rename"))
        .headers(fixture.headers.clone())
        .json(&json!({"directory": "manual", "name": "Renamed service"}))
        .send()
        .await
        .unwrap();
    assert_eq!(renamed.status(), 200);
    let restored = ProjectRuntimeStore::new(&fixture.state.runtime_plugins);
    let preferences = restored.preferences(&project.id).unwrap();
    assert_eq!(preferences.names["manual"], "Renamed service");
    assert_eq!(preferences.locations["manual"], Location::Docker);
    assert_eq!(
        preferences.targets["manual"].configuration.start_command,
        "printf manual"
    );
    let edited = json!({"name": "Renamed service", "target": {
        "directory": "manual", "configuration": {"start_command": "printf edited"}
    }});
    assert_eq!(
        client
            .post(format!("{url}/targets"))
            .headers(fixture.headers.clone())
            .json(&edited)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        store
            .targets(&fixture.state, &project)
            .unwrap()
            .iter()
            .find(|target| target.directory == "manual")
            .unwrap()
            .configuration
            .start_command,
        "printf edited"
    );
    for directory in ["manual", "./manual/", "../outside", "/tmp", "missing"] {
        let mut invalid = input.clone();
        invalid["target"]["directory"] = json!(directory);
        assert_eq!(
            client
                .post(format!("{url}/targets"))
                .headers(fixture.headers.clone())
                .json(&invalid)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&fixture.root, root.join("outside-link")).unwrap();
        let mut invalid = input.clone();
        invalid["target"]["directory"] = json!("outside-link");
        assert_eq!(
            client
                .post(format!("{url}/targets"))
                .headers(fixture.headers.clone())
                .json(&invalid)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    // Make the manual directory discoverable before deletion: tombstones must
    // suppress both saved profiles and a subsequent scan of project manifests.
    std::fs::write(
        root.join("manual/package.json"),
        r#"{"packageManager":"npm@10","scripts":{"start":"node index.js"}}"#,
    )
    .unwrap();
    let stale_config = fixture
        .state
        .acceptance
        .environments
        .config(&project.id)
        .unwrap();
    assert_eq!(
        client
            .post(format!("{url}/targets/delete"))
            .headers(fixture.headers.clone())
            .json(&json!({"directory": "manual"}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        std::fs::read_to_string(root.join("manual/source.txt")).unwrap(),
        "keep source"
    );
    let preferences = restored.preferences(&project.id).unwrap();
    assert!(preferences.excluded.contains("manual"));
    assert!(
        store
            .save_location(&project.id, "manual", Location::Host)
            .await
            .is_err()
    );
    assert!(!preferences.names.contains_key("manual"));
    assert!(!preferences.locations.contains_key("manual"));
    let mut detection = detect(root);
    store
        .prepare_detection(&fixture.state, &project, &mut detection)
        .unwrap();
    assert!(
        !detection
            .candidates
            .iter()
            .any(|candidate| candidate.directory == "manual")
    );
    let stale_targets = stale_config.build_targets.clone();
    store
        .remember_targets(&project.id, &stale_targets)
        .await
        .unwrap();
    let mut stale = serde_json::to_value(stale_config).unwrap();
    stale.as_object_mut().unwrap().remove("generation");
    // Use only fields accepted by the public save request.
    let stale = json!({"build_targets": stale["build_targets"], "build": stale["build"]});
    assert_eq!(
        client
            .post(format!("{origin}/v2/projects/{}/environment", project.id))
            .headers(fixture.headers.clone())
            .json(&stale)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(
        !store
            .targets(&fixture.state, &project)
            .unwrap()
            .iter()
            .any(|target| target.directory == "manual")
    );
    for directory in ["api", "client"] {
        store
            .delete_target(&fixture.state, &project, directory)
            .await
            .unwrap();
    }
    let overview = store.overview(&fixture.state, &project).await.unwrap();
    assert!(overview.targets.is_empty());
    assert!(overview.runtimes.is_empty());
    assert_eq!(
        client
            .post(format!("{url}/targets"))
            .headers(fixture.headers.clone())
            .json(&input)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(
        !restored
            .preferences(&project.id)
            .unwrap()
            .excluded
            .contains("manual")
    );
    assert_eq!(store.targets(&fixture.state, &project).unwrap().len(), 1);
    server.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn active_targets_can_be_renamed_and_edited_but_only_deleted_after_stopping() {
    let (fixture, project) = setup("sleep 60").await;
    let store = &fixture.state.project_runtimes;
    let started = store
        .start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            None,
            "test".into(),
        )
        .await
        .unwrap();
    wait_for(&fixture, &project, Phase::Running).await;
    let lease = store
        .claim(&fixture.state, &project.id, "api", "agent", None)
        .await
        .unwrap();
    store
        .rename_target(&fixture.state, &project, "api", "New display name")
        .await
        .unwrap();
    let mut edited = store
        .targets(&fixture.state, &project)
        .unwrap()
        .into_iter()
        .find(|target| target.directory == "api")
        .unwrap();
    edited.configuration.start_command = "printf next-start".into();
    store
        .save_target(
            &fixture.state,
            &project,
            edited,
            "New display name".into(),
            false,
        )
        .await
        .unwrap();
    let error = store
        .delete_target(&fixture.state, &project, "api")
        .await
        .unwrap_err();
    assert_eq!(api::error(error).status, axum::http::StatusCode::CONFLICT);
    let running = store
        .overview(&fixture.state, &project)
        .await
        .unwrap()
        .runtimes
        .into_iter()
        .find(|record| record.directory == "api")
        .unwrap();
    assert_eq!(running.id, started.id);
    assert_eq!(running.phase, Phase::Running);
    assert_eq!(running.command, "sleep 60");
    assert_eq!(running.lease.unwrap().id, lease.id);
    store
        .release(&project.id, "api", "agent", &lease.id)
        .await
        .unwrap();
    store
        .stop(&fixture.state, &project.id, "api")
        .await
        .unwrap();
    store
        .delete_target(&fixture.state, &project, "api")
        .await
        .unwrap();
    let overview = store.overview(&fixture.state, &project).await.unwrap();
    assert!(
        !overview
            .targets
            .iter()
            .any(|target| target.directory == "api")
    );
    assert!(
        !overview
            .runtimes
            .iter()
            .any(|runtime| runtime.directory == "api")
    );
    assert!(
        Path::new(&project.root_path)
            .join("api/runtime-fixture")
            .exists()
    );
}

#[tokio::test]
async fn location_preferences_survive_reload_and_are_isolated_by_project_and_subproject() {
    let (fixture, project) = setup("sleep 60").await;
    let store = &fixture.state.project_runtimes;
    store
        .save_location(&project.id, "api", Location::Docker)
        .await
        .unwrap();
    store
        .save_location(&project.id, "client", Location::Host)
        .await
        .unwrap();
    let restored = ProjectRuntimeStore::new(&fixture.state.runtime_plugins);
    let preferences = restored.preferences(&project.id).unwrap();
    assert_eq!(preferences.locations["api"], Location::Docker);
    assert_eq!(preferences.locations["client"], Location::Host);
    assert!(
        restored
            .preferences("other-project")
            .unwrap()
            .locations
            .is_empty()
    );
    assert!(
        store
            .save_location(&project.id, "../escape", Location::Host)
            .await
            .is_err()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn host_lifecycle_exclusive_leases_expiration_and_fencing() {
    let (fixture, project) = setup("printf 'runtime started\n'; exec sleep 60").await;
    let store = &fixture.state.project_runtimes;
    let record = store
        .start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            None,
            "test".into(),
        )
        .await
        .unwrap();
    assert_eq!(record.location, Location::Host);
    wait_for(&fixture, &project, Phase::Running).await;
    assert!(
        store
            .start(
                fixture.state.clone(),
                project.clone(),
                "api".into(),
                None,
                "test".into()
            )
            .await
            .is_err()
    );
    let (first, second) = tokio::join!(
        store.claim(&fixture.state, &project.id, "api", "agent-a", None),
        store.claim(&fixture.state, &project.id, "api", "agent-b", None)
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let lease = first.or(second).unwrap();
    assert!(
        store
            .stop(&fixture.state, &project.id, "api")
            .await
            .is_err()
    );
    assert!(
        store
            .release(&project.id, "api", "wrong-agent", &lease.id)
            .await
            .is_err()
    );
    let result = store
        .execute(
            &fixture.state,
            &project.id,
            "api",
            &lease.session_id,
            &lease.id,
            "printf '%s' \"$PWD\"",
        )
        .await
        .unwrap();
    assert_eq!(result["success"], true);
    assert_eq!(result["output"], project.root_path);
    assert!(
        store
            .logs(&fixture.state, &project.id, "api")
            .await
            .unwrap()
            .contains("runtime started")
    );
    let entry = store.entry(&project.id, "api").await.unwrap().unwrap();
    entry.lock().await.record.lease.as_mut().unwrap().expires_at =
        Utc::now() - chrono::Duration::seconds(1);
    let new_lease = store
        .claim(&fixture.state, &project.id, "api", "agent-c", None)
        .await
        .unwrap();
    assert_ne!(lease.id, new_lease.id);
    assert!(
        store
            .execute(
                &fixture.state,
                &project.id,
                "api",
                &lease.session_id,
                &lease.id,
                "touch stale-owner"
            )
            .await
            .is_err()
    );
    assert!(!Path::new(&project.root_path).join("stale-owner").exists());
    store
        .release(&project.id, "api", "agent-c", &new_lease.id)
        .await
        .unwrap();
    assert_eq!(
        store
            .stop(&fixture.state, &project.id, "api")
            .await
            .unwrap()
            .phase,
        Phase::Stopped
    );
}

#[cfg(unix)]
#[tokio::test]
async fn status_and_conflict_checks_remain_responsive_during_a_test() {
    let (fixture, project) = setup("exec sleep 60").await;
    let store = &fixture.state.project_runtimes;
    store
        .start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            None,
            "test".into(),
        )
        .await
        .unwrap();
    wait_for(&fixture, &project, Phase::Running).await;
    let lease = store
        .claim(&fixture.state, &project.id, "api", "test-agent", None)
        .await
        .unwrap();
    let worker_state = fixture.state.clone();
    let project_id = project.id.clone();
    let lease_id = lease.id.clone();
    let test = tokio::spawn(async move {
        worker_state
            .project_runtimes
            .execute(
                &worker_state,
                &project_id,
                "api",
                "test-agent",
                &lease_id,
                "sleep 1.5; printf finished",
            )
            .await
            .unwrap()
    });
    for _ in 0..50 {
        let entry = store.entry(&project.id, "api").await.unwrap().unwrap();
        if test_busy(&entry.lock().await.record) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let view = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        store.overview(&fixture.state, &project),
    )
    .await
    .expect("Status blocked behind test execution")
    .unwrap();
    assert_eq!(
        view.runtimes[0].lease.as_ref().unwrap().session_id,
        "test-agent"
    );
    assert!(
        store
            .claim(&fixture.state, &project.id, "api", "other-agent", None)
            .await
            .is_err()
    );
    assert!(
        store
            .release(&project.id, "api", "test-agent", &lease.id)
            .await
            .is_err()
    );
    assert_eq!(test.await.unwrap()["output"], "finished");
    store
        .release(&project.id, "api", "test-agent", &lease.id)
        .await
        .unwrap();
    store
        .stop(&fixture.state, &project.id, "api")
        .await
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn concurrent_start_and_stop_during_start_do_not_spawn_duplicate_services() {
    let (fixture, project) = setup("exec sleep 60").await;
    let store = &fixture.state.project_runtimes;
    let (a, b) = tokio::join!(
        store.start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            None,
            "test".into()
        ),
        store.start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            None,
            "test".into()
        )
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    store
        .stop(&fixture.state, &project.id, "api")
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let overview = store.overview(&fixture.state, &project).await.unwrap();
    assert_eq!(overview.runtimes[0].phase, Phase::Stopped);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn restored_host_checks_process_identity_and_detects_exit() {
    let (fixture, project) = setup("exec sleep 60").await;
    let store = &fixture.state.project_runtimes;
    store
        .start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            None,
            "test".into(),
        )
        .await
        .unwrap();
    wait_for(&fixture, &project, Phase::Running).await;
    let restored = ProjectRuntimeStore::new(&fixture.state.runtime_plugins);
    let overview = restored.overview(&fixture.state, &project).await.unwrap();
    assert_eq!(overview.runtimes[0].phase, Phase::Running);
    restored
        .stop(&fixture.state, &project.id, "api")
        .await
        .unwrap();
    let record = wait_for(&fixture, &project, Phase::Failed).await;
    assert!(record.lease.is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn a_docker_start_without_an_installed_plugin_can_be_retried_on_host() {
    let (fixture, project) = setup("exec sleep 60").await;
    let store = &fixture.state.project_runtimes;
    store
        .start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            Some(Location::Docker),
            "test".into(),
        )
        .await
        .unwrap();
    wait_for(&fixture, &project, Phase::Failed).await;
    store
        .start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            Some(Location::Host),
            "test".into(),
        )
        .await
        .unwrap();
    wait_for(&fixture, &project, Phase::Running).await;
    store
        .stop(&fixture.state, &project.id, "api")
        .await
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn failed_command_exposes_exit_and_logs_without_leaving_a_running_status() {
    let (fixture, project) = setup("echo startup-error >&2; exit 7").await;
    let store = &fixture.state.project_runtimes;
    store
        .start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            None,
            "test".into(),
        )
        .await
        .unwrap();
    let failed = wait_for(&fixture, &project, Phase::Failed).await;
    assert!(failed.error.unwrap().contains('7'));
    assert!(
        store
            .logs(&fixture.state, &project.id, "api")
            .await
            .unwrap()
            .contains("startup-error")
    );
}

#[tokio::test]
async fn runtime_http_requires_authentication_and_shares_environment_settings_with_acceptance() {
    let (fixture, project) = setup("sleep 60").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = api::router()
        .merge(crate::acceptance_api::router())
        .with_state(fixture.state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();
    let url = format!("{origin}/v2/projects/{}/runtime", project.id);
    assert_eq!(client.get(&url).send().await.unwrap().status(), 401);
    let response: Value = client
        .get(&url)
        .headers(fixture.headers.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["data"]["targets"].as_array().unwrap().len(), 2);
    assert_eq!(
        client
            .post(&url)
            .headers(fixture.headers.clone())
            .json(&json!({"directory":"api","location":"docker"}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(&url)
            .headers(fixture.headers.clone())
            .json(&json!({"directory":"missing","location":"host"}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let env_url = format!("{origin}/v2/projects/{}/environment", project.id);
    let generic: Value = client
        .get(&env_url)
        .headers(fixture.headers.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let acceptance: Value = client
        .get(format!(
            "{origin}/v2/projects/{}/acceptance-environment",
            project.id
        ))
        .headers(fixture.headers.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(generic["data"]["config"], acceptance["data"]["config"]);
    let mut subset = acceptance["data"]["config"].clone();
    subset.as_object_mut().unwrap().remove("generation");
    subset["build_targets"].as_array_mut().unwrap().truncate(1);
    assert_eq!(
        client
            .post(format!(
                "{origin}/v2/projects/{}/acceptance-environment",
                project.id
            ))
            .headers(fixture.headers.clone())
            .json(&subset)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let restored: Value = client
        .get(&url)
        .headers(fixture.headers.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        restored["data"]["targets"].as_array().unwrap().len(),
        2,
        "Acceptance scope must not delete other subproject profiles"
    );
    let restored: Value = client
        .get(&env_url)
        .headers(fixture.headers.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        restored["data"]["config"]["build_targets"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        client
            .get(format!("{origin}/v2/projects/missing/runtime"))
            .headers(fixture.headers.clone())
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    server.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn agent_capability_is_session_scoped_and_requires_a_lease_for_execution() {
    let (fixture, project) = setup("exec sleep 60").await;
    let store = &fixture.state.project_runtimes;
    let mut connections = Vec::new();
    for session in ["runtime-agent-a", "runtime-agent-b"] {
        fixture
            .state
            .create_session(
                serde_json::from_value(
                    json!({"client_session_id":session,"project_id":project.id,"agent":"pi"}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let instructions = store
            .agent_instructions(&fixture.state, session, &project.id)
            .await
            .unwrap()
            .unwrap();
        assert!(instructions.contains("project-runtime --context"));
        assert!(instructions.contains("frozen source snapshot"));
        let path = store
            .root
            .join("agent-connections")
            .join(format!("{}.json", key(session)));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        connections.push(serde_json::from_slice::<Value>(&std::fs::read(path).unwrap()).unwrap());
    }
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let url = connections[0]["url"].as_str().unwrap();
    assert_eq!(
        client
            .post(url)
            .json(&json!({"action":"status"}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let token_a = connections[0]["token"].as_str().unwrap();
    let token_b = connections[1]["token"].as_str().unwrap();
    assert_ne!(token_a, token_b);
    store
        .start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            None,
            "test".into(),
        )
        .await
        .unwrap();
    wait_for(&fixture, &project, Phase::Running).await;
    let status: Value = client
        .post(url)
        .bearer_auth(token_a)
        .json(&json!({"action":"status"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["data"]["runtimes"][0]["project_id"], project.id);
    let lease: Value = client
        .post(url)
        .bearer_auth(token_a)
        .json(&json!({"action":"claim","directory":"api"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let lease_id = lease["data"]["id"].as_str().unwrap();
    assert_eq!(
        client
            .post(url)
            .bearer_auth(token_b)
            .json(&json!({"action":"claim","directory":"api"}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(client.post(url).bearer_auth(token_b).json(&json!({"action":"exec","directory":"api","lease":lease_id,"command":"touch wrong-agent"})).send().await.unwrap().status(), 409);
    let executed: Value = client.post(url).bearer_auth(token_a).json(&json!({"action":"exec","directory":"api","lease":lease_id,"command":"printf capability-ok"})).send().await.unwrap().json().await.unwrap();
    assert_eq!(executed["data"]["output"], "capability-ok");
    assert!(!Path::new(&project.root_path).join("wrong-agent").exists());
    assert_eq!(
        client
            .post(url)
            .bearer_auth(token_a)
            .json(&json!({"action":"release","directory":"api","lease":lease_id}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    store
        .stop(&fixture.state, &project.id, "api")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires OMNI_CODE_TEST_RUNTIME_IMAGE naming an existing compatible local x11box image"]
async fn real_docker_runtime_start_lease_execute_and_stop() -> Result<()> {
    let image = std::env::var("OMNI_CODE_TEST_RUNTIME_IMAGE")
        .context("set OMNI_CODE_TEST_RUNTIME_IMAGE")?;
    let (fixture, project) = setup("sleep 120").await;
    let runtime = &fixture.state.runtime_plugins;
    let image_id = runtime
        .docker(&["image", "inspect", "--format", "{{.Id}}", &image], 15)
        .await?
        .trim()
        .to_owned();
    atomic_json(
        &runtime.root.join("installation.json"),
        &crate::runtime_plugin::PluginInstallation {
            phase: crate::runtime_plugin::InstallPhase::Ready,
            image_id: Some(image_id),
            ..Default::default()
        },
    )?;
    let mut config = fixture.state.acceptance.environments.config(&project.id)?;
    config.mode = crate::project_environment::EnvironmentMode::Base;
    fixture
        .state
        .project_runtimes
        .remember_targets(&project.id, &config.build_targets)
        .await?;
    // The acceptance selection points at another component. Manual startup
    // must still use the requested component's persisted profile.
    config
        .build_targets
        .retain(|target| target.directory == "client");
    fixture
        .state
        .acceptance
        .environments
        .save_config(&project.id, config, false)?;
    let store = &fixture.state.project_runtimes;
    let initial = store
        .start(
            fixture.state.clone(),
            project.clone(),
            "api".into(),
            Some(Location::Docker),
            "test".into(),
        )
        .await?;
    let id = initial.acceptance_run_id.as_deref().unwrap();
    let result: Result<()> = async {
        tokio::time::timeout(std::time::Duration::from_secs(150), async {
            loop {
                let overview = store.overview(&fixture.state, &project).await?;
                let record = &overview.runtimes[0];
                ensure!(
                    record.phase != Phase::Failed,
                    "Docker startup failed: {:?}",
                    record.error
                );
                if record.phase == Phase::Running {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("Docker startup timed out")??;
        let run = fixture.state.acceptance.snapshot(id).await?;
        let selected = &run
            .environment
            .context("Docker run has no environment")?
            .config
            .build_targets;
        ensure!(
            selected.len() == 1 && selected[0].directory == "api",
            "Manual Docker startup used the acceptance target selection"
        );
        let lease = store
            .claim(&fixture.state, &project.id, "api", "docker-agent", None)
            .await?;
        let output = store
            .execute(
                &fixture.state,
                &project.id,
                "api",
                "docker-agent",
                &lease.id,
                "test -f /workspace/api/runtime-fixture && printf docker-runtime-ok",
            )
            .await?;
        ensure!(
            output["success"] == true && output["output"] == "docker-runtime-ok",
            "Docker exec failed: {output}"
        );
        ensure!(
            store
                .claim(&fixture.state, &project.id, "api", "other-agent", None)
                .await
                .is_err(),
            "Lease was not exclusive"
        );
        store
            .release(&project.id, "api", "docker-agent", &lease.id)
            .await?;
        ensure!(
            store.stop(&fixture.state, &project.id, "api").await?.phase == Phase::Stopped,
            "Docker runtime did not stop"
        );
        Ok(())
    }
    .await;
    // Clean up this test's own desktop and network even when validation fails.
    let _ = fixture.state.acceptance.stop(id, None).await;
    result
}
