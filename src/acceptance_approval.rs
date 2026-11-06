//! Offline approval of routine operations bound to a live, confirmed acceptance.
//! Unknown shell syntax and operations continue through the full reviewer.
use crate::{
    ai_approval::ApprovalContext,
    models::{ApprovalKind, ApprovalRequest},
};
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq)]
enum Token {
    Word(String),
    Separator,
    Redirect,
    StderrToStdout,
}

pub(crate) fn permits(request: &ApprovalRequest, root: &Path, context: &ApprovalContext) -> bool {
    if !context.plan_confirmed
        || !matches!(context.phase.as_str(), "preparing" | "executing")
        || !matches!(
            request.kind,
            ApprovalKind::CommandExecution | ApprovalKind::ExecCommand
        )
    {
        return false;
    }
    let Some(cwd) = request.cwd.as_deref().map(Path::new) else {
        return false;
    };
    if !within(cwd, root) {
        return false;
    }
    request
        .command
        .as_deref()
        .is_some_and(|command| script(command, cwd, root, 0))
}

fn script(command: &str, cwd: &Path, root: &Path, depth: usize) -> bool {
    if depth > 6 || command.len() > 16_384 {
        return false;
    }
    let Some(tokens) = lex(command) else {
        return false;
    };
    let mut words = Vec::new();
    let mut iter = tokens.iter();
    while let Some(token) = iter.next() {
        match token {
            Token::Word(word) => words.push(word.as_str()),
            Token::Redirect => {
                let Some(Token::Word(path)) = iter.next() else {
                    return false;
                };
                if !path.ends_with(".log") || !within(&cwd.join(path), root) {
                    return false;
                }
            }
            Token::StderrToStdout => {}
            Token::Separator => {
                if !simple(&words, cwd, root, depth) {
                    return false;
                }
                words.clear();
            }
        }
    }
    simple(&words, cwd, root, depth)
}

fn simple(words: &[&str], cwd: &Path, root: &Path, depth: usize) -> bool {
    if depth > 6 {
        return false;
    }
    let Some((program, args)) = words.split_first() else {
        return false;
    };
    let Some(name) = trusted_program(program) else {
        return false;
    };
    match name {
        "sh" | "bash" | "zsh" if args.len() == 2 && matches!(args[0], "-c" | "-lc") => {
            script(args[1], cwd, root, depth + 1)
        }
        "timeout" => {
            let Some((duration, rest)) = args.split_first() else {
                return false;
            };
            !duration.is_empty()
                && duration
                    .chars()
                    .all(|c| c.is_ascii_digit() || matches!(c, '.' | 's' | 'm' | 'h'))
                && simple(rest, cwd, root, depth + 1)
        }
        "nix" if args.first() == Some(&"develop") => {
            let Some(index) = args.iter().position(|a| *a == "--command" || *a == "-c") else {
                return false;
            };
            args[1..index].iter().all(|a| {
                matches!(*a, "--no-write-lock-file" | "--offline")
                    || (!a.starts_with('-') && flake_in_project(a, cwd, root))
            }) && simple(&args[index + 1..], cwd, root, depth + 1)
        }
        "nix-shell" if args.len() >= 2 && args[args.len() - 2] == "--run" => {
            args[..args.len() - 2]
                .iter()
                .all(|a| *a == "--pure" || (!a.starts_with('-') && within(&cwd.join(a), root)))
                && script(args[args.len() - 1], cwd, root, depth + 1)
        }
        "flutter" => {
            build_args(args, &["build", "analyze", "test", "--version"], cwd, root)
                || (args.first() == Some(&"pub")
                    && args.get(1) == Some(&"get")
                    && scoped_args(&args[2..], cwd, root))
        }
        "dart" => {
            build_args(args, &["analyze", "test", "--version"], cwd, root)
                || (args.first() == Some(&"pub")
                    && args.get(1) == Some(&"get")
                    && scoped_args(&args[2..], cwd, root))
        }
        "go" => build_args(args, &["build", "test", "vet", "list"], cwd, root),
        "python" | "python3" => {
            args.first() == Some(&"-m")
                && args.get(1) == Some(&"build")
                && scoped_args(&args[2..], cwd, root)
        }
        "dotnet" => build_args(args, &["build", "test", "restore"], cwd, root),
        "cargo" => build_args(
            args,
            &["build", "check", "test", "clippy", "metadata", "fetch"],
            cwd,
            root,
        ),
        "npm" | "pnpm" | "yarn" | "bun" => {
            build_args(
                args,
                &["ci", "install", "build", "test", "typecheck", "lint"],
                cwd,
                root,
            ) || (args.first() == Some(&"run")
                && args
                    .get(1)
                    .is_some_and(|s| matches!(*s, "build" | "test" | "typecheck" | "lint"))
                && scoped_args(&args[2..], cwd, root))
        }
        "pwd" | "uname" => args.iter().all(|a| a.starts_with('-')),
        "command" => {
            args.first() == Some(&"-v") && args[1..].iter().all(|a| trusted_program(a).is_some())
        }
        "file" | "readelf" | "ldd" | "stat" | "ls" | "head" | "tail" | "wc" => {
            scoped_args(args, cwd, root)
        }
        "nix-store" => {
            matches!(args.first(), Some(&"-q") | Some(&"--query"))
                && args[1..].iter().all(|a| {
                    matches!(*a, "--requisites" | "--references" | "--deriver") || nix_store_path(a)
                })
        }
        _ => false,
    }
}

fn build_args(args: &[&str], subcommands: &[&str], cwd: &Path, root: &Path) -> bool {
    args.first().is_some_and(|a| subcommands.contains(a)) && scoped_args(&args[1..], cwd, root)
}

fn scoped_args(args: &[&str], cwd: &Path, root: &Path) -> bool {
    args.iter().all(|arg| {
        if matches!(
            arg.split('=').next().unwrap_or_default(),
            "--global"
                | "-g"
                | "--prefix"
                | "--userconfig"
                | "--script-shell"
                | "--exec"
                | "--eval"
                | "--fix"
                | "--config"
                | "--config-env"
        ) || arg.starts_with("-g")
            || (arg.starts_with("-c") && !arg.starts_with("--"))
        {
            return false;
        }
        let value = arg.split_once('=').map_or(*arg, |(_, value)| value);
        if value.chars().any(|c| matches!(c, '*' | '?' | '[' | ']')) {
            return false;
        }
        if Path::new(value).components().any(|part| {
            let name = part.as_os_str().to_string_lossy().to_ascii_lowercase();
            name.starts_with(".env")
                || matches!(
                    name.as_str(),
                    ".ssh" | ".aws" | ".codex" | "client-auth.json" | "credentials.json"
                )
                || name.ends_with(".pem")
                || name.ends_with(".key")
        }) {
            return false;
        }
        if value.starts_with('/') || value.starts_with('.') || value.contains('/') {
            within(&cwd.join(value), root) || nix_store_path(value)
        } else {
            !value.starts_with('~')
                && !value.contains(':')
                && !value.contains("..")
                && !matches!(
                    *arg,
                    "--global"
                        | "-g"
                        | "--prefix"
                        | "--userconfig"
                        | "--script-shell"
                        | "--exec"
                        | "--eval"
                )
        }
    })
}

fn flake_in_project(arg: &str, cwd: &Path, root: &Path) -> bool {
    let path = arg
        .strip_prefix("path:")
        .unwrap_or(arg)
        .split('#')
        .next()
        .unwrap_or_default();
    !path.contains(':') && within(&cwd.join(path), root)
}

fn nix_store_path(value: &str) -> bool {
    Path::new(value).starts_with("/nix/store") && !value.split('/').any(|p| p == "..")
}

fn trusted_program(program: &str) -> Option<&str> {
    let path = Path::new(program);
    let name = path.file_name()?.to_str()?;
    if !program.contains('/') {
        return Some(name);
    }
    let parent = path.parent()?;
    (matches!(
        parent.to_str()?,
        "/bin" | "/usr/bin" | "/usr/local/bin" | "/run/current-system/sw/bin"
    ) || (nix_store_path(program) && parent.ends_with("bin")))
    .then_some(name)
}

fn within(path: &Path, root: &Path) -> bool {
    fn resolve(path: &Path) -> Option<PathBuf> {
        let mut result = PathBuf::new();
        for part in path.components() {
            match part {
                std::path::Component::RootDir => result.push("/"),
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    if !result.pop() {
                        return None;
                    }
                }
                std::path::Component::Normal(value) => {
                    result.push(value);
                    if result.is_symlink() {
                        result = result.canonicalize().ok()?;
                    }
                }
                _ => return None,
            }
        }
        Some(result)
    }
    resolve(path)
        .zip(resolve(root))
        .is_some_and(|(path, root)| path.starts_with(root))
}

// Expansions, input redirects, background jobs and pipelines need AI review.
fn lex(input: &str) -> Option<Vec<Token>> {
    let mut result = Vec::new();
    let mut word = String::new();
    let mut active_word = false;
    let mut quote = None;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        match quote {
            Some('\'') => {
                if ch == '\'' {
                    quote = None;
                } else {
                    word.push(ch);
                }
            }
            Some('"') => match ch {
                '"' => quote = None,
                '$' | '\x60' => return None,
                '\\' => {
                    let next = chars.next()?;
                    if matches!(next, '$' | '\x60') {
                        return None;
                    }
                    word.push(next);
                }
                _ => word.push(ch),
            },
            _ => match ch {
                '\'' | '"' => {
                    quote = Some(ch);
                    active_word = true;
                }
                '\\' => {
                    word.push(chars.next()?);
                    active_word = true;
                }
                '$' | '\x60' | '<' | '|' | '(' | ')' | '{' | '}' | '\0' => return None,
                '&' | ';' | '\n' => {
                    if active_word {
                        result.push(Token::Word(std::mem::take(&mut word)));
                        active_word = false;
                    }
                    if ch == '&' && chars.next()? != '&' {
                        return None;
                    }
                    result.push(Token::Separator);
                }
                '>' => {
                    if word == "2" && chars.peek() == Some(&'&') {
                        chars.next();
                        if chars.next()? != '1' {
                            return None;
                        }
                        if chars
                            .peek()
                            .is_some_and(|c| !c.is_whitespace() && !matches!(c, ';' | '&'))
                        {
                            return None;
                        }
                        word.clear();
                        active_word = false;
                        result.push(Token::StderrToStdout);
                    } else {
                        if active_word {
                            result.push(Token::Word(std::mem::take(&mut word)));
                            active_word = false;
                        }
                        if chars.peek() == Some(&'>') {
                            chars.next();
                        }
                        result.push(Token::Redirect);
                    }
                }
                c if c.is_whitespace() => {
                    if active_word {
                        result.push(Token::Word(std::mem::take(&mut word)));
                        active_word = false;
                    }
                }
                '#' if !active_word => return None,
                _ => {
                    word.push(ch);
                    active_word = true;
                }
            },
        }
    }
    if quote.is_some() {
        return None;
    }
    if active_word {
        result.push(Token::Word(word));
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn context() -> ApprovalContext {
        ApprovalContext {
            run_id: "test".into(),
            phase: "preparing".into(),
            plan_confirmed: true,
            scope: serde_json::json!({"cases":["test app"]}),
            container: "omni-acceptance-test".into(),
            image: "test-image".into(),
        }
    }
    fn request(command: &str) -> ApprovalRequest {
        serde_json::from_value(serde_json::json!({
        "request_id":"test", "cwd":"/project/.cache/run/source/app", "kind":"command_execution", "command":command,
        "allow_accept_for_session":false,"allow_cancel":true,"resolvable":true})).unwrap()
    }
    #[test]
    fn acceptance_builds_and_local_logs_work_without_remote_review() {
        for command in [
            "timeout 600 nix develop path:./ --no-write-lock-file --command flutter build linux --release -v > /project/.cache/run/build.log 2>&1",
            "/run/current-system/sw/bin/zsh -lc 'timeout 600 nix develop path:./ --no-write-lock-file --command flutter build linux --release -v > /project/.cache/run/build.log 2>&1'",
            "nix develop path:./ --no-write-lock-file --command bash -c 'flutter --version; flutter build linux --release'",
            "nix-shell shell.nix --run 'cargo build --release'",
            "cargo test --manifest-path ./Cargo.toml",
            "python3 -m build --outdir ./dist",
            "go build ./cmd/app",
            "npm ci",
            "pnpm run build",
            "readelf -l ./build/app",
            "nix-store -q --requisites /nix/store/abc-package",
        ] {
            assert!(
                permits(&request(command), Path::new("/project"), &context()),
                "{command}"
            );
        }
    }
    #[test]
    fn scope_shell_payload_and_sensitive_paths_are_checked_in_full() {
        for command in [
            "flutter build linux; rm -rf /home/user",
            "flutter build linux && curl https://example.com",
            "flutter build linux > /etc/profile",
            "flutter build linux > /project/src/main.rs",
            "flutter build linux $(rm -rf /home/user)",
            "flutter build linux | sh",
            "flutter build linux & touch /tmp/marker",
            "bash -c 'cargo build; bash -c \"curl https://example.com\"'",
            "PATH=/tmp flutter build linux",
            "./flutter build linux",
            "/tmp/flutter build linux",
            "npm install -g something",
            "npm install --prefix=/project/other something",
            "head /project/.env",
            "head /project/client-auth.json",
            "cargo fmt",
            "cargo clippy --fix",
            "cargo build --config build.rustc-wrapper=/nix/store/package/bin/sh",
            "npm install -gf",
            "npm run lint -- --fix",
            "cargo build --manifest-path /outside/Cargo.toml",
            "nix develop github:other/repo --command cargo build",
        ] {
            assert!(
                !permits(&request(command), Path::new("/project"), &context()),
                "{command}"
            );
        }
        let mut ctx = context();
        ctx.plan_confirmed = false;
        assert!(!permits(
            &request("cargo build"),
            Path::new("/project"),
            &ctx
        ));
        ctx.plan_confirmed = true;
        ctx.phase = "planning".into();
        assert!(!permits(
            &request("cargo build"),
            Path::new("/project"),
            &ctx
        ));
        let mut req = request("cargo build");
        req.cwd = None;
        assert!(!permits(&req, Path::new("/project"), &context()));
        req.cwd = Some("/outside".into());
        assert!(!permits(&req, Path::new("/project"), &context()));
    }
    #[test]
    fn output_symlinks_cannot_escape_project() {
        let root =
            std::env::temp_dir().join(format!("acceptance-approval-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        std::os::unix::fs::symlink("/etc/profile", root.join("output.log")).unwrap();
        let mut req = request("cargo build > output.log");
        req.cwd = Some(root.to_string_lossy().into());
        assert!(!permits(&req, &root, &context()));
        std::fs::remove_dir_all(root).unwrap();
    }
}
