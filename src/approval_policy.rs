use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoApprovalDecision {
    Accept,
}

pub fn should_auto_approve(command: &str, project_root: &Path) -> Option<AutoApprovalDecision> {
    let command = command.trim();
    if command.is_empty() || contains_shell_control(command) {
        return None;
    }

    let tokens = shell_split(command)?;
    let (program, args) = split_command(&tokens)?;

    if is_safe_read_only_command(program, args) {
        return Some(AutoApprovalDecision::Accept);
    }

    if is_safe_project_command(program, args) {
        return Some(AutoApprovalDecision::Accept);
    }

    if is_project_scoped_file_op(program, args, project_root) {
        return Some(AutoApprovalDecision::Accept);
    }

    None
}

fn split_command<'a>(tokens: &'a [String]) -> Option<(&'a str, &'a [String])> {
    let mut index = 0;
    while index < tokens.len() && is_env_assignment(&tokens[index]) {
        index += 1;
    }
    let program = tokens.get(index)?.as_str();
    Some((program, &tokens[index + 1..]))
}

fn is_env_assignment(token: &str) -> bool {
    let Some((key, value)) = token.split_once('=') else {
        return false;
    };
    !key.is_empty()
        && !value.is_empty()
        && key
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn is_safe_read_only_command(program: &str, args: &[String]) -> bool {
    matches!(
        program,
        "pwd"
            | "ls"
            | "cat"
            | "head"
            | "tail"
            | "wc"
            | "sort"
            | "uniq"
            | "cut"
            | "sed"
            | "awk"
            | "rg"
            | "fd"
            | "find"
            | "grep"
            | "which"
            | "whereis"
            | "env"
            | "printenv"
            | "date"
            | "uname"
            | "whoami"
            | "ps"
            | "stat"
            | "du"
            | "tree"
            | "realpath"
    ) || is_read_only_git_command(program, args)
}

fn is_read_only_git_command(program: &str, args: &[String]) -> bool {
    if program != "git" {
        return false;
    }
    let subcommand = args
        .iter()
        .find(|arg| !arg.starts_with('-'))
        .map(String::as_str);
    matches!(
        subcommand,
        Some("status" | "diff" | "show" | "log" | "branch" | "rev-parse" | "ls-files" | "grep")
    )
}

fn is_safe_project_command(program: &str, args: &[String]) -> bool {
    match program {
        "cargo" => matches!(
            args.iter()
                .find(|arg| !arg.starts_with('-'))
                .map(String::as_str),
            Some("check" | "test" | "fmt" | "clippy")
        ),
        "flutter" => matches!(
            args.iter()
                .find(|arg| !arg.starts_with('-'))
                .map(String::as_str),
            Some("analyze" | "test" | "format")
        ),
        "dart" => matches!(
            args.iter()
                .find(|arg| !arg.starts_with('-'))
                .map(String::as_str),
            Some("analyze" | "format" | "test")
        ),
        "npm" | "pnpm" | "yarn" | "bun" => matches!(
            args.iter()
                .find(|arg| !arg.starts_with('-'))
                .map(String::as_str),
            Some("test" | "lint" | "format" | "typecheck")
        ),
        _ => false,
    }
}

fn is_project_scoped_file_op(program: &str, args: &[String], project_root: &Path) -> bool {
    let path_args = match program {
        "touch" | "mkdir" | "rm" => collect_non_flag_args(args),
        "cp" | "mv" => collect_non_flag_args(args),
        _ => return false,
    };

    !path_args.is_empty()
        && path_args
            .iter()
            .all(|arg| path_is_within_project(arg, project_root))
}

fn collect_non_flag_args(args: &[String]) -> Vec<&str> {
    args.iter()
        .filter(|arg| !arg.starts_with('-'))
        .map(String::as_str)
        .collect()
}

fn path_is_within_project(raw: &str, project_root: &Path) -> bool {
    if raw.is_empty() || raw == "." {
        return true;
    }
    if raw.starts_with('~') {
        return false;
    }
    let path = Path::new(raw);
    let candidate = if path.is_absolute() {
        PathBuf::from(path)
    } else {
        project_root.join(path)
    };
    normalize_without_fs(&candidate)
        .map(|normalized| normalized.starts_with(project_root))
        .unwrap_or(false)
}

fn normalize_without_fs(path: &Path) -> Option<PathBuf> {
    let mut normalized = if path.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir().ok()?
    };

    for component in path.components() {
        match component {
            Component::RootDir => normalized.push(std::path::MAIN_SEPARATOR.to_string()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            Component::Normal(part) => normalized.push(part),
            Component::Prefix(_) => return None,
        }
    }

    Some(normalized)
}

fn contains_shell_control(command: &str) -> bool {
    let forbidden = ["&&", "||", "|", ";", ">", "<", "`", "$(", "\n"];
    forbidden.iter().any(|marker| command.contains(marker))
}

fn shell_split(command: &str) -> Option<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut chars = command.chars().peekable();

    while let Some(ch) = chars.next() {
        match quote {
            Some(active) if ch == active => quote = None,
            Some(_) => current.push(ch),
            None if ch == '"' || ch == '\'' => quote = Some(ch),
            None if ch.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            None if ch == '\\' => {
                let next = chars.next()?;
                current.push(next);
            }
            None => current.push(ch),
        }
    }

    if quote.is_some() {
        return None;
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    (!tokens.is_empty()).then_some(tokens)
}
