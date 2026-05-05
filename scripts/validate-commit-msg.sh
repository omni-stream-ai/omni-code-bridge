#!/usr/bin/env sh

set -eu

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <commit-msg-file>" >&2
  exit 2
fi

commit_msg_file="$1"

if [ ! -f "$commit_msg_file" ]; then
  echo "Commit message file not found: $commit_msg_file" >&2
  exit 2
fi

subject_line=$(
  grep -v '^[[:space:]]*#' "$commit_msg_file" | sed -n '1p'
)

pattern='^(build|chore|ci|docs|feat|fix|perf|refactor|revert|style|test)(\([[:alnum:]_.-]+\))?(!)?: .+'

if ! printf '%s\n' "$subject_line" | grep -Eq "$pattern"; then
  echo "Commit message does not follow Conventional Commits format." >&2
  echo "Expected: <type>(<scope>)?(!)?: <description>" >&2
  echo "Types: build, chore, ci, docs, feat, fix, perf, refactor, revert, style, test" >&2
  echo "Examples:" >&2
  echo "  feat: add approval webhook fallback" >&2
  echo "  fix(api): guard missing client id header" >&2
  exit 1
fi
