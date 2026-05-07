#!/usr/bin/env bash
set -e

REPO="omni-stream-ai/omni-code-bridge"
BIN_NAME="omni-code-bridge"
INSTALL_DIR="${HOME}/.omni-code-bridge/bin"
CONFIG_DIR="${HOME}/.omni-code-bridge"
TEMP_DIR=$(mktemp -d)

cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

detect_shell_rc() {
    local shell_name
    shell_name="$(basename "${SHELL:-}")"

    case "$shell_name" in
        zsh) echo "${HOME}/.zshrc" ;;
        bash) echo "${HOME}/.bashrc" ;;
        *)
            if [ -f "${HOME}/.zshrc" ]; then
                echo "${HOME}/.zshrc"
            else
                echo "${HOME}/.bashrc"
            fi
            ;;
    esac
}

ensure_path_in_shell_rc() {
    local rc_file
    local path_line='export PATH="${HOME}/.omni-code-bridge/bin:$PATH"'

    rc_file="$(detect_shell_rc)"
    mkdir -p "$(dirname "$rc_file")"
    touch "$rc_file"

    if grep -Fqs "$path_line" "$rc_file"; then
        echo "PATH already configured in ${rc_file}"
        return 0
    fi

    {
        echo ""
        echo "# Added by omni-code-bridge installer"
        echo "$path_line"
    } >> "$rc_file"

    echo "Added ${INSTALL_DIR} to PATH in ${rc_file}"
}

detect_platform() {
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')
    local arch=$(uname -m)

    case "$arch" in
        x86_64) arch="x64" ;;
        aarch64|arm64) arch="arm64" ;;
        *) echo "Unsupported architecture: $arch" >&2; exit 1 ;;
    esac

    case "$os" in
        linux) os="linux" ;;
        darwin) os="macos" ;;
        *) echo "Unsupported OS: $os" >&2; exit 1 ;;
    esac

    echo "${os}-${arch}"
}

download_release() {
    local version="$1"
    local platform="$2"
    local url
    local extracted_bin

    if [ "$version" = "latest" ]; then
        url="https://github.com/${REPO}/releases/latest/download/${BIN_NAME}-${platform}.tar.gz"
    else
        url="https://github.com/${REPO}/releases/download/${version}/${BIN_NAME}-${platform}.tar.gz"
    fi

    echo "Downloading ${BIN_NAME} ${version} for ${platform}..."
    if ! curl -fsSL "$url" -o "${TEMP_DIR}/${BIN_NAME}.tar.gz"; then
        echo "Failed to download from GitHub releases: ${url}" >&2
        return 1
    fi

    mkdir -p "$INSTALL_DIR"
    tar -xzf "${TEMP_DIR}/${BIN_NAME}.tar.gz" -C "$TEMP_DIR"
    extracted_bin=$(find "$TEMP_DIR" -type f -name "$BIN_NAME" -perm -u+x | head -n1)

    if [ -z "$extracted_bin" ]; then
        echo "Failed to find ${BIN_NAME} in downloaded archive" >&2
        return 1
    fi

    mv "$extracted_bin" "${INSTALL_DIR}/${BIN_NAME}"
    chmod +x "${INSTALL_DIR}/${BIN_NAME}"
    echo "Installed to ${INSTALL_DIR}/${BIN_NAME}"
}

build_from_source() {
    echo "Building ${BIN_NAME} from source..."
    if ! command -v cargo &> /dev/null; then
        echo "Cargo not found. Please install Rust: https://rustup.rs" >&2
        exit 1
    fi

    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-}")" 2>/dev/null && pwd)"
    local repo_dir="${script_dir}/.."

    if [ ! -f "${repo_dir}/Cargo.toml" ]; then
        echo "No local source found. Clone the repo and run the script directly:" >&2
        echo "  git clone https://github.com/${REPO}.git" >&2
        echo "  cd omni-code-bridge && bash scripts/install.sh" >&2
        exit 1
    fi

    cd "$repo_dir"
    cargo build --release --quiet
    mkdir -p "$INSTALL_DIR"
    cp "target/release/${BIN_NAME}" "${INSTALL_DIR}/${BIN_NAME}"
    echo "Installed to ${INSTALL_DIR}/${BIN_NAME}"
}

has_local_source() {
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-}")" 2>/dev/null && pwd)"
    [ -f "${script_dir}/../Cargo.toml" ]
}

get_installed_version() {
    if [ -x "${INSTALL_DIR}/${BIN_NAME}" ]; then
        "${INSTALL_DIR}/${BIN_NAME}" --version 2>/dev/null | awk '{print $2}'
    else
        echo ""
    fi
}

setup_systemd_service() {
    if ! command -v systemctl &> /dev/null; then
        return 0
    fi

    local service_dir="${HOME}/.config/systemd/user"
    local service_file="${service_dir}/omni-code-bridge.service"
    local should_create_service="yes"
    local should_start_service="yes"

    if [ -f "$service_file" ]; then
        echo "systemd user service already exists at ${service_file}"
    else
        echo ""
        if [ -t 0 ] && [ -t 1 ]; then
            read -r -p "Create a systemd user service for omni-code-bridge? [Y/n] " answer
            answer="${answer:-Y}"
            if [[ ! "$answer" =~ ^[Yy]$ ]]; then
                should_create_service="no"
            fi
        else
            echo "Non-interactive shell detected; creating a systemd user service by default."
        fi

        if [ "$should_create_service" != "yes" ]; then
            return 0
        fi

        mkdir -p "$service_dir"
        cat > "$service_file" <<EOF
[Unit]
Description=Omni Code Bridge
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${INSTALL_DIR}/${BIN_NAME} serve
Restart=on-failure
RestartSec=3
Environment=RUST_LOG=info
EnvironmentFile=-${CONFIG_DIR}/.env

[Install]
WantedBy=default.target
EOF

        echo "Created user service at ${service_file}"

        systemctl --user daemon-reload
        systemctl --user enable omni-code-bridge.service
        echo "Enabled omni-code-bridge.service (user)"

        echo ""
        if [ -t 0 ] && [ -t 1 ]; then
            read -r -p "Start the service now? [Y/n] " start_answer
            start_answer="${start_answer:-Y}"
            if [[ ! "$start_answer" =~ ^[Yy]$ ]]; then
                should_start_service="no"
            fi
        else
            echo "Non-interactive shell detected; starting the service by default."
        fi

        if [ "$should_start_service" = "yes" ]; then
            if systemctl --user start omni-code-bridge.service; then
                echo "omni-code-bridge.service started."
                echo "Check status: systemctl --user status omni-code-bridge"
            else
                echo "Failed to start omni-code-bridge.service automatically."
                echo "You can start it later with: systemctl --user start omni-code-bridge"
            fi
        fi
    fi

    echo ""
    echo "Service management commands:"
    echo "  systemctl --user start omni-code-bridge"
    echo "  systemctl --user stop omni-code-bridge"
    echo "  systemctl --user status omni-code-bridge"
    echo "  journalctl --user -u omni-code-bridge -f"
    echo ""
    echo "Download the client at: https://github.com/omni-stream-ai/omni-code/releases"
}

main() {
    local version="${1:-latest}"
    local platform=$(detect_platform)

    local installed_version
    installed_version=$(get_installed_version)

    if [ -n "$installed_version" ] && [ -n "$version" ] && [ "$installed_version" = "$version" ]; then
        echo "${BIN_NAME} ${version} is already up to date."
        exit 0
    fi

    mkdir -p "$INSTALL_DIR"

    if ! download_release "$version" "$platform"; then
        if ! has_local_source; then
            echo "Run from a local checkout to build from source:" >&2
            echo "  git clone https://github.com/${REPO}.git" >&2
            echo "  cd omni-code-bridge && bash scripts/install.sh" >&2
            exit 1
        fi
        build_from_source
    fi

    echo ""
    ensure_path_in_shell_rc

    if [[ ":$PATH:" != *":${INSTALL_DIR}:"* ]]; then
        echo ""
        echo "For the current shell, run:"
        echo "  export PATH=\"\${HOME}/.omni-code-bridge/bin:\$PATH\""
    fi

    setup_systemd_service
}

main "$@"
