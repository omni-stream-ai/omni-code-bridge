# Omni Code Bridge

Omni Code 的 Rust 桥接服务。它对外提供 HTTP 和 SSE API，供移动端使用，并连接到本地编码代理，例如 `codex`、`claudecode` 和 `opencode`。

Flutter 客户端仓库：
`https://github.com/omni-stream-ai/omni-code`

## 安装

**Homebrew（macOS / Linux）：**

```bash
brew tap omni-stream-ai/homebrew-omni-code-bridge
brew install omni-code-bridge
```

**Arch Linux（AUR）：**

```bash
yay -S omni-code-bridge-bin
systemctl --user daemon-reload
systemctl --user enable --now omni-code-bridge.service
```

该包由 GitHub Actions 自动发布。

**curl（macOS / Linux）：**

```bash
curl -fsSL https://raw.githubusercontent.com/omni-stream-ai/omni-code-bridge/main/scripts/install.sh | bash
```

**PowerShell（Windows）：**

```powershell
powershell -ExecutionPolicy Bypass -Command "iwr https://raw.githubusercontent.com/omni-stream-ai/omni-code-bridge/main/scripts/install.ps1 -UseBasicParsing | iex"
```

**cargo install：**

```bash
cargo install omni-code-bridge
```

安装完成后可以到这里下载客户端：
`https://github.com/omni-stream-ai/omni-code/releases`

## 依赖

- `Rust` / `cargo`
- 可选 agent CLI：`codex`、`claude` 或 `opencode`
- 可选：本地检出 `https://github.com/omni-stream-ai/omni-code`，用于桥接服务提供内置 APK 更新接口

Agent 二进制路径可以通过 `ECHO_MATE_CODEX_BIN` 和 `ECHO_MATE_OPENCODE_BIN` 覆盖。

## 运行

```bash
cp .env.example .env
cargo run
```

默认监听 `http://127.0.0.1:8787`。

## HTTP API

- `POST /client-auth/requests` 申请客户端授权
- `POST /client/messages` 向项目/会话流程推送消息
- `POST /devices/register` 注册客户端设备以接收推送
- `GET /app-update/manifest` 和 `GET /app-update/apk` 提供内置 APK 更新源

## 客户端授权

客户端通过 CLI 申请并审批授权：

1. 客户端发送 `POST /client-auth/requests`，参数为 `{ client_id, device_name }`
2. 管理员查看待审批请求并批准：

```bash
# 查看待审批请求
omni-code-bridge client-auth list --pending

# 批准指定请求
omni-code-bridge client-auth approve --request-id <request-id>

# 一次批准全部待审批请求
omni-code-bridge client-auth approve
```

3. 客户端轮询 `GET /client-auth/requests/{request_id}`，直到 `status` 变为 `approved` 并返回 `token`
4. 客户端后续请求都需要同时带上：
   - `Authorization: Bearer <token>`
   - `x-omni-code-client-id: <client_id>`

批准记录默认保存在 `~/.omni-code/client-auth.json`。

## 客户端 APK 更新接口

桥接服务可以从本地客户端仓库中找到最新 Android APK：

- `GET /app-update/manifest`
- `GET /app-update/apk`

默认会从本地检出的 `https://github.com/omni-stream-ai/omni-code` 仓库中查找 APK 构建产物，并从该仓库的 `pubspec.yaml` 读取版本号。

这些接口主要用于本地开发或自托管分发。客户端默认会检查官方 GitHub Release manifest，只有在你显式配置自定义更新地址时才会使用桥接服务的 manifest。

## 开发

```bash
cargo check
sh scripts/setup-git-hooks.sh
```

首次克隆后，运行 `sh scripts/setup-git-hooks.sh` 以启用本地 `commit-msg` hook。

## 提交信息

仓库使用 shell 版 `commit-msg` hook 校验提交信息，要求使用 Conventional Commits。GitHub Release Notes 也会基于这些提交信息生成：

- `feat: add approval webhook fallback`
- `fix(api): guard missing client id header`
- `docs: update bridge deployment notes`

## 发布

该仓库通过 GitHub Actions 发布 Linux、macOS 和 Windows 的 release 二进制。Release Notes 由 Conventional Commit 消息生成。

## 许可证

Omni Code Desktop Bridge 采用 [MIT License](LICENSE)。
