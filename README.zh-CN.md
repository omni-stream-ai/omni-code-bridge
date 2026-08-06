<p align="center">
  <img src="https://raw.githubusercontent.com/omni-stream-ai/omni-code/main/assets/app-icon.svg" width="128" alt="Omni Code Bridge">
</p>

<h1 align="center">Omni Code Bridge</h1>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT License"></a>
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/Rust-2024--edition-dea584?logo=rust" alt="Rust"></a>
  <a href="https://github.com/omni-stream-ai/omni-code-bridge/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/omni-stream-ai/omni-code-bridge/ci.yml?label=CI" alt="CI"></a>
  <a href="https://github.com/omni-stream-ai/omni-code-bridge/releases"><img src="https://img.shields.io/github/v/release/omni-stream-ai/omni-code-bridge" alt="Release"></a>
  <a href="https://crates.io/crates/omni-code-bridge"><img src="https://img.shields.io/crates/v/omni-code-bridge" alt="Crates.io"></a>
</p>

<p align="center">
  <a href="README.md">English</a>
</p>

---

Omni Code 的 Rust 桥接服务。对外提供 HTTP 和 SSE API，供移动端客户端连接，并桥接到本地编码代理（`codex`、`claude`、`opencode`）。

## 安装

| 方式 | 命令 |
| --- | --- |
| **Homebrew** (macOS / Linux) | `brew install omni-stream-ai/omni-code-bridge/omni-code-bridge` |
| **Arch Linux** (AUR) | `yay -S omni-code-bridge-bin` |
| **cargo** | `cargo install omni-code-bridge` |
| **curl** (macOS / Linux) | `curl -fsSL https://raw.githubusercontent.com/omni-stream-ai/omni-code-bridge/main/scripts/install.sh \| bash` |
| **PowerShell** (Windows) | `powershell -ExecutionPolicy Bypass -Command "iwr https://raw.githubusercontent.com/omni-stream-ai/omni-code-bridge/main/scripts/install.ps1 -UseBasicParsing \| iex"` |

> **Arch Linux 用户：** 安装后请启用 systemd 服务：
> ```bash
> systemctl --user enable --now omni-code-bridge.service
> ```
> 现在随附的 user service 会先通过 `ExecStartPre` 执行
> `omni-code-bridge settings-validate`，所以一旦 bridge settings 非法，会在服务启动前直接失败。

## 快速开始

```bash
cp .env.example .env
cargo run
```

默认监听 `http://127.0.0.1:8787`。

> `cargo run` 需要 Rust 工具链。如果你通过 Homebrew、AUR 或安装脚本安装，直接运行 `omni-code-bridge` 即可。

## 可选依赖

- Agent CLI：`codex`、`claude`、`opencode` 或 `kiro-cli`
- 本地检出 [omni-code](https://github.com/omni-stream-ai/omni-code)，用于桥接服务提供内置 APK 更新接口

Agent 二进制路径可以通过 `ECHO_MATE_CODEX_BIN` 和 `ECHO_MATE_OPENCODE_BIN` 覆盖。

## HTTP API

| 接口 | 说明 |
| --- | --- |
| `POST /client-auth/requests` | 申请客户端授权 |
| `POST /client/messages` | 向项目/会话流程推送消息 |
| `POST /devices/register` | 注册客户端设备以接收推送 |
| `GET /files?path=...` | 返回已登记本地项目目录中的文件 |
| `GET /sessions/{id}/messages` | 列出会话消息；支持 `limit`、`before_id`、`after_id` |
| `GET /app-update/manifest` | APK 更新 manifest |
| `GET /app-update/apk` | 下载最新 APK |

`GET /sessions/{id}/messages` 按存储顺序返回消息。未传 cursor 时返回最新一页：
`limit` 默认 `50`，并限制在 `1..=200`，因此正在生成中的 assistant 回复会保留在默认页。
`before_id` 返回某条消息之前的消息；`after_id` 返回某条消息之后的消息。
`before_id` 和 `after_id` 不能同时使用。`limit` 按展示消息计数：每条 `user`
消息算一条，连续的 agent 回复段算一条，即使其中包含多条 `assistant`/`system` 消息。响应为
`data: { messages, has_more, next_cursor }`；
向上翻历史时把 `next_cursor` 作为下一次请求的 `before_id`，轮询新消息时把它作为
下一次请求的 `after_id`。

### Provider 选择

创建会话（`POST /sessions`）、更新会话（`PATCH /sessions/{id}`）和发送消息
（`POST /sessions/{id}/messages`）都支持 `provider_id`。

- 不传 `provider_id`：完全跳过 bridge 侧 provider 解析
- `provider_id: "AUTO"`：按优先级从项目级或全局 provider 自动选择
- `provider_id: "<具体 provider id>"`：强制使用指定 provider

示例：

```json
// 不做 bridge provider 解析
{
  "provider_id": null
}
```

```json
// 显式自动选择
{
  "provider_id": "AUTO"
}
```

```json
// 显式指定 provider
{
  "provider_id": "openai-primary"
}
```

对于 `PATCH /sessions/{id}`，请求体里省略 `provider_id` 表示不修改当前会话设置，
而 `"provider_id": null` 表示清空已保存的会话级选择。

### ACP Profiles

实验性的 ACP agent 支持通过 `~/.omni-code/settings.json` 里的 `acp_servers` 配置。
你可以直接从仓库里的
[`config/settings.acp.example.json`](config/settings.acp.example.json)
拷贝一份，再按本地环境做修改。

当前 bridge 支持这些 ACP profile：

- `stdio`：本地 ACP `stdio + JSON-RPC` runtime，例如 `kiro-cli acp` 或其他 ACP 兼容 CLI 命令
- `kiro`：旧版 Kiro 专用 stdio profile 的兼容别名
- `generic_http`：实验性的 HTTP/SSE ACP endpoint 兼容模式

通过 `stdio` 或 `kiro` 使用 Kiro 前，请先确认已经安装并登录 `kiro-cli`：

```bash
kiro-cli login
```

对于 `AgentKind::Acp`，可以把 `provider_id` 设为某个 `acp_servers[].id`，也可以传 `"AUTO"`
按优先级自动选择启用中的 ACP server。Codex 和 OpenCode ACP 应该作为这里的 ACP stdio
server 来验证；bridge 不需要为了这个再走单独的 Codex/OpenCode 接入路径。

stdio ACP 配置示例：

```json
{
  "ai_approval": {
    "enabled": false,
    "base_url": "https://api.openai.com/v1",
    "api_key": "",
    "model": "gpt-4.1-mini",
    "max_risk": "low"
  },
  "model_providers": [],
  "acp_servers": [
    {
      "id": "kiro-local",
      "name": "Kiro Local ACP",
      "profile": "stdio",
      "command": "kiro-cli",
      "args": ["acp"],
      "default_model": "claude-sonnet-4",
      "enabled": true,
      "priority": 0,
      "headers": [],
      "env": []
    },
    {
      "id": "opencode-acp",
      "name": "OpenCode ACP",
      "profile": "stdio",
      "command": "opencode",
      "args": ["acp"],
      "enabled": false,
      "priority": 20,
      "headers": [],
      "env": []
    },
    {
      "id": "codex-acp",
      "name": "Codex ACP",
      "profile": "stdio",
      "command": "codex",
      "args": ["acp"],
      "enabled": false,
      "priority": 30,
      "headers": [],
      "env": []
    }
  ]
}
```

请按本机实际 ACP runtime 调整 `command` 和 `args`。可以先保留 disabled 验证模板，再用
`GET /agents/acp/diagnostic?provider_id=<id>&probe=true&refresh=true` 定向探测，确认后再按
priority 启用。

generic HTTP ACP 配置示例：

```json
{
  "ai_approval": {
    "enabled": false,
    "base_url": "https://api.openai.com/v1",
    "api_key": "",
    "model": "gpt-4.1-mini",
    "max_risk": "low"
  },
  "model_providers": [],
  "acp_servers": [
    {
      "id": "acp-http",
      "name": "ACP HTTP Gateway",
      "profile": "generic_http",
      "endpoint": "https://acp.example.com",
      "auth_token": "replace-me",
      "enabled": true,
      "priority": 10,
      "headers": [
        { "key": "X-ACP-Client", "value": "omni-code-bridge" }
      ],
      "env": []
    }
  ]
}
```

使用 ACP 创建会话示例：

```json
{
  "project_id": "my-project",
  "title": "Try ACP",
  "agent": "acp",
  "provider_id": "kiro-local"
}
```

对于 `generic_http`，bridge 当前会按顺序尝试这些 turn URL：
`/turns`、`/turn`、`/sessions/{session_id}/turns`，最后才是你配置的 endpoint 本身。
请求会以 JSON 形式发送，常见字段包括 `session_id`、`thread_id`、`conversation_id`、
`cwd`、`project_root`、`input`、`message`、`prompt`。响应可以是：

- JSON：assistant 文本可放在 `output_text`、`text`、`content` 或 `message.text`
- SSE：通过 `data: {...}` 连续发送事件，文本增量走 `delta.text`，审批事件走 approval /
  permission 类事件，并用 `{ "type": "done" }` 作为结束标记

审批回传时，bridge 当前会依次尝试这些 URL：
`/approvals/{request_id}/reply`、`/approval/{request_id}/reply`、
`/permissions/{request_id}/reply`、`/permission/{request_id}/reply`、
`/approvals/{request_id}`、`/approval/{request_id}`。
取消则是 best-effort，会尝试 `/sessions/{session_ref}/cancel`、
`/session/{session_ref}/cancel`、`/turns/cancel`、`/turn/cancel`、`/cancel`。

`GET /agents` 现在除了报告 binary 是否存在，也会返回结构化的 readiness 状态。对于
Kiro 型 ACP，如果 `kiro-cli` 已安装但还没有完成 `kiro-cli login`，`readiness` 会变成
`attention_required`，同时 `readiness_message` 会直接提示下一步动作。通用 `stdio` 条目只
检查配置的 command 是否存在；真实 ACP 握手请用 `?probe=true`。ACP 条目还会带上
`acp_diagnostic`，描述当前实际会选中的 enabled server，包括它的 `server_id`、名称、
`profile`、`command`/`args` 或 `endpoint`、auth/model/header/env 摘要，以及当前启用了
多少个 ACP server。

如果前端或脚本需要单独做健康检查，可以调用 `GET /agents/acp/diagnostic`，它会返回当前
选中 ACP server 的结构化诊断，以及 `installed`、`installed_path`、`readiness` 和
`readiness_message`。响应里还会带上 `source: "live_probe"` 和 `probed_at`，并且接口
支持 `?refresh=true`，让调用方可以显式声明需要一次新的探测结果。也可以传
`?provider_id=<acp_server_id>` 来定向诊断某个已配置的 ACP server，包括低优先级或
disabled 的条目。不传时，响应会自动回填最终命中的 `provider_id`。如果需要批量排查，
可以传 `?all=true` 一次返回所有已配置 ACP server 的诊断结果。如果需要一次最小真实运行
探测，可以传 `?probe=true`，响应里会多出 `handshake_probe`。当前它会对 `stdio`/`kiro`
ACP 执行一次实时的 `initialize -> session/new -> session/prompt` 最小 probe turn 检查；对
`generic_http` 则会执行一次真实的 turn-creation `POST` 探测。probe 结果里会带上 `mode`
和 `stage`，方便客户端区分“完整 stdio probe turn”与“尽力模拟真实 turn 请求的 HTTP 探测”。
需要注意的是，`generic_http` 的 probe 会沿用真实运行链路的候选 turn URL 去发请求，但它
仍然是轻量健康检查；它既能验证 JSON 响应，也能验证 SSE 流式 turn 响应，但仍不代表完整
ACP 对话已经端到端验证通过。对于 `generic_http`，诊断结果还会直接带上
`turn_url_candidates`、`approval_reply_url_templates`、`cancel_url_templates`，方便排障时
直接确认 bridge 真实会尝试哪些 URL 模板，而不需要再翻源码。如果传了 `provider_id` 但
settings 里没有对应 ACP server，接口会直接返回 `400 Bad Request`。批量结果里还会带上
`is_default_selected`，方便客户端判断当前默认会选中哪条 ACP 配置。`all=true` 和
`provider_id` 互斥，同时使用时也会返回 `400 Bad Request`。

### 思考等级

创建会话（`POST /sessions`）、更新会话（`PATCH /sessions/{id}`）和发送消息
（`POST /sessions/{id}/messages`）也支持可选的 `reasoning_effort`。

允许值为 `"low"`、`"medium"`、`"high"`、`"xhigh"` 和 `"max"`。消息级
`reasoning_effort` 会覆盖本轮的会话默认值。对于 `PATCH /sessions/{id}`，
省略 `reasoning_effort` 表示不修改，`"reasoning_effort": null` 表示清空会话默认值。

Codex 会收到 `model_reasoning_effort`，Claude Code 会收到 `--effort`。OpenCode 当前会忽略
这个字段，因为 bridge 使用的 headless prompt API 还没有暴露对应选项。

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

## 文件获取接口

`GET /files?path=<file-path>`

返回已登记本地项目目录中的文件。需要带上客户端授权头：

- `Authorization: Bearer <token>`
- `x-omni-code-client-id: <client_id>`

响应体是文件原始内容。服务会根据扩展名自动推断 `Content-Type`，图片、文本、JSON、PDF、音频等文件都可以直接返回。

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `path` | 是 | 绝对路径可直接返回；相对路径需搭配 `project_id` 或 `session_id` |
| `project_id` | 否 | 将查找范围限制在某个项目根目录下 |
| `session_id` | 否 | 将查找范围限制在该会话对应的本地项目根目录下 |

> `project_id` 和 `session_id` 不能同时传。

```bash
# 绝对路径
curl -H "Authorization: Bearer <token>" \
  -H "x-omni-code-client-id: <client_id>" \
  "http://127.0.0.1:8787/files?path=/absolute/path/to/image.png"

# 相对路径 + project_id
curl -H "Authorization: Bearer <token>" \
  -H "x-omni-code-client-id: <client_id>" \
  "http://127.0.0.1:8787/files?project_id=<project-id>&path=assets/logo.png"
```

## APK 更新接口

桥接服务可以从本地 [omni-code](https://github.com/omni-stream-ai/omni-code) 仓库中找到最新 Android APK：

- `GET /app-update/manifest`
- `GET /app-update/apk`

默认会从本地检出仓库中查找 APK 构建产物，并从 `pubspec.yaml` 读取版本号。这些接口主要用于本地开发或自托管分发。客户端默认会检查官方 GitHub Release manifest，只有在你显式配置自定义更新地址时才会使用桥接服务的 manifest。

## 开发

```bash
cargo check
sh scripts/setup-git-hooks.sh
```

首次克隆后，运行 `sh scripts/setup-git-hooks.sh` 以启用本地 `commit-msg` hook。

### Settings 校验

可以在启动服务前先校验 bridge settings 文件：

```bash
cargo run -- settings-validate --path config/settings.acp.example.json
```

如果省略 `--path`，命令会校验当前解析出的默认 settings 路径
（`~/.omni-code/settings.json`，或者你通过 `ECHO_MATE_SETTINGS_PATH` 指定的路径）。
现在生产服务启动路径也会使用同样严格的 settings 解析与校验规则；如果配置文件非法，
服务会直接 fail fast，而不会再静默回退到默认配置。

### Session Trace

查看本地 Codex 或 Claude transcript 中，某个会话最近几条发给 agent 的命令及其响应：

```bash
omni-code-bridge session-trace --session "<会话 id 或标题>"
omni-code-bridge session-trace --session "<会话 id 或标题>" --limit 10
```

`--session` 支持会话 id、标题精确匹配，或标题模糊匹配。`--limit` 可选，默认是 `5`。

### ACP Smoke Test

针对 ACP，也提供了一个 bridge 级别的 smoke 脚本：

```bash
scripts/acp-smoke.sh --keep-artifacts
```

它会做这些事：

- 检查 `GET /health`
- 如果没有提供 `BRIDGE_CLIENT_ID` 和 `BRIDGE_TOKEN`，自动申请并批准一个本地 client auth token
- 调用 `GET /agents`，确认 ACP summary 存在
- 调用 `GET /agents/acp/diagnostic`
- 默认附带 `?probe=true&refresh=true`，把一次实时 ACP probe 结果也纳入 smoke 检查
- 如果 ACP readiness 不是 `ready`，或者实时 probe 失败，脚本会直接返回非零退出码

常用选项：

| 选项 | 说明 |
| --- | --- |
| `--provider-id ID` | 定向诊断某个 `acp_servers[].id` |
| `--all` | 一次获取所有已配置 ACP server 的诊断 |
| `--no-probe` | 跳过实时 probe，只检查静态诊断信息 |
| `--allow-attention-required` | 允许 `readiness=attention_required` 时不把 smoke 判定为失败 |
| `--output-dir DIR` | 把 JSON 输出保存在固定目录 |
| `--no-auto-auth` | 要求提前提供 `BRIDGE_CLIENT_ID` 和 `BRIDGE_TOKEN` |

脚本依赖 `curl` 和 `jq`。如果启用了 auto-auth，而本地又没有现成的 `omni-code-bridge`
可执行文件，它还会使用 `cargo` 来批准临时 client auth 请求。

### 提交信息

仓库使用 shell 版 `commit-msg` hook 校验提交信息，要求使用 Conventional Commits。GitHub Release Notes 也会基于这些提交信息生成：

- `feat: add approval webhook fallback`
- `fix(api): guard missing client id header`
- `docs: update bridge deployment notes`

## 发布

该仓库通过 GitHub Actions 发布 Linux、macOS 和 Windows 的 release 二进制。Release Notes 由 Conventional Commit 消息生成。

## 许可证

[MIT](LICENSE)
