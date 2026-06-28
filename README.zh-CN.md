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
| `GET /app-update/manifest` | APK 更新 manifest |
| `GET /app-update/apk` | 下载最新 APK |
| `GET /speech`, `GET /speech/models` | 本地语音模型管理 |
| `POST /speech/models/downloads` | 下载语音模型 |
| `GET /speech/models/downloads/{task_id}` | 轮询下载状态 |
| `GET/PUT /speech/profiles/{profile}/model` | 绑定模型到 profile |
| `GET /speech/realtime` | 实时语音 descriptor |
| `GET /speech/realtime/ws` | Websocket 实时/通话模式语音 |
| `GET /v1/models` | OpenAI 兼容模型列表 |
| `POST /v1/audio/transcriptions` | OpenAI 兼容 ASR |
| `POST /v1/audio/speech` | OpenAI 兼容 TTS |

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

当前 bridge 支持两个 ACP profile：

- `kiro`：通过 `kiro-cli acp` 走本地 `stdio + JSON-RPC`
- `generic_http`：实验性的 HTTP/SSE ACP endpoint 兼容模式

使用 `kiro` profile 前，请先确认已经安装并登录 `kiro-cli`：

```bash
kiro-cli login
```

对于 `AgentKind::Acp`，可以把 `provider_id` 设为某个 `acp_servers[].id`，也可以传 `"AUTO"`
按优先级自动选择启用中的 ACP server。

Kiro ACP 配置示例：

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
      "profile": "kiro",
      "command": "kiro-cli",
      "args": ["acp"],
      "default_model": "claude-sonnet-4",
      "enabled": true,
      "priority": 0,
      "headers": [],
      "env": []
    }
  ]
}
```

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
  "title": "Try Kiro ACP",
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
`attention_required`，同时 `readiness_message` 会直接提示下一步动作。ACP 条目还会带上
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
探测，可以传 `?probe=true`，响应里会多出 `handshake_probe`。当前它会对 Kiro 型 ACP
执行一次实时的 `initialize -> session/new -> session/prompt` 最小 probe turn 检查；对
`generic_http` 则会执行一次真实的 turn-creation `POST` 探测。probe 结果里现在还会带上
`mode` 和 `stage`，方便客户端区分“完整 stdio probe turn”与“尽力模拟真实 turn 请求的
HTTP 探测”。需要注意的是，
`generic_http` 的 probe 现在会沿用真实运行链路的候选 turn URL 去发请求，但它仍然是
轻量健康检查；它现在既能验证 JSON 响应，也能验证 SSE 流式 turn 响应，但仍不代表完整
ACP 对话已经端到端验证通过。
对于 `generic_http`，诊断结果现在还会直接带上 `turn_url_candidates`、
`approval_reply_url_templates`、`cancel_url_templates`，方便排障时直接确认 bridge 真实会
尝试哪些 URL 模板，而不需要再翻源码。
如果传了 `provider_id` 但 settings 里没有对应 ACP server，接口会直接返回
`400 Bad Request`。批量结果里还会带上 `is_default_selected`，方便客户端判断当前默认会
选中哪条 ACP 配置。`all=true` 和 `provider_id` 互斥，同时使用时也会返回
`400 Bad Request`。

### 思考等级

创建会话（`POST /sessions`）、更新会话（`PATCH /sessions/{id}`）和发送消息
（`POST /sessions/{id}/messages`）也支持可选的 `reasoning_effort`。

允许值为 `"low"`、`"medium"`、`"high"`、`"xhigh"` 和 `"max"`。消息级
`reasoning_effort` 会覆盖本轮的会话默认值。对于 `PATCH /sessions/{id}`，
省略 `reasoning_effort` 表示不修改，`"reasoning_effort": null` 表示清空会话默认值。

Codex 会收到 `model_reasoning_effort`，Claude Code 会收到 `--effort`。OpenCode 当前会忽略
这个字段，因为 bridge 使用的 headless prompt API 还没有暴露对应选项。

## 本地语音接口

桥接服务现在可以通过 `sherpa-onnx` 跑本地 ASR 和 TTS，并提供两层 API：

- `/speech/*` 模型管理接口，供客户端列出、下载、查看、选择本地语音模型
- `/v1/*` OpenAI 兼容接口，供客户端复用标准音频请求流程

当前内置模型目录包括：

- 批量转写 ASR：`sensevoice-small-int8`
- 实时/通话模式 ASR 候选：`streaming-paraformer-zh-en`、`funasr-streaming-paraformer-zh-yue-en`
- TTS：`vits-melo-tts-zh-en`、`kokoro-int8-multi-lang-v1_1`
- VAD：`silero-vad`

`GET /speech/models` 会返回客户端可直接用于过滤的信息，包括：

- `kind`、`runtime`、`backend`
- `capabilities`，例如 `streaming`、`batch_asr`、`realtime_asr`、`speech_synthesis`、`vad`
- `features`、`languages`、`download_size_mb`、`memory_hint`、`notes`
- `sample_rate_hz`、`default_voice`、`voices`，用于客户端 TTS 参数设置
  目前本地 TTS 的 `voice` 仍使用数字 speaker ID。`vits-melo-tts-zh-en` 只有 `0` 这一个音色，
  `kokoro-int8-multi-lang-v1_1` 则会暴露多个 speaker。
- `supports_profiles`、`recommended_profiles`、`selected_by`

### 推荐接入流程

1. 调用 `GET /speech/models`
2. 选择兼容模型并通过 `POST /speech/models/downloads` 发起下载
3. 轮询 `GET /speech/models/downloads/{task_id}` 直到 `completed`
4. 通过 `PUT /speech/profiles/{profile}/model` 把已安装模型绑定到 profile
5. 调用 `/v1/audio/transcriptions` 或 `/v1/audio/speech`

Speech profile 绑定默认持久化保存到 `~/.omni-code/settings.json`。可以通过
`ECHO_MATE_SETTINGS_PATH` 覆盖设置文件位置。
TTS 音色通过 `GET/PUT /speech/models/{model_id}/voice` 按模型保存，因此在单音色和多音色
TTS 模型之间切换时不会复用不兼容的 voice。

### OpenAI 兼容语音行为

- `GET /v1/models` 只返回已经安装且可被 OpenAI 兼容音频接口使用的本地模型
- `POST /v1/audio/transcriptions` 接收标准 multipart 字段，例如 `file`、`model`、`language`、`prompt`、`response_format`、`timestamp_granularities[]`
- `POST /v1/audio/speech` 接收 OpenAI 风格 JSON，包括 `model`、`input`、`voice`、`response_format`、`speed`
- 两个 `/v1/audio/*` 接口里的 `model` 都是可选的；如果不传，会回退到当前已选择的 speech profile 模型

当前兼容限制：

- `/v1/audio/transcriptions` 支持 `response_format=json|text|verbose_json`
- `/v1/audio/transcriptions` 暂不支持 `stream=true`
- `/v1/audio/transcriptions` 目前只支持 `timestamp_granularities[]=segment`，不支持 `word`
- `/v1/audio/speech` 目前只支持 `response_format=wav`
- `/v1/audio/speech` 暂不支持 `instructions`

## 实时语音接口

实时语音接口和 `/v1/audio/*` 分开提供，因为它面向后续通话模式，而不是批量推理兼容层。

- `GET /speech/realtime` 返回 websocket descriptor、音频要求、默认 profile 绑定、命令名和事件名
- `GET /speech/realtime/ws` 升级为 websocket 会话
- Websocket 鉴权和其他受保护接口一致：
  `Authorization: Bearer <token>` 和 `x-omni-code-client-id: <client_id>`

### 实时协议约束

- 客户端通过 websocket binary frame 发送原始 `pcm_s16le`
- 采样率固定为 `16000`
- 声道支持 `1` 或 `2`；服务端会把双声道下混为单声道
- `session.update` 可覆盖 `asr_model`、`vad_model`、`channels`、`sample_rate_hz`、`enable_vad`
- `input_audio_buffer.commit` 用于提交并冲刷当前话语
- `input_audio_buffer.clear` 用于清空当前话语状态

### 服务端事件

- `session.created`
- `session.updated`
- `input_audio_buffer.committed`
- `input_audio_buffer.cleared`
- `input_audio_buffer.speech_started`
- `input_audio_buffer.speech_stopped`
- `response.audio_transcript.delta`
- `response.audio_transcript.completed`
- `error`

### 客户端模型过滤

- 先调用 `GET /speech/models`
- 保留 `installed == true` 的模型
- 实时 ASR 用 `capabilities.realtime_asr == true` 过滤
- VAD 用 `capabilities.vad == true` 过滤

`session.update` 示例：

```json
{
  "type": "session.update",
  "session": {
    "asr_model": "funasr-streaming-paraformer-zh-yue-en",
    "vad_model": "silero-vad",
    "sample_rate_hz": 16000,
    "channels": 1,
    "enable_vad": true
  }
}
```

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

### 语音烟测脚本

仓库里现在提供了一套本地端到端语音烟测脚本：

```bash
scripts/speech-smoke.sh --keep-artifacts
```

这套脚本会：

- 检查 `GET /health`
- 在没有提供 `BRIDGE_CLIENT_ID` 和 `BRIDGE_TOKEN` 时，自动申请并批准本机 client auth
- 通过 `/speech/models/downloads` 下载缺失的 ASR/TTS 模型
- 绑定 `asr.batch` 和 `tts.default`
- 通过 `/v1/audio/speech` 合成一段 wav
- 再通过 `/v1/audio/transcriptions` 用 profile 回退和显式模型两种方式转写这段 wav

常用选项：

| 选项 | 说明 |
| --- | --- |
| `--with-call-models` | 额外安装并绑定 `asr.realtime` 和 `vad.default` |
| `--with-realtime` | 额外执行 websocket realtime ASR 烟测 example |
| `--skip-download` | 如果模型未安装则直接失败 |
| `--output-dir DIR` | 把生成的产物固定写到指定目录 |
| `--no-auto-auth` | 强制要求先提供现成的 `BRIDGE_CLIENT_ID` 和 `BRIDGE_TOKEN` |

脚本依赖 `curl` 和 `jq`。

另外还提供了一个 realtime websocket 烟测 example：

```bash
cargo run --example speech_realtime_smoke -- \
  --bridge-url http://127.0.0.1:8787 \
  --client-id "$BRIDGE_CLIENT_ID" \
  --token "$BRIDGE_TOKEN" \
  --wav /tmp/omni-code-bridge-speech-smoke-12345/tts.wav
```

这个 example 会读取本地 wav，重采样到 `16 kHz`，流式发送到
`/speech/realtime/ws`，并打印收到的 realtime 事件和最终转写结果。

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
