# 项目运行环境

项目详情页的「项目运行环境」用于独立运行各子项目。它与验收共用安装、构建、启动和 Docker 镜像配置，手动启动无需创建测试计划。配置入口也保留在验收流程中。

点击「一键自动配置所有环境」会先由程序读取项目清单、包管理器和脚本声明，补全安装、构建和启动命令，再仅将仍无法确定启动方式的子项目交给 Agent 兜底，无需提供验收需求。已有启动命令或程序已确定启动方式的子项目不会触发 Agent，可选的构建命令和产物目录留空也不会触发。已有的非空配置、程序已确定的字段、运行位置及 Docker 镜像设置会保留。Agent 不可用时仍会保存程序识别结果，并显示未完成配置的原因。完成后显示保存的子项目数量及已配置启动命令的数量；缺少启动依据、共享库或没有独立运行入口的目录会保留空启动命令，供手动补充。此操作保存环境配置；安装依赖、构建和启动仍按运行流程执行。目前单次配置沿用最多 16 个子项目的限制，超出时明确报错，不会只保存部分项目。

每个子项目分别保存下次启动的位置，默认是 Bridge 服务端。切换位置立即保存，只影响下次启动；运行中的位置另行显示。子项目配置按目录保存，后续验收只选择部分目标时，不会删除其他子项目已保存的配置。

「添加子项目」允许手动填写显示名称、项目内已有的相对目录和安装、构建、启动命令，目录无需具备可自动识别的项目清单。每个目录对应一份配置，同一目录不能重复添加。卡片菜单提供「编辑配置」「重命名」「删除子项目」；重命名只修改显示名称，不改变目录、源码和运行中的服务。编辑后的命令在下次启动时使用。

删除前必须停止服务并结束 Agent 测试。删除会移除配置、名称和运行位置，将该目录排除出自动识别及一键配置；源码和历史日志保留。已删除目录可通过手动添加恢复。

- **Bridge 服务端**：在项目根目录执行该子项目的启动命令，继承 Bridge 的环境。安装和构建由命令或项目自己的脚本安排，服务应在前台运行。停止时清理其进程组。日志、启动失败和退出状态可查看。
- **验收 Docker 环境**：复用 x11box 基础环境、项目镜像准备、冻结源码和远程桌面能力，以手动模式启动，不执行自动验收。可查看桌面和环境准备日志。沿用桌面运行的四小时有效期及清理机制。运行使用的源码版本会显示在状态中。

运行位置、子项目配置、运行记录和 Agent 租约存放在 Bridge 运行时数据目录的 `project-runtimes/`，不写入项目源码。镜像设置仍由项目共用。

## 状态

状态包括 `starting`、`running`、`stopping`、`stopped`、`failed` 和 `unknown`。运行中表示进程或容器存活，应用就绪由实际测试确认。状态未知时阻止重复启动。Linux 重启恢复会核对启动时间与 boot ID，避免把复用的 PID 当成原服务；其他系统在不能确认原进程身份时显示未知。Docker 状态同时核对运行记录和容器实际状态，Docker 不可用不会显示为运行成功。

## Agent 使用

Bridge 在普通执行会话的公共系统上下文中提供本项目的查询入口，所有经此入口执行的 Agent 均可使用。连接文件只允许当前用户读取，使用只绑定当前项目和会话的本机能力令牌，不暴露客户端认证凭据。内部验收会话继续使用自己的冻结运行。

```sh
omni-code-bridge project-runtime --context /path/from/session.json status
omni-code-bridge project-runtime --context /path/from/session.json logs --directory api
omni-code-bridge project-runtime --context /path/from/session.json claim --directory api
omni-code-bridge project-runtime --context /path/from/session.json exec --directory api --lease LEASE_ID --command 'curl --fail http://localhost:3000/health'
omni-code-bridge project-runtime --context /path/from/session.json claim --directory api --lease LEASE_ID
omni-code-bridge project-runtime --context /path/from/session.json release --directory api --lease LEASE_ID
```

Agent 先查询启动、运行及占用状态，再按任务需要申请使用。申请是原子操作，租约有效期五分钟，可由原持有者续约。执行命令必须提供本会话的有效租约，其他 Agent 无法借用该租约执行或释放。测试命令从项目根目录执行，限时 120 秒；使用项目声明的 Nix 等环境时，命令应包含相应入口。测试执行期间状态查询保持可用，租约不能提前释放；Docker 传输失败时保留占用至容器内超时上限。租约过期不停止用户手动启动的服务。

## HTTP 接口

所有以下接口沿用 Bridge 客户端认证：

| 接口 | 用途 |
| --- | --- |
| `GET /v2/projects/{id}/environment` | 通用项目环境与已保存子项目配置 |
| `POST /v2/projects/{id}/environment` | 保存配置，与验收配置接口共用实现 |
| `POST /v2/projects/{id}/environment/detect` | 识别项目配置 |
| `GET /v2/projects/{id}/runtime` | 子项目、保存的位置、运行和占用状态 |
| `POST /v2/projects/{id}/runtime` | 保存 `{directory, location: host或docker}` |
| `POST /v2/projects/{id}/runtime/start` | 启动 `{directory, location?}`，省略位置时使用保存值 |
| `POST /v2/projects/{id}/runtime/stop` | 停止 `{directory}`，活动租约返回冲突 |
| `POST /v2/projects/{id}/runtime/logs` | 查看 `{directory}` 的日志 |
| `POST /v2/projects/{id}/runtime/targets` | 新增或编辑 `{target, name, create}`，新增需 `create: true` |
| `POST /v2/projects/{id}/runtime/targets/rename` | 修改显示名称 `{directory, name}` |
| `POST /v2/projects/{id}/runtime/targets/delete` | 删除配置并排除自动识别 `{directory}`，运行中返回冲突 |

原有 `/acceptance-environment` 接口继续兼容。Agent 的查询、申请、续约、执行、释放通过只监听 loopback 的独立会话入口处理，不接受调用者自报其他会话或项目身份。

项目配置识别请求使用 `{agent: true, all_targets: true, current_targets: [...]}` 时先由程序识别所有候选子项目，再仅针对缺少启动命令的子项目调用 Agent。`agent: false` 时只返回程序识别及已有配置的合并结果，完全不调用 Agent。服务端保留非空字段及 Agent 未返回的候选目录；Agent 失败时返回程序结果并在 `reasoning` 中说明原因。返回的 `targets` 通过环境保存接口持久化。省略 `all_targets` 时仍按本次验收需求识别相关项目。

常规回归运行 `cargo test project_runtime`。真实 Docker 测试需提供已有兼容镜像，不下载镜像：

```sh
OMNI_CODE_TEST_RUNTIME_IMAGE=your-local-x11box-image cargo test project_runtime -- --include-ignored
```
