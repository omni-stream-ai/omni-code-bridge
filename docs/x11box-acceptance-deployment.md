# x11box 验收插件部署（开发分支）

当前实现仍在验证，不能作为生产就绪声明。发布验收项目见
[x11box-acceptance-plan.md](x11box-acceptance-plan.md)。

## 安装位置与方式

这是 Bridge 管理的可选运行时插件。手机发起安装，环境部署在运行 Bridge 的 Linux 机器上。每次验收启动一个独立 Docker 容器。手机只访问 Bridge 的鉴权网关。宿主机需要预先安装 Docker，Bridge 服务账号需要能够使用本机 Docker daemon。当前网关通过本机回环地址访问容器，不支持远程 Docker daemon。

开发环境可在 Bridge 服务启动前配置：

	export OMNI_CODE_X11BOX_SOURCE=/data/code/x11box

客户端选择“安装环境”后，Bridge 使用内置 deploy/x11box.Dockerfile 与该目录源码构建镜像，随后执行健康检查。安装失败可重试；安装中重启 Bridge 会标记中断，需要重试。

固定版本应配置不可变镜像摘要：

	docker image inspect --format '{{.Id}}' omni-x11box:acceptance
	export OMNI_CODE_X11BOX_IMAGE='sha256:<完整摘要>'

不要使用浮动 tag。反向代理必须转发 WebSocket Upgrade，外部访问使用 HTTPS，不要公开容器的 Xpra 14500 或 x11box 27811 端口。

手机可从项目或会话进入验收，项目不要求使用 Git。有提交的 Git 仓库使用记录的提交快照；普通目录和尚无提交的仓库使用创建验收时冻结的目录快照，以 snapshot-sha256 摘要标识。目录快照排除版本控制数据、常见依赖缓存、本地 .env 和凭据配置，保留 .env.example 等模板及应用源码、静态产物，不跟随指向项目外的符号链接。人工控制期间自动化指令被拒绝；控制租约 90 秒。桌面票据单次使用、30 秒有效；浏览器会话最长 1 小时。录像支持 Range 下载。卸载会停止活动验收并保留产物。

启动命令留空时，由 Agent 选择应用类型和启动方案。网页方案使用 `kind: web`、服务启动命令和本地 `url`，Bridge 等待地址就绪后打开带 CDP 的 Chromium；静态网页可以省略命令。原生桌面方案使用 `kind: desktop`、应用启动命令和用于匹配窗口的 `app` 标题或类名，无需 URL，Bridge 不会添加浏览器或要求 CDP。验收镜像内置 Node.js 24、npm 和 Corepack；新项目环境使用真实的包管理器与 SDK，不会替换为 npm 兼容命令；首次安装依赖可能需要几分钟，网页服务就绪等待上限为 15 分钟。Agent 的命令通过容器内的 shell 在 `/workspace` 执行，支持引号、安装／构建步骤和脚本。验收原生应用时使用 X11 窗口观察、点击、输入、快捷键和截图，缺少可验证证据时不能判定通过。

自动验收在执行清单前开始录像，保存初始画面和每项检查后的截图；操作结束后立即导出 MP4 和 JSON 报告，桌面继续运行，供人工查看或接管。原生应用截图按 `app` 匹配窗口，减少空白桌面。客户端“验收证据”中的“回放操作录像”在 App 内播放，支持暂停、拖动进度和重播；截图是补充证据。移动端通过已鉴权的 Bridge HTTP 客户端下载到临时文件再播放，避免原生播放器的网络代理配置导致请求失败，退出播放器后清理缓存。旧记录尚未导出录像时，可点击“生成录像与报告”；自动验收仍在执行时不能提前结束录像。

会话入口的自动验收在创建时冻结该会话的完整用户消息。长会话按批次交给 Agent 整理，保留早期目标和后续补充，后续纠正、取消和范围缩小优先；Agent 回复只作辅助背景。每批提炼使用独立的隐藏内部会话，执行 Agent 仅接收最终需求摘要，避免历史工具日志挤占上下文。客户端可填写“本次验收重点”补充或限定范围，并查看提炼后的需求。每条需求保存来源用户消息 ID，验收清单通过 requirement_ids 关联并校验覆盖，遗漏会要求 Agent 修正；最终判定也必须覆盖全部检查项。会话后续新增消息不会自动改变已创建验收的范围；确认前可在测试用例页沟通或手动调整，确认后需要新范围时重新创建。有提交的 Git 仓库仍需先提交待验收修改；目录快照在创建后不随项目文件变化而更新。

## 确认测试用例

新的自动验收先生成供用户审阅的测试用例（标题、操作步骤和预期结果），进入 planning / awaiting_plan_approval。此时只冻结源码和需求，不构建项目镜像、不启动桌面，也不执行验收。用户可在详情页增删或编辑用例，或与 Agent 沟通调整；草稿和沟通记录持久保存。Agent 调整期间不能确认或编辑，服务失败会保留原稿供重试。

POST /v2/acceptance/{id}/test-plan 保存手动编辑的 cases；/test-plan/messages 接收调整意见；/test-plan/confirm 确认执行。以上请求携带当前 version，过期版本返回冲突；重复确认同一版本不会重复启动。确认后冻结用例版本，再准备环境、启动应用，并将已确认用例转成桌面/浏览器指令。执行清单必须覆盖确认后的用例，不恢复用户已经删除的原始要求。初次生成失败可 POST /test-plan/retry 重试。项目入口的手动远程桌面保持直接启动。

## 按项目准备运行环境

安装插件只准备公共 x11box 基础镜像。项目或会话的启动对话框可进入“项目运行环境”，选择：

- Agent 自动准备（默认）：只读分析项目清单、SDK 约束和启动方式，生成完整 Dockerfile；已有工具足够时直接使用基础镜像。
- 基础环境：不额外准备 SDK，适合基础镜像已经能运行的项目。
- 自定义 Dockerfile：填写完整 Dockerfile；使用 FROM {{BASE_IMAGE}} 继承已安装镜像。构建上下文为本次冻结的源码快照，支持多阶段构建。最终镜像必须保留 uid 1000 的 x11box 用户及可写工作目录、桌面和控制接口。
- 已有兼容镜像：从已准备列表选择，或输入本地/仓库镜像名（允许 tag）。首次准备解析为不可变 image ID，后续运行固定使用该 ID。要更新浮动 tag 的实际版本，选择“下次重新识别并准备环境”。

配置与状态存放在插件数据目录的 environments/ 下，不修改项目文件。接口为 GET/POST /v2/projects/{project_id}/acceptance-environment；POST 参数为 mode（auto/base/dockerfile/image）、dockerfile、image、rebuild。保存配置仅影响下一次运行，已运行桌面继续使用自己的镜像。

首次进入时异步识别、构建、检查 SDK，再验证 Xpra HTML5、控制接口及截图。运行详情显示环境阶段、生成的 Dockerfile、构建日志和错误。同一环境的并发请求只构建一次。准备失败不会启动错误的基础环境，可重试或修改配置；重启时未完成的准备被标记为中断。停止运行不会让准备完成后再启动该运行，但已经开始的环境准备可以继续完成缓存。

自动环境构建或 SDK 检查失败时，会将真实诊断交给 Agent 修正一次并复用成功的构建层。应用启动进程提前退出时，会立即读取启动日志并让 Agent 修正一次启动方式；活着的编译进程可等待至多 15 分钟。启动命令继承镜像的 SDK PATH，避免使用登录 shell 覆盖路径。包含原生插件的 Flutter 项目可能还需缓存 CMake/FetchContent 拉取的第三方原生源码；仅缓存 Dart 包不等于所有原生依赖都可离线构建。

构建网络仅由服务管理员配置。可设置 OMNI_CODE_X11BOX_BUILD_PROXY=http://127.0.0.1:7890；指定代理时默认使用 host 构建网络，也可用 OMNI_CODE_X11BOX_BUILD_NETWORK=default/none/host 显式选择。代理通过 Docker 的预定义 build args 传递，不写入项目 Dockerfile 或运行镜像；这些选项不会修改验收容器的隔离网络和防火墙。Bridge 重启后重试即可保留和复用此前成功的 Docker 构建层。

自动环境按项目、基础镜像、配置和冻结的依赖清单/锁文件/SDK 版本文件缓存，普通源码内容修改不重建环境；新增顶层文件、平台目录或依赖变化会重新识别。自动构建仅提供依赖相关文件，启动时另行解压本次完整源码，避免在环境镜像中固化旧应用。自定义 Dockerfile 可以复制完整源码，因此完整源码变化会生成新环境。新环境保留其 Agent 识别的启动方式供后续复用，强制重新准备可清除旧识别结果。Docker 构建缓存仍可复用公共 SDK 层。

Flutter 环境由 Agent 根据 .fvmrc、pubspec.yaml、锁文件和目标平台准备 SDK、Linux 编译依赖及包缓存。支持接入已有的 Linux/Web 启动和验收流程；Android 模拟器和 iOS 验收仍未接入。本次真实镜像测试验证了派生构建、桌面接口和复用，并不代表任意 Flutter 项目均已通过端到端验收。

真实派生镜像验证：

    X11BOX_TEST_IMAGE='sha256:<完整摘要>' cargo test --no-default-features real_derived_image_is_verified_and_reused -- --ignored --nocapture

当前限制：最多 4 个活动验收；容器 2 CPU、4 GiB 内存、512 个进程；最长 4 小时；Bridge 每 30 秒清理过期运行。长期停机时，尚不能保证容器自行退出。

真实验证：

	X11BOX_TEST_IMAGE='sha256:<完整摘要>' cargo test real_container_acceptance_lifecycle -- --ignored --nocapture

移动浏览器测试还需 Node、Playwright 和 Chromium，并设置 NODE_PATH。测试必须验证连接、触摸、中文输入和控制撤销。Android/iOS 真机软键盘与网络切换仍需独立验收。

## Android 模拟器内的桌面连接

客户端以 --dart-define=OMNI_CODE_BRIDGE_URL=http://10.0.2.2:8787 连接宿主机。

## Codex profile 会话聚合

Bridge 只使用一个自身数据目录。会话列表会扫描 Codex 的 `$CODEX_HOME/config.toml` 和同目录下的 `<profile>.config.toml`，并根据 Codex 会话 JSONL 中的 `model_provider` 识别来源。`/v2/sessions` 返回 `profile_id` / `profile_name`，`profile_id` 查询参数可以筛选；`/v2/codex/profiles` 返回发现的 Codex profile。
在项目页进入“应用验收”，创建或打开运行，再点击“打开远程桌面”，桌面会在 App 内的 WebView 显示。

WebView 会使用 Android 系统代理。若模拟器设置了 10.0.2.2:7890 等代理，代理进程可能无法访问模拟器专用的 10.0.2.2 地址，导致桌面请求返回 HTTP 502，而 Flutter 的 Bridge API 仍正常工作。先记录模拟器原有代理配置，再在本机调试时停用该代理：

    adb shell settings get global http_proxy
    adb shell settings put global http_proxy :0

返回 App 点击“重试”会取得新的单次票据；不要刷新已消费的票据 URL。故障排查应检查 WebView 的实际 HTTP 状态、页面画面和远程输入，桌面浏览器的移动视口测试不能代替 Android WebView 验证。

### 手机显示与操作

桌面默认按比例适应可视区域，保留验收时选择的分辨率；横竖屏切换只调整显示比例。顶部保留“适应屏幕”和“手机键盘”。单指轻点操作，单指拖动查看放大的桌面，双指滑动滚动远端内容，捏合缩放；双指轻点右键，快速连点双击，长按后拖动远端对象。

键盘默认收起。网页输入框，以及通过 AT-SPI 暴露可编辑焦点的 GTK、Tauri/WebKitGTK 原生输入框，点击后自动显示手机键盘。焦点检测只读取前台应用，并跨进程遍历 WebKit 的无障碍节点；查询超时、没有无障碍接口的应用保留“手机键盘”按钮。原生检测需要镜像包含 `at-spi2-core`、`python3-gi` 和 `gir1.2-atspi-2.0`；旧镜像需补齐这些包并更新准备环境使用的不可变 image ID。文字直接进入远端应用，支持中文输入法组合与候选词替换、emoji、退格，以及方向键、Ctrl/Alt/Shift 组合键和复制、剪切、粘贴。软键盘显示时保持桌面文字比例，并将最后点击的位置移入可视区域。

锁屏或切到后台后返回，客户端会重新取得桌面票据和控制权；顶部“重试”按钮也可重新连接。移动交互页面由 Bridge 提供，更新客户端时必须同时更新 Bridge，单独覆盖安装 APK 不会更新正在运行的旧版桌面页面。

Unicode 输入使用 Xpra 的 UTF-8 剪贴板通道，并按顺序执行后续编辑操作。剪贴板优先使用 UTF8_STRING；关闭 iframe 的剪贴板轮询，由外层输入框处理粘贴，避免旧剪贴板覆盖新复制的内容。Android 客户端将远端复制结果交给系统剪贴板。

文字过小时，双指捏合放大；点击“适应屏幕”回到完整画面。需要验收手机布局时，新建运行选择 mobile（390 × 844）；查看 PC 桌面时无需改变验收分辨率。
