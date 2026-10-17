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

手机可从项目或会话进入验收。项目内容来自已提交的 Git 快照。人工控制期间自动化指令被拒绝；控制租约 90 秒。桌面票据单次使用、30 秒有效；浏览器会话最长 1 小时。录像支持 Range 下载。卸载会停止活动验收并保留产物。

当前限制：最多 4 个活动验收；容器 2 CPU、2 GiB 内存、512 个进程；最长 4 小时；Bridge 每 30 秒清理过期运行。长期停机时，尚不能保证容器自行退出。

真实验证：

	X11BOX_TEST_IMAGE='sha256:<完整摘要>' cargo test real_container_acceptance_lifecycle -- --ignored --nocapture

移动浏览器测试还需 Node、Playwright 和 Chromium，并设置 NODE_PATH。测试必须验证连接、触摸、中文输入和控制撤销。Android/iOS 真机软键盘与网络切换仍需独立验收。

## Android 模拟器内的桌面连接

客户端以 --dart-define=OMNI_CODE_BRIDGE_URL=http://10.0.2.2:8787 连接宿主机。
在项目页进入“应用验收”，创建或打开运行，再点击“打开远程桌面”，桌面会在 App 内的 WebView 显示。

WebView 会使用 Android 系统代理。若模拟器设置了 10.0.2.2:7890 等代理，代理进程可能无法访问模拟器专用的 10.0.2.2 地址，导致桌面请求返回 HTTP 502，而 Flutter 的 Bridge API 仍正常工作。先记录模拟器原有代理配置，再在本机调试时停用该代理：

    adb shell settings get global http_proxy
    adb shell settings put global http_proxy :0

返回 App 点击“重试”会取得新的单次票据；不要刷新已消费的票据 URL。故障排查应检查 WebView 的实际 HTTP 状态、页面画面和远程输入，桌面浏览器的移动视口测试不能代替 Android WebView 验证。
