# omni-relay-connector

供 OmniStream Core / Omni Code Bridge 嵌入的 Rust Connector。仅电脑端需要依赖；手机和网页客户端通过 Relay API 连接。

- start(Config) 使用宿主 Tokio 运行时，返回 ConnectorHandle。
- subscribe() 观察连接/在线/重连/停止状态。
- stop() 请求取消，shutdown().await 等待清理；丢弃句柄也会取消。
- RelayClient 提供浏览器配对、服务登记和提供方管理。
- Host 统一私有配置、配对、账号核对、启停和设备策略；resume() 与 shutdown() 随宿主生命周期调用。宿主必须验证控制请求，禁止隧道访问管理接口。
- host_cli() 提供启动前配置；状态不包含凭据。嵌入模式每次启动生成上游身份，仅按摘要验证，不把该身份写磁盘。
- Session::load/save 支持私有身份文件；凭据不实现 Debug，状态事件不包含凭据。

完整接入步骤见 omni-relay 仓库的 docs/integration.md。crate 尚未发布到 crates.io，可使用本地路径依赖。

当前由 OmniStream Core 和 Omni Code Bridge 作为源码依赖使用。scripts/sync-connectors.mjs 可同步源码并生成内容摘要，--check 校验副本，避免公开产品构建依赖私有 Git 凭据。
