# OCI Puller 仓库指引

## 先看哪里
- 先读 `README.md` 了解产品定位与部署方式，但**不要直接相信其中端口示例**。
- 真正的源码入口是 `main.go -> cmd.Execute() -> cmd/root.go`。
- 服务主链路看 `cmd/server.go`；缓存/下载编排看 `pkg/manager/manager.go`；边下边播同步看 `pkg/coordinator/coordinator.go`；并行分块下载看 `pkg/downloader/downloader.go`。

## 这个项目实际怎么接线
- `cmd/root.go` 负责全局初始化：读取 `--config/-c`、当前目录 `config.yaml`、`OCI_` 前缀环境变量，然后加载 `config.GlobalConfig` 并初始化日志。
- `cmd/server.go` 是服务入口，不是普通 library：
  - 按 `Host` 从 `registries[].host` 选择上游。
  - `GET /.../blobs/sha256:*` 走加速链路。
  - `GET/HEAD /.../manifests/...` 走 manifest 拦截，并异步更新本地 OCI index。
  - 其他请求直接反代透传。
- Blob 请求主链路：`handleBlob -> manager.GetBlobStream -> (缓存命中 | 复用活跃下载 | startDownload)`；下载器持续写临时文件，协调器向读端放行已完成区间，实现“边下边播”。
- `pkg/manager` 不只是缓存封装：它还负责活跃下载复用、临时文件/最终文件切换、后台 GC、`cache.db` 元数据存储和 OCI layout 初始化。

## 运行与验证命令
- 构建主程序：`go build .`
- 启动服务：`go run . server` 或构建后二进制 `./oci-puller server`
- 指定配置：`./oci-puller server -c /path/to/config.yaml`
- 查看缓存统计：`./oci-puller cache stats`
- 手动清理缓存：`./oci-puller cache clean 10`
- 自动清理到阈值以下：`./oci-puller cache clean`
- 基础验证优先顺序：
  1. `gofmt -w <changed files>`
  2. `go test ./...`
  3. `go vet ./...`

## 配置与文档的几个“别猜”点
- `cmd/root.go` 默认从**当前工作目录**读取 `config.yaml`；不是自动从 `/etc` 或仓库根之外找。
- 环境变量前缀是 `OCI_`；文档和 compose 里的示例也应写成 `OCI_LOG_LEVEL=...` 这种形式。
- 现在 `config.yaml` 样例、`compose.yml` 映射、README 反代示例、`Dockerfile EXPOSE` 与 `pkg/config/config.go` 的代码默认端口都统一为 `9800`；改端口时要一起改，不要只动其中一个。
- 下载器代码默认 `workers=16`，而 `config.yaml` 样例写的是 `4`；不要把样例值误当成程序默认值。
- `cache.max_size` 与 `cache.cleanup_interval` 在 `config.md` 有说明，但 `config.yaml` 样例里未显式写出；代码默认分别是 `10GB` 和 `5m`。

## 容器与部署注意事项
- 仓库内实际编排文件名是 `compose.yml`，不是 `docker-compose.yml`。
- `Dockerfile` 当前 `EXPOSE 9800`，并且与 README、`compose.yml`、`config.yaml`、代码默认端口保持一致；涉及容器端口时，先同时检查这几处，不要只改一个。
- `Dockerfile` 入口是 `ENTRYPOINT ["/oci-puller", "server"]`，说明容器默认直接跑服务模式。

## 目录边界
- `cmd/`: CLI 与 HTTP 服务装配层。
- `pkg/config`: 配置结构与默认值。
- `pkg/manager`: 下载编排、缓存命中、活跃任务复用、GC。
- `pkg/coordinator`: 下载进度与读取进度同步，支撑流式返回。
- `pkg/downloader`: 多 worker Range 下载执行层。
- `pkg/storage`: bbolt 元数据存储与缓存统计/淘汰。
- `cmd/manual-pull/main.go`: 独立实验入口，不是正式子命令；不要把这里的硬编码地址当成正式行为。

## 代理在这里最容易做错的事
- 不要把 `config.yaml` 的样例值当成代码默认值，尤其是 `addr` 和 `downloader.workers`。
- 不要把样例配置值和代码默认值混为一谈；这个仓库虽然端口已统一到 `9800`，但 `downloader.workers` 等字段仍存在“样例值 ≠ 代码默认值”。
- 不要只改文档不改代码，或只改 `Dockerfile` 不改 `compose.yml` / `config.yaml` / README / `pkg/config/config.go`；端口相关内容必须整体同步。
- 不要把 `cache` 子命令漏掉；它是仓库里真实存在的运维入口。

## 语言与输出约束
- 本仓库现有指令要求：代理回复、代码注释、文档、用户交互、错误消息、日志都使用中文。
