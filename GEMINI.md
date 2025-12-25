# OCI Puller 实现计划

## 背景

目标：基于 `design.md` 和 `README.md`，实现一个深度集成 OCI 分发规范的 Docker Pull 加速器。

## 设计分析与优化

基于 `design.md`，核心架构（拦截器 -> 缓存 -> 下载器 -> 协调器）非常稳固。
以下是我提出的具体优化和实现细节：

### 1. 区间管理 (关键)

原设计建议使用简单的 `downloaded map[int64]bool`。这对鲁棒性来说是不够的。

- **优化**: 使用专门的 **区间树 (Interval Tree)** 或 **段列表 (Segment List)**。
  - 合并后的区间可以简洁地表示“已下载范围”。
  - `MaxSafeOffset` 仅仅是 `intervals.First().End` (如果 `First().Start == 0`)。
  - 这能优雅地处理部分重试或可变分块大小的情况。

### 2. 优先级下载

Docker 客户端（以及流式读取器）需要立即获取文件的 _开头_ 部分。

- **优化**: 下载器的 Worker Pool 应该使用 **优先级队列**。
  - 更接近 `CurrentReadOffset` (或 0) 的 Chunk 优先级更高。
  - Worker 1 总是尝试获取 `[0, ChunkSize]`。
  - 这能最小化客户端在请求开始时的“阻塞”时间。

### 3. 优雅降级与特性检测

- **优化**: 获取大小的通用 `HEAD` 请求同时也应该检查 `Accept-Ranges: bytes`。
  - 如果缺失，禁用多线程下载并回退到简单的 `ProxyPass` (直接从上游流式传输到客户端 + 磁盘)。

### 4. 缓存驱逐 (LRU)

`README.md` 提到了 LRU。

- **提案**:
  - 维护一个 `sqlite` 数据库或简单的嵌入式 KV (如 `bbolt`) 来跟踪访问时间。
  - 或者，更简单点：使用文件系统的 `atime` (如果可靠) 或 sidecar 元数据文件。
  - _决策_: 对于 V1，我们可能只实现一个基于 `modtime` 或元数据记录删除最旧文件的“容量上限 (Size Cap)”。

### 5. 零拷贝与稀疏文件

- **确认**: Linux `os.Truncate` 创建稀疏文件。
- **读取路径**: 使用文件的 `io.Copy`。Go 标准库优化了从 `*os.File` 到 `net.TCPConn` 的 `io.Copy`，尽可能使用 `sendfile`。这非常棒。

## 实现路线图

### 第一阶段：核心原语 (Core Primitives) - [x] 已完成

- [x] **IntervalManager**: 用于合并 [start, end] 范围的数据结构 (实现于 `pkg/interval`)。
- [x] **SparseFile**: 文件操作的包装器 (直接使用 os.File + Truncate)。
- [x] **Coordinator**: `sync.Cond` 逻辑 + IntervalManager (实现于 `pkg/coordinator`)。

### 第二阶段：引擎 (Engine) - [x] 已完成

- [x] **Downloader**:
  - Worker pool (实现于 `pkg/downloader`)。
  - 任务队列 (带优先级) (实现于 `pkg/downloader`)。
  - HTTP Range 客户端。
- [x] **DownloadManager**: Singleflight + 活跃传输映射 (实现于 `pkg/manager`)。

### 第四阶段：标准化与扩展 (Standardization & Extension)

- [ ] **OCI Layout Compliance**: 重构存储层以兼容 OCI Image Layout 规范 (`blobs/alg/hash`, `oci-layout` file)。
- [ ] **LRU Cache & GC**: 实现基于 Sidecar Metadata 的缓存清理。
- [ ] **TLS 伪装**: 引入 `utls` 避免指纹识别。

## 下一步

1. 初始化 Go module (已完成)。
2. 创建 `pkg/interval` 和 `pkg/coordinator`。
