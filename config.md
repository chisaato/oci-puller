# 配置文件详解

例如,一份配置可能如下

```yaml
server:
  addr: ":9800"
  cache_dir: "./data/cache"

log:
  level: "debug"

downloader:
  workers: 4
  chunk_size: "5MB"
  min_speed: "32KB"
  stall_timeout: "20s"
  max_retries: 5

cache:
  max_size: "10GB"
  cleanup_interval: "5m"

registries:
  - id: "docker"
    host: "docker-local.example.com"
    urls:
      - "https://mirror-a.example.com"
      - "https://mirror-b.example.com"
  - id: "ghcr"
    host: "ghcr-local.example.com"
    urls:
      - "https://ghcr.io"
  - id: "lscr"
    host: "lscr-local.example.com"
    urls:
      - "https://lscr.io"
```

主要是两部分

## 下载器配置

因为是多线程下载,这里可以配置要开多少个线程,以及一个分块的大小.就像 Aria2 那样.

```yaml
downloader:
  workers: 4
  chunk_size: "5MB"
  min_speed: "32KB"
  stall_timeout: "20s"
  max_retries: 5
```

下面是 Gemini 给出的建议

### 1. 并发数 (`workers`)

- **建议范围**: 8 - 32
- **原理**: 增加并发数可以绕过某些 Registry 对单连接的限速（如 GFW 环境）。
- **调优**: 如果网络延迟很高，可以适当增加；如果机器 CPU 负载过高或频繁触发对方服务器的安全限制，应减小。由于我们优化了 Buffer 内存，增加 `workers` 对内存的压力极小。

### 2. 分块大小 (`chunk_size`)

- **建议值**: `5MB` - `50MB`
- **原理**: 较大的分块可以显著减少 HTTP 握手次数，提高网络吞吐率和连接稳定性。
- **警告**: 不要设置得过小（如 < 1MB），否则会产生极高的 HTTP 请求开销，导致下载变慢。
- **提示**: 较大的 `chunk_size` 并不意味着高内存占用。如“性能与资源优化”章节所述，数据的实际流转始终使用的是 32KB 的“小勺子”。

### 3. 黄金组合建议

- **低配环境**: `workers: 10`, `chunk_size: 10MB`
- **极速拉取 (推荐)**: `workers: 24`, `chunk_size: 20MB`

### 4. 长尾/卡顿检测 (`min_speed` / `stall_timeout`)

多线程下载到后期，剩余分片数量往往少于 Worker 数（例如只剩最后 2 个分片，却有 24 个 Worker），
如果其中一个分片恰好走上了一条拥塞/被限速的链路，它不会报错也不会断开连接，只是**极慢地**
往下走——这种情况单纯的连接超时（拨号、TLS 握手、响应头）完全发现不了，因为连接一直"活着"，
数据也在流动，只是流动得很慢，整个下载就会被这一个长尾分片拖住，直到 1 小时的总超时才会暴露问题。

为此下载器为每个分片维护了一个独立的吞吐量监控协程，按固定窗口采样，触发两种判定：

```yaml
downloader:
  min_speed: "32KB"     # 单个分片的最低瞬时速度，持续低于该值视为长尾
  stall_timeout: "20s"  # 单个分片允许的最长完全无新字节时间，视为连接已死
```

- **`stall_timeout`**：完全没有新字节到达超过该时长，判定连接已死。
- **`min_speed`**：连续 3 个采样窗口的平均速度低于该值，判定为长尾慢分片（要求连续命中而非单次采样，避免网络瞬时抖动误杀正常分片）。

命中任一条件后，该分片会被主动取消并交还给现有的重试机制（换一个连接重新下载），
不会影响其他正在正常下载的分片。两者均设为 `"0"` 可禁用对应检测，回退到只依赖连接层超时的行为。

`min_speed` 的合理值取决于你的实际带宽环境：默认的 32KB/s 是一个非常保守的下限（只用来兜底判断
"这个分片基本等于停滞"），如果你的上游本身就比较慢，不需要调大它；但如果你所在环境普遍能跑到几
MB/s，把它调到更接近正常速度的一部分（例如 200KB/s ~ 1MB/s）能更快地识别并淘汰长尾分片。

### 5. 分片重试次数 (`max_retries`)

单个分片下载失败（连接断开、HTTP 错误、长尾/卡顿检测主动取消）后，会换一个连接重试。
`max_retries` 限制每个分片允许的失败重试次数，超过后整个下载任务失败。

```yaml
downloader:
  max_retries: 5
```

- **默认值**: `5`
- **未配置或 <=0**: 回退到默认值 5
- **调优**: 上游不稳定时可适当增大；若希望尽快失败以便外层处理，可减小

## 镜像源配置

在这里决定你想要为哪些镜像站设置加速.

```yaml
registries:
  - id: "docker"
    host: "docker-local.example.com"
    urls:
      - "https://mirror-a.example.com"
      - "https://mirror-b.example.com"
```

- `id`: 镜像站的唯一标识符,用于在其他地方引用.
- `host`: 当匹配到这个 Host 头的时候,使用加速地从下面的原始地址拉取
- `urls`: 镜像站的上游地址列表,可以有一个或多个. 当前同一 `host` 下会先按轮询选择首个 URL,如果该上游在请求建立阶段失败,会在同一请求内继续尝试后续 URL.

上面这类配置表示同一个入口 Host 对应多个上游地址,当前行为是“请求级轮询 + 建立阶段失败切换”.

当然考虑到实际情况,你大概率这里是不能用原始地址的,你要想办法自己找个访问顺畅的反代. 即便配置了多个反代地址,目前也还不包含健康检查、加权分流、跨请求粘性这类更完整的调度能力.

## 缓存配置

OCI Puller 现在支持自动缓存管理和定期清理功能。

```yaml
cache:
  max_size: "10GB"
  cleanup_interval: "5m"
```

### 缓存大小限制 (`max_size`)

- **格式**: 支持 "GB", "MB", "KB", "B" 后缀
- **默认值**: "10GB"
- **说明**: 当缓存总大小超过此限制时，会自动清理最旧的项目

### 清理间隔 (`cleanup_interval`)

- **格式**: Go duration 格式（如 "5m", "1h", "30s"）
- **默认值**: "5m" (5分钟)
- **说明**: 后台垃圾回收运行的间隔

### 缓存管理命令

```bash
# 查看缓存统计信息
./oci-puller cache stats

# 清理指定数量的最旧项目
./oci-puller cache clean 10

# 自动清理（使使用率降到90%以下）
./oci-puller cache clean
```
