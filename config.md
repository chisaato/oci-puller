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
