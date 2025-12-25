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

registries:
  - id: "docker"
    host: "docker-local.example.com"
    urls:
      - "https://registry-1.docker.io"
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
      - "https://registry-1.docker.io"
```

- `id`: 镜像站的唯一标识符,用于在其他地方引用.
- `host`: 当匹配到这个 Host 头的时候,使用加速地从下面的原始地址拉取
- `urls`: 镜像站的原始地址,可以有多个.

当然考虑到实际情况,你大概率这里是不能用原始地址的,你要想办法自己找个访问顺畅的反代.
