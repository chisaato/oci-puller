# OCI Puller 设计文档

OCI Puller 是一个旨在加速 Docker Pull 过程的代理工具。

## 1. 核心特性

- **多线程并行下载**: 将客户端的单线程拉取转换为后端的并发 Range 请求。
- **高效磁盘缓存**: 支持带 TTL 的自动清理或 LRU 淘汰策略。
- **智能代理**: 仅对大文件（Blobs）开启加速，元数据（Manifests）透传。
- **灵活路由与负载均衡**: 支持多镜像源配置与故障转移。
- **零拷贝技术**: 生产环境下尽可能利用 `sendfile` 提升性能。

## 2. 核心架构设计

为了满足“Docker Pull 加速”和“磁盘缓存”，我们需要一个中间层来接管 Docker Client 的请求。架构逻辑如下：

1.  **拦截层 (Interceptor):** 识别 `GET /v2/<name>/blobs/<digest>` 请求。
2.  **缓存层 (Cache Layer):** 检查磁盘上是否已有该 Blob。
    - **Hit:** 直接通过 `http.ServeFile` 返回文件（利用 OS 的 `sendfile` 零拷贝特性，性能极高）。
    - **Miss:** 进入下载引擎。
3.  **下载引擎 (Download Engine):**
    - 使用 `HEAD` 请求获取 Content-Length。
    - **预分配文件:** 在磁盘上创建一个空文件（Sparse File，稀疏文件），大小等于 Content-Length。
    - **分片规划:** 将文件切分为 N 个 Chunk（例如每个 20MB）。
    - **并发下载:** 启动 Worker Pool，利用 `Range: bytes=start-end` 头并行下载。
    - **乱序写入:** 哪个 Chunk 先下完，就通过 `WriteAt` 写入到文件的对应偏移量位置。
4.  **流式响应 (Stream Coordinator):**
    - 这是最难的部分。Docker Client 需要顺序的数据流（0, 1, 2...），但后端下载是乱序完成的。
    - 需要设计一个**同步器 (Coordinator)**，它监视文件的下载进度，并允许 HTTP Response Writer 像“追赶者”一样读取文件，直到读到尚未下载的区域时阻塞等待。

---

### 2. 关键技术点

#### A. 稀疏文件 (Sparse Files) 与 WriteAt

不要在内存中拼接 Buffer，内存会爆。
在 Linux (ext4/xfs) 上，你可以创建一个 1GB 的文件，但如果还没写入数据，它几乎不占磁盘空间。

- **Go 实现:** `os.Create`, `f.Truncate(size)`.
- **写入:** 使用 `file.WriteAt(data, offset)`，这是线程安全的（底层是 `pwrite` 系统调用），允许多个 Goroutine 同时写同一个文件的不同区域。

#### B. Singleflight (防缓存击穿)

当 10 个 Docker 客户端同时拉取同一个新的 Image Layer 时，你不能启动 10 次多线程下载。

- **工具:** `golang.org/x/sync/singleflight`。
- **作用:** 确保针对同一个 Digest 的下载任务，全局只有一个在运行，其他请求等待或共享结果。

#### C. 读写同步 (The Barrier)

如何实现“边下边播”？
我们需要一个数据结构维护**“当前连续已下载的最大偏移量” (Continuous Offset)**。

- **Writer (Downloader):** 每下载完一个 Chunk，通知 Coordinator。Coordinator 检查是否填补了之前的空洞，更新 Continuous Offset，并 `Broadcast` 唤醒 Reader。
- **Reader (Response):** 循环读取文件。如果 `ReadOffset < ContinuousOffset`，直接读磁盘；如果 `ReadOffset == ContinuousOffset`，阻塞等待 `Cond` 信号。

---

### 3. 详细设计与代码思路

#### 步骤一：文件存储结构

建议目录结构：

- `/cache/blobs/<sha256>/<digest>` (下载完成的最终文件)
- `/cache/temp/<digest>` (正在下载的临时文件)

#### 步骤二：核心协调器 (Coordinator) 实现

这是实现的核心，我写了一个简化的 Go 原型来展示如何协调**多线程乱序写**和**单线程顺序读**。

```go
package main

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// BlobCoordinator 负责协调下载进度和读取进度
type BlobCoordinator struct {
	mu          sync.Mutex
	cond        *sync.Cond
	file        *os.File
	fileSize    int64

	// 记录每个 Chunk 的下载状态
	// 简单起见，这里假设我们按固定大小分片，实际上可以用区间树(Interval Tree)来管理
	chunkSize   int64
	downloaded  map[int64]bool // 分片索引 -> 是否完成

	// 当前连续可读的最大字节偏移量
	maxSafeOffset int64
	done          bool
	err           error
}

func NewCoordinator(f *os.File, size int64, chunkSize int64) *BlobCoordinator {
	bc := &BlobCoordinator{
		file:       f,
		fileSize:   size,
		chunkSize:  chunkSize,
		downloaded: make(map[int64]bool),
	}
	bc.cond = sync.NewCond(&bc.mu)
	return bc
}

// MarkChunkDone 由下载 Worker 调用，表示某一段写完了
func (bc *BlobCoordinator) MarkChunkDone(chunkIndex int64) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.downloaded[chunkIndex] = true

	// 重新计算连续可读区域
	// 这是一个简化的算法，实际上应该维护一个确权的偏移量（verified offset）
	currentChunkIdx := bc.maxSafeOffset / bc.chunkSize
	for {
		if bc.downloaded[currentChunkIdx] {
			// 如果当前块已下载，安全偏移量推进到下一块
			bc.maxSafeOffset = (currentChunkIdx + 1) * bc.chunkSize
			if bc.maxSafeOffset > bc.fileSize {
				bc.maxSafeOffset = bc.fileSize
			}
			currentChunkIdx++
		} else {
			break
		}
	}

	// 唤醒所有等待读取的客户端
	bc.cond.Broadcast()
}

func (bc *BlobCoordinator) SetDone(err error) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.done = true
	bc.err = err
	bc.cond.Broadcast()
}

// Reader 这是一个实现了 io.Reader 的封装，给 http.ResponseWriter 使用
type BlobReader struct {
	bc     *BlobCoordinator
	offset int64
}

func (r *BlobReader) Read(p []byte) (n int, err error) {
	r.bc.mu.Lock()
	defer r.bc.mu.Unlock()

	for {
		// 1. 如果有错误（下载失败），返回错误
		if r.bc.err != nil {
			return 0, r.bc.err
		}

		// 2. 计算有多少数据是安全可读的（已下载且连续）
		available := r.bc.maxSafeOffset - r.offset

		if available > 0 {
			// 有数据可读，解锁去读文件
			// 注意：这里解锁是为了不阻塞其他写入者（Writer）或读取者（Reader）
			r.bc.mu.Unlock()

			// 限制读取长度，不能超过 available
			readLen := int64(len(p))
			if readLen > available {
				readLen = available
			}

			// 从文件中读取
			n, err = r.bc.file.ReadAt(p[:readLen], r.offset)
			r.offset += int64(n)

			// 重新加锁以维持 defer Unlock
			r.bc.mu.Lock()

			// ReadAt 到达文件末尾会返回 EOF，但在我们的场景里，
			// 只有当下载完成（done）且读到了 fileSize 时才是真正的 EOF
			if err == io.EOF {
				if r.bc.done && r.offset == r.bc.fileSize {
					return n, io.EOF
				}
				// 否则忽略 EOF，因为还要等新数据
				err = nil
			}
			return n, err
		}

		// 3. 如果没有新数据
		if r.bc.done {
			return 0, io.EOF
		}

		// 4. 等待下载进度更新
		r.bc.cond.Wait()
	}
}
```

#### 步骤三：整合逻辑 (伪代码)

```go
func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    digest := extractDigest(r)

    // 1. 检查最终缓存
    if fileExists(finalPath(digest)) {
        http.ServeFile(w, r, finalPath(digest))
        return
    }

    // 2. 使用 Singleflight 防止并发下载同一个 Blob
    // 注意：这里的 Key 应该是 digest
    res, err, shared := h.sfGroup.Do(digest, func() (interface{}, error) {
        // --- 可以在这里启动下载任务 ---
        // 但为了实现流式传输，我们需要把 Coordinator 暴露出去
        // 比较好的做法是：Singleflight 仅用于创建 Coordinator，
        // 或者使用一个专门的 Manager 来管理正在进行的下载。
        return h.startDownloadManager(digest)
    })

    coordinator := res.(*BlobCoordinator)

    // 3. 设置响应头（Response Headers）
    w.Header().Set("Content-Length", fmt.Sprintf("%d", coordinator.fileSize))
    w.Header().Set("Content-Type", "application/octet-stream")

    // 4. 创建 Reader 并复制（Copy）到响应
    reader := &BlobReader{bc: coordinator, offset: 0}
    io.Copy(w, reader)
}
```

#### 步骤四：下载器实现 (Downloader)

下载器需要运行在后台（Goroutine 中）：

1.  **Head Request:** 获取 `Content-Length`.
2.  **File Setup:**
    ```go
    f, _ := os.Create(tempPath)
    f.Truncate(contentLength)
    ```
3.  **Worker Pool:**
    使用 `errgroup` 来管理并发。

    ```go
    g := new(errgroup.Group)
    chunkSize := 10 * 1024 * 1024 // 10 兆字节 (10MB)

    for i := int64(0); i < contentLength; i += chunkSize {
        start := i
        end := start + chunkSize - 1
        if end >= contentLength { end = contentLength - 1 }

        g.Go(func() error {
            // 发起 HTTP Range 请求
            req, _ := http.NewRequest("GET", upstreamUrl, nil)
            req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
            resp, _ := http.DefaultClient.Do(req)
            defer resp.Body.Close()

            // 写入文件指定位置
            // 注意：这里需要 buffer，不能一次性全读进内存再写
            buf := make([]byte, 32*1024)
            localOffset := start
            for {
                n, err := resp.Body.Read(buf)
                if n > 0 {
                    f.WriteAt(buf[:n], localOffset)
                    localOffset += int64(n)
                }
                if err != nil { break }
            }

            // 标记该块完成 (这里简化了，实际上应该更细粒度或者在 Loop 结束后标记)
            // 更好的做法是把 WriteAt 包装一下，每写一点就更新 Coordinator 的区间树 (Interval Tree)
            coordinator.MarkChunkDone(start / chunkSize)
            return nil
        })
    }

    if err := g.Wait(); err != nil {
        coordinator.SetDone(err)
        // 删除临时文件
    } else {
        coordinator.SetDone(nil)
        // 移动临时文件到最终缓存目录 (原子重命名)
        os.Rename(tempPath, finalPath)
    }
    ```

### 4. 优化与坑

1.  **Range 请求被拒绝:** 有些 Registry (如 Harbor 的某些配置或 S3 后端) 可能对 Range 支持不完善，需要做 Fallback 处理（退化为单线程下载）。
2.  **区间管理:** 上面的代码用 `ChunkIndex` 做状态管理太简单了。如果网络断开重连，可能只下了一半。建议引入 **Interval Tree** 或 **Bitmap** 来管理 `[0, 100) done`, `[200, 300) done`，从而精确计算 `maxSafeOffset`（即 Interval Tree 中从 0 开始的最长连续区间的右边界）。
3.  **磁盘 I/O 瓶颈:** 虽然是多线程下载，但写入的是同一个物理磁盘。如果磁盘 IOPS 低，多线程反而会因为磁头跳动（机械硬盘）变慢。SSD 没问题。
4.  **优雅关闭:** 如果 Client 断开了连接，代理服务器应该继续下载完文件（为了缓存），还是中断下载？通常建议继续下载完，除非文件巨大且长时间无人访问。
5.  **Manifest 处理:** Manifest 文件（JSON）很小，不要多线程，直接透传并缓存（注意 `Docker-Content-Digest` 校验）。

### 5. 总结

这种方式既利用了 Go 强大的并发能力（Goroutine + Channel/Cond），又利用了操作系统的文件缓存机制，是构建高性能下载代理的标准做法。

---

## 4. 负载均衡与路由

### 4.1 路由规则

基于 Host 头决定代理哪个镜像站,这给了反向代理极大的灵活性.

### 4.2 配置示例

```yaml
registries:
  - id: docker
    # 匹配这个头就反代这个 registry
    host: docker.example.com
    urls:
      - https://registry-1.docker.io
      - https://xxmirror.example.com
  - id: ghcr
    host: ghcr.example.com
    urls:
      - https://ghcr.io
      - https://xxghcrmirror.example.io
```

## 5. 性能与资源优化

为了在高并发下保持极低的内存占用，我们对下载缓冲区进行了深度的优化设计：

### 5.1 内存与缓冲区的关系

我们严格区分了 **任务切片 (Chunk Size)** 和 **内存缓冲区 (Buffer Size)** 的概念：

- **Chunk Size (默认 10MB+)**: 决定了每个 Worker 的任务粒度。较大的 Chunk Size 保证了 HTTP 连接的长久复用，避免了频繁建立连接的开销，从而保证了下载速度。
- **Buffer Size (默认 32KB)**: 决定了数据在内存中停留的大小。我们将原先常用的 5MB 缓冲区缩小至标准的 32KB (syscall page size 级别)。

**针对内存的影响对比：**

| 设置                     | 单 Worker 内存 | 64 并发总内存 | GC 压力  | 适用场景                |
| :----------------------- | :------------- | :------------ | :------- | :---------------------- |
| **优化前 (5MB Buffer)**  | 5MB            | **320MB**     | 高       | 仅限高性能服务器        |
| **优化后 (32KB Buffer)** | 32KB           | **2MB**       | **极低** | 任何环境 (包括低配容器) |

通过这种“大任务粒度 + 小搬运勺子”的设计，实现了**在极低内存占用下的满速下载**。

---

## 5. 实现状态追踪

以下是基于设计要求的实现情况检查清单（更新于 2025-12-24）：

### 核心组件

- [x] **拦截层 (Interceptor)**: 已在 `main.go` 实现。支持基于 Host 头的路由匹配，识别并拦截 `/blobs/sha256:` 请求。
- [x] **缓存层 (Cache Layer)**: 已在 `pkg/manager` 实现。支持 `blobs/` 目录缓存检查和活跃任务合并（Singleflight 模式）。
- [x] **下载引擎 (Downloader)**:
  - [x] **稀疏文件预分配**: 通过 `f.Truncate(size)` 实现。
  - [x] **Worker Pool 并发下载**: 使用 `errgroup` 管理并发。支持断点续传。
  - [!] **优先级调度**: **半完成**。目前使用简单的 `NextClear(0)` 扫描实现顺序倾向，但尚未支持基于客户端读取偏移的动态优先级。
- [x] **同步器 (Coordinator)**:
  - [x] **区间管理**: 已实现 `pkg/interval`。使用有序区间列表（Segment List）高效管理已下载范围。
  - [x] **边下边播**: 通过 `sync.Cond` 实现。

### 进阶优化与遗留项

- [x] **Singleflight**: 本机实现，防止对同一 Blob 的并发拉取。
- [x] **长尾/低速分片检测**: 已在 `pkg/downloader` 实现。此前只有连接建立阶段的超时（拨号/TLS 握手/响应头），
  无法发现"连接存活但吞吐量骤降"的长尾分片；现在为每个分片单独统计单位时间吞吐量，支持 `stall_timeout`
  （完全无进展）与 `min_speed`（持续低速）两种触发条件，命中后主动取消并交还给现有重试机制换连接重试。
- [x] **零拷贝**: 命中缓存时直接 `os.Open` + `io.Copy`。流式模式下因包装层存在，无法直接利用 `sendfile`。
- [-] **优雅降级**: **存在问题**。当前代码检测到不支持 Range 时会尝试多线程，这会导致数据混乱。需重构为单线程透传。
- [ ] **LRU 缓存清理**: **未完成**。目前仅实现了基本的文件存储。
- [ ] **TLS 伪装**: **未完成**。尚未集成 `utls`。
- [ ] **增强负载均衡**: **未完成**。目前仅支持一对一的 Host 到 Upstream 映射。
