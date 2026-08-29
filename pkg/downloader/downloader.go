package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"oci-puller/pkg/coordinator"
	"oci-puller/pkg/logger"

	"github.com/bits-and-blooms/bitset"
	"golang.org/x/sync/errgroup"
)

// Downloader 负责多线程分片下载
type Downloader struct {
	url        string
	file       *os.File
	coord      *coordinator.Coordinator
	chunkSize  int64
	workers    int
	headers    http.Header
	clientPool *ClientPool

	// 长尾/卡顿检测：仅靠连接层超时（拨号、TLS 握手、响应头）无法发现
	// “连接活着但单位时间内吞吐量极低”的分片，需要单独的速度统计。
	minSpeedBytesPerSec int64         // 单个分片持续低于该速度视为长尾，0 表示禁用
	stallTimeout        time.Duration // 单个分片完全无新字节的最长时间，0 表示禁用
	speedCheckInterval  time.Duration // 速度采样窗口
	maxRetries          int           // 单个分片失败后的最大重试次数

	// Bitmap Scheduling
	bitmap    *bitset.BitSet
	numChunks uint
	mu        sync.Mutex // 保护 bitmap 和 retries
	retries   map[uint]int
}

// New 创建一个新的下载器
func New(url string, f *os.File, c *coordinator.Coordinator, headers http.Header) *Downloader {

	// 设置 ChunkSize 为 10M
	chunkSize := 1024 * 1024 * 10

	return &Downloader{
		url:                 url,
		file:                f,
		coord:               c,
		chunkSize:           int64(chunkSize),
		workers:             64,
		headers:             headers,
		minSpeedBytesPerSec: 32 * 1024, // 默认 32KB/s
		stallTimeout:        20 * time.Second,
		speedCheckInterval:  3 * time.Second,
		maxRetries:          5,
	}
}

// SetWorkers 设置并发工人数
func (d *Downloader) SetWorkers(n int) {
	d.workers = n
}

// SetChunkSize 设置分片大小
func (d *Downloader) SetChunkSize(size int64) {
	d.chunkSize = size
}

// SetMinSpeed 设置单个分片的最低瞬时速度（字节/秒）。
// 持续（连续 3 个采样窗口）低于该速度的分片会被判定为长尾并中断重试。0 表示禁用该检测。
func (d *Downloader) SetMinSpeed(bytesPerSec int64) {
	d.minSpeedBytesPerSec = bytesPerSec
}

// SetStallTimeout 设置单个分片允许的最长完全无新字节时间。0 表示禁用该检测。
func (d *Downloader) SetStallTimeout(dur time.Duration) {
	d.stallTimeout = dur
}

// SetMaxRetries 设置单个分片失败后的最大重试次数。
func (d *Downloader) SetMaxRetries(n int) {
	d.maxRetries = n
}

// Start 开始下载任务
// 会阻塞直到下载完成或出错
func (d *Downloader) Start(ctx context.Context) error {
	totalSize := d.coord.Size()
	logger.S.Debugw("下载器正在启动", "url", d.url, "totalSize", totalSize, "workers", d.workers)

	// 0. 初始化 Client Pool (每个 Worker 一个独立的 Client/Transport)
	d.clientPool = NewClientPool(d.workers)

	// 1. 初始化 Bitmap
	d.numChunks = uint((totalSize + d.chunkSize - 1) / d.chunkSize)
	d.bitmap = bitset.New(d.numChunks)
	d.retries = make(map[uint]int)

	// 2. 检查已完成的区间 (Resume support)
	preFilled := 0
	for i := uint(0); i < d.numChunks; i++ {
		start, end := d.getChunkRange(i, totalSize)
		if d.coord.IsDownloaded(start, end) {
			d.bitmap.Set(i)
			preFilled++
		}
	}
	logger.S.Debugf("位图初始化完成：共 %d 个分片，其中 %d 个已完成", d.numChunks, preFilled)

	// 3. 启动 Worker
	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i < d.workers; i++ {
		workerID := i
		// workerClient := d.clientPool.GetWorkerClient(workerID)
		g.Go(func() error {
			//logger.S.Debugw("Worker started", "workerID", workerID)
			for {
				// 获取任务
				d.mu.Lock()
				// 寻找第一个未完成的分片 (Scanner logic)
				// NextClear 返回索引和 found bool
				idx, found := d.bitmap.NextClear(0)
				if !found || idx >= d.numChunks {
					d.mu.Unlock()
					//logger.S.Debugw("Worker finished (no more jobs)", "workerID", workerID)
					return nil
				}
				// 标记为“已分配/处理中” (Pending)
				d.bitmap.Set(idx)
				d.mu.Unlock()

				// 计算范围
				start, end := d.getChunkRange(idx, totalSize)

				logger.S.Debugw("工人开始领取任务",
					"workerID", workerID,
					"chunkIdx", idx,
					"start", start,
					"end", end)

				// 复用 Client Pool 中的连接，避免频繁创建和销毁
				client := d.clientPool.GetWorkerClient(workerID)
				// 执行下载
				err := d.downloadChunk(ctx, client, start, end)
				// client.CloseIdleConnections() // 不要关闭，以便复用于下一个 Chunk

				if err != nil {
					// 检查是否重试
					d.mu.Lock()
					d.retries[idx]++
					retryCount := d.retries[idx]
					maxRetries := d.maxRetries
					d.mu.Unlock()

					if retryCount > maxRetries {
						logger.S.Errorw("工人任务执行完毕，但由于超过最大重试次数而失败",
							"workerID", workerID,
							"chunkIdx", idx,
							"error", err,
							"retryCount", retryCount)
						// 失败，清除标记(虽然这里要退出了，但保持一致性)
						d.mu.Lock()
						d.bitmap.Clear(idx)
						d.mu.Unlock()
						return err
					}

					logger.S.Warnw("工人任务执行失败，正在重试",
						"workerID", workerID,
						"chunkIdx", idx,
						"error", err,
						"retryCount", retryCount)

					// 简单的退避策略
					time.Sleep(time.Second * time.Duration(retryCount))

					// 清除标记，允许重新调度
					d.mu.Lock()
					d.bitmap.Clear(idx)
					d.mu.Unlock()

					continue
				}
				logger.S.Debugw("工人成功完成任务", "workerID", workerID, "chunkIdx", idx)
			}
		})
	}

	// 4. 等待完成
	err := g.Wait()

	// 通知协调器结束
	d.coord.SetDone(err)

	if err != nil {
		logger.S.Errorw("下载器执行过程中出错", "error", err)
	} else {
		logger.S.Info("下载器圆满完成任务")
	}

	return err
}

func (d *Downloader) getChunkRange(idx uint, totalSize int64) (int64, int64) {
	start := int64(idx) * d.chunkSize
	end := start + d.chunkSize - 1
	if end >= totalSize {
		end = totalSize - 1
	}
	return start, end
}

func (d *Downloader) downloadChunk(ctx context.Context, client *http.Client, start, end int64) error {
	// 使用可单独取消的子 Context：一旦检测到该分片长尾/卡顿，
	// 只取消这一个分片的请求，不影响其他正在正常下载的 Worker。
	chunkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(chunkCtx, "GET", d.url, nil)
	if err != nil {
		return err
	}

	// 复制原始 Headers
	for k, v := range d.headers {
		req.Header[k] = v
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
	req.Header.Set("Range", rangeHeader)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// 启动长尾/卡顿监控：只靠连接层超时（拨号/握手/响应头）无法发现
	// “TCP 连接仍然存活、但单位时间内吞吐量骤降”的长尾分片，这里用一个
	// 独立的采样 goroutine 统计吞吐量，触发条件满足时取消 chunkCtx。
	watchdog := newChunkWatchdog(cancel, d.minSpeedBytesPerSec, d.stallTimeout, d.speedCheckInterval, start, end)
	go watchdog.run(chunkCtx)
	defer watchdog.stop()

	// TODO: 配置文件化. 默认使用 32KB (标准 io.Copy 大小)，兼顾内存和 CPU
	buf := make([]byte, 32*1024)
	currentOffset := start
	//lastLoggedProgress := start

	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			// 写入文件
			_, writeErr := d.file.WriteAt(buf[:n], currentOffset)
			if writeErr != nil {
				return writeErr
			}

			// 更新 Coordinator
			endOffset := currentOffset + int64(n) - 1
			d.coord.MarkDownloaded(currentOffset, endOffset)

			currentOffset += int64(n)
			watchdog.recordProgress(int64(n))

			// 每下载 1MB 记录一次 DEBUG 日志
			//if currentOffset-lastLoggedProgress > 1024*1024 {
			//	logger.S.Debugw("Chunk download progress",
			//		"start", start,
			//		"current", currentOffset,
			//		"end", end)
			//	lastLoggedProgress = currentOffset
			//}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			// 如果是 watchdog 触发的取消，用更明确的长尾/卡顿错误替代原始的
			// "context canceled"，方便日志排查和区分于普通网络错误。
			if watchdogErr := watchdog.err(); watchdogErr != nil {
				return watchdogErr
			}
			return readErr
		}
	}
	return nil
}

// chunkWatchdog 统计单个分片在下载过程中的吞吐量，用来发现“连接没断，
// 但已经慢到没有意义”的长尾分片，并主动取消让上层重试（换一个新连接）。
//
// 两个独立的触发条件：
//  1. stallTimeout：完全没有新字节达到一定时长 —— 判定连接已经死掉。
//  2. minSpeedBytesPerSec：连续多个采样窗口的平均速度低于阈值 —— 判定为
//     长尾慢分片。要求“连续多次”而非单次采样是为了避免瞬时抖动误杀。
type chunkWatchdog struct {
	cancel       context.CancelFunc
	minSpeed     int64
	stallTimeout time.Duration
	interval     time.Duration
	start, end   int64

	progress     atomic.Int64 // 当前采样窗口内累计读到的字节数
	lastProgress atomic.Int64 // 最近一次有新字节到达的时间 (UnixNano)
	lowSpeedHits int          // 连续低速采样次数，仅在 run 所在的 goroutine 中访问

	stopCh     chan struct{}
	stopOnce   sync.Once
	triggerErr atomic.Value // error
}

const lowSpeedStrikeThreshold = 3 // 连续 3 个采样窗口低速才判定为长尾，避免抖动误杀

func newChunkWatchdog(cancel context.CancelFunc, minSpeed int64, stallTimeout, interval time.Duration, start, end int64) *chunkWatchdog {
	w := &chunkWatchdog{
		cancel:       cancel,
		minSpeed:     minSpeed,
		stallTimeout: stallTimeout,
		interval:     interval,
		start:        start,
		end:          end,
		stopCh:       make(chan struct{}),
	}
	w.lastProgress.Store(time.Now().UnixNano())
	return w
}

// recordProgress 由读取循环在每次成功读到数据后调用
func (w *chunkWatchdog) recordProgress(n int64) {
	w.progress.Add(n)
	w.lastProgress.Store(time.Now().UnixNano())
}

func (w *chunkWatchdog) stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}

func (w *chunkWatchdog) err() error {
	if e, ok := w.triggerErr.Load().(error); ok {
		return e
	}
	return nil
}

func (w *chunkWatchdog) run(ctx context.Context) {
	if w.minSpeed <= 0 && w.stallTimeout <= 0 {
		return
	}

	// 采样粒度不能盖过 stallTimeout：如果固定用 speedCheckInterval（默认 3s）
	// 采样，配置一个更短的 stallTimeout 也不会更快触发，实际卡顿超时会被
	// 拖长到最近一次采样点。这里让采样间隔自适应地不超过 stallTimeout 的一半。
	effectiveInterval := w.interval
	if effectiveInterval <= 0 {
		effectiveInterval = 3 * time.Second
	}
	if w.stallTimeout > 0 && w.stallTimeout/2 < effectiveInterval {
		effectiveInterval = w.stallTimeout / 2
	}
	const minSampleInterval = 100 * time.Millisecond
	if effectiveInterval < minSampleInterval {
		effectiveInterval = minSampleInterval
	}

	ticker := time.NewTicker(effectiveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			windowBytes := w.progress.Swap(0)

			// 1. 完全无进展检测：无论速度阈值是否启用，长时间零字节都视为连接已死
			if w.stallTimeout > 0 {
				idle := time.Duration(time.Now().UnixNano() - w.lastProgress.Load())
				if idle > w.stallTimeout {
					w.triggerErr.Store(fmt.Errorf(
						"chunk stalled: no progress for %s (range=%d-%d)",
						idle.Round(time.Second), w.start, w.end))
					logger.S.Warnw("检测到分片下载卡顿（长时间无新数据），主动取消并触发重试",
						"idle", idle, "start", w.start, "end", w.end)
					w.cancel()
					return
				}
			}

			// 2. 低速长尾检测：要求连续命中，避免瞬时抖动误杀正常分片
			if w.minSpeed > 0 {
				speed := float64(windowBytes) / effectiveInterval.Seconds()
				if speed < float64(w.minSpeed) {
					w.lowSpeedHits++
				} else {
					w.lowSpeedHits = 0
				}

				if w.lowSpeedHits >= lowSpeedStrikeThreshold {
					w.triggerErr.Store(fmt.Errorf(
						"chunk too slow: %.1f KB/s sustained below min %.1f KB/s (range=%d-%d)",
						speed/1024, float64(w.minSpeed)/1024, w.start, w.end))
					logger.S.Warnw("检测到分片长尾（持续低于最低速度阈值），主动取消并触发重试",
						"speed_kbps", speed/1024, "min_kbps", float64(w.minSpeed)/1024,
						"start", w.start, "end", w.end)
					w.cancel()
					return
				}
			}
		}
	}
}
