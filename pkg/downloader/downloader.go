package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"oci-puller/pkg/config"
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

	// Bitmap Scheduling
	bitmap    *bitset.BitSet
	numChunks uint
	mu        sync.Mutex // 保护 bitmap 和 retries
	retries   map[uint]int
}

// New 创建一个新的下载器
func New(url string, f *os.File, c *coordinator.Coordinator, headers http.Header) *Downloader {
	cfg := config.GlobalConfig

	// 解析 ChunkSize
	chunkSize, err := cfg.Downloader.ParseChunkSize()
	if err != nil {
		logger.S.Warnf("Invalid chunk size configuration: %v, using default 10MB", err)
		chunkSize = 10 * 1024 * 1024
	}

	return &Downloader{
		url:       url,
		file:      f,
		coord:     c,
		chunkSize: chunkSize,
		workers:   cfg.Downloader.Workers,
		headers:   headers,
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
					maxRetries := 5 // TODO: 配置文件化. 硬编码默认值，后续可配置
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
	req, err := http.NewRequestWithContext(ctx, "GET", d.url, nil)
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
			return readErr
		}
	}
	return nil
}
