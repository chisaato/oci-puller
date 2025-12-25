package coordinator

import (
	"context"
	"encoding/json"
	"io"
	"oci-puller/pkg/interval"
	"oci-puller/pkg/logger"
	"os"
	"sync"
	"time"
)

// Coordinator 协调下载进度和读取进度
type Coordinator struct {
	mu          sync.Mutex
	cond        *sync.Cond
	intervalMgr *interval.Manager
	file        *os.File
	fileSize    int64
	done        bool
	err         error

	refCount      int
	cancelFunc    context.CancelFunc
	shutdownTimer *time.Timer
	metaPath      string
	saveTicker    *time.Ticker
	stopSave      chan struct{}
	closed        bool
}

// NewCoordinator 创建一个新的协调器
func NewCoordinator(f *os.File, size int64, mgr *interval.Manager, cancel context.CancelFunc, metaPath string) *Coordinator {
	logger.S.Debugw("新的协调器已创建", "size", size, "metaPath", metaPath)
	c := &Coordinator{
		file:        f,
		fileSize:    size,
		intervalMgr: mgr,
		cancelFunc:  cancel,
		metaPath:    metaPath,
		stopSave:    make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)

	// Start periodic saver
	c.saveTicker = time.NewTicker(5 * time.Second)
	go c.runPeriodicSave()

	return c
}

// MarkDownloaded 标记一段数据已下载完成
func (c *Coordinator) MarkDownloaded(start, end int64) {
	// 1. 更新区间树 (线程安全)
	c.intervalMgr.Add(start, end)

	// 2. 广播通知所有等待的 Reader
	c.mu.Lock()
	c.cond.Broadcast()
	c.mu.Unlock()
}

// Cancel 主动取消下载任务
func (c *Coordinator) Cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancelFunc != nil {
		c.cancelFunc()
		c.cancelFunc = nil
	}

	// 标记错误如果是被动取消
	if !c.done {
		c.done = true
		c.err = context.Canceled
		c.cond.Broadcast()
	}
}

// Close 关闭协调器，关闭底层文件
func (c *Coordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// Stop saver
	if c.saveTicker != nil {
		c.saveTicker.Stop()
		close(c.stopSave)
		c.saveTicker = nil
	}

	// Final save
	// We need to release lock for SaveState to avoid deadlock if it acquires lock
	c.mu.Unlock()
	c.SaveState()
	c.mu.Lock()

	logger.S.Debugw("协调器正在关闭底层文件", "metaPath", c.metaPath)
	return c.file.Close()
}

// SetDone 标记总体下载完成（成功或失败）
func (c *Coordinator) SetDone(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Stop saver
	if c.saveTicker != nil {
		c.saveTicker.Stop()
		close(c.stopSave)
		c.saveTicker = nil
	}

	// Final save if successful or partial
	// 注意：这里不要在和持有锁的情况下做 IO
	c.mu.Unlock()
	c.SaveState()
	c.mu.Lock()

	c.done = true
	c.err = err
	logger.S.Debugw("协调器已标记为完成状态", "error", err)
	c.cond.Broadcast()

	// If done successfully, remove meta file
	if err == nil {
		os.Remove(c.metaPath)
	}
}

// IncRef 增加引用计数
func (c *Coordinator) IncRef() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.refCount++
	if c.shutdownTimer != nil {
		if c.shutdownTimer.Stop() {
			logger.S.Debugw("协调器关闭定时器已停止", "refCount", c.refCount)
		}
		c.shutdownTimer = nil
	}
}

// GracePeriod 默认宽限期
var GracePeriod = 10 * time.Second

// DecRef 减少引用计数
func (c *Coordinator) DecRef() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.refCount--
	if c.refCount <= 0 {
		// 如果任务已经完成 (成功或失败)，直接清理资源
		if c.done {
			// 如果没有错误，删除 meta 文件
			if c.err == nil {
				os.Remove(c.metaPath)
			}
			// 不在锁内做 Close IO，使用协程或解锁
			c.mu.Unlock()
			c.Close()
			c.mu.Lock()
			return
		}

		// 如果任务未完成，启动 Grace Period
		logger.S.Debugw("协调器引用计数降至 0，正在启动优雅关闭定时器", "timeout", GracePeriod)

		c.shutdownTimer = time.AfterFunc(GracePeriod, func() {
			c.mu.Lock()
			// Double check inside lock
			if c.refCount <= 0 {
				logger.S.Infow("宽限期已到，正在取消下载任务")
				c.mu.Unlock()
				c.Cancel() // Cancel 会设置 done = true
				c.Close()  // Close 关闭文件
				c.mu.Lock()
			}
			c.mu.Unlock()
		})
	}
}

// runPeriodicSave 定期保存状态
func (c *Coordinator) runPeriodicSave() {
	for {
		select {
		case <-c.saveTicker.C:
			c.SaveState()
		case <-c.stopSave:
			return
		}
	}
}

// SaveState 将当前状态保存到 meta 文件
func (c *Coordinator) SaveState() {
	if c.metaPath == "" {
		return
	}

	intervals := c.intervalMgr.Dump()
	// 如果没有数据，不需要保存? 不，还是保存一下空列表表示开始了

	bytes, err := json.Marshal(intervals)
	if err != nil {
		logger.S.Warnw("状态序列化失败", "error", err)
		return
	}

	// Atomic write: write to temp then rename
	tmpPath := c.metaPath + ".tmp"
	if err := os.WriteFile(tmpPath, bytes, 0644); err != nil {
		logger.S.Warnw("写入状态文件失败", "path", tmpPath, "error", err)
		return
	}

	if err := os.Rename(tmpPath, c.metaPath); err != nil {
		logger.S.Warnw("重命名状态文件失败", "path", c.metaPath, "error", err)
	}
}

// LoadState 加载状态
func (c *Coordinator) LoadState() error {
	if c.metaPath == "" {
		return nil
	}

	bytes, err := os.ReadFile(c.metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var intervals []interval.Range
	if err := json.Unmarshal(bytes, &intervals); err != nil {
		return err
	}

	c.intervalMgr.Load(intervals)
	logger.S.Infow("状态已加载", "intervals", len(intervals))
	return nil
}

// StreamReader 实现了 io.Reader 接口，用于从 Coordinator 读取数据
// 它会自动阻塞等待数据下载，直到读完或报错
type StreamReader struct {
	coord  *Coordinator
	offset int64
}

// NewReader 创建一个新的读取器
func (c *Coordinator) NewReader() *StreamReader {
	c.IncRef()
	return &StreamReader{
		coord:  c,
		offset: 0,
	}
}

func (r *StreamReader) Read(p []byte) (n int, err error) {
	r.coord.mu.Lock()
	defer r.coord.mu.Unlock()

	for {
		// 1. 检查下载是否发生错误
		if r.coord.err != nil {
			logger.S.Debugw("读取器遇到来自协调器的错误", "error", r.coord.err, "offset", r.offset)
			return 0, r.coord.err
		}

		// 2. 获取当前已连续下载到的最大偏移量
		safeOffset := r.coord.intervalMgr.GetMaxSafeOffset()

		// 3. 计算可读字节数
		// available = [offset, safeOffset) 的长度
		available := safeOffset - r.offset

		if available > 0 {
			// 有数据可读，暂时释放锁进行 IO 操作
			r.coord.mu.Unlock()

			// 限制本次读取长度
			want := int64(len(p))
			if want > available {
				want = available
			}

			// 执行读取
			n, err = r.coord.file.ReadAt(p[:want], r.offset)

			// 重新加锁
			r.coord.mu.Lock()

			if n > 0 {
				r.offset += int64(n)
			}

			// 处理 ReadAt 的 EOF
			if err == io.EOF {
				if r.offset == r.coord.fileSize {
					logger.S.Debugw("读取器已到达文件末尾", "offset", r.offset)
					return n, io.EOF
				}
				err = nil
			} else if err != nil {
				logger.S.Warnw("读取器执行 ReadAt 时出错", "error", err, "offset", r.offset)
				return n, err
			}

			return n, nil
		}

		// 4. 如果没有新数据可读
		// 检查是否已经彻底完成了
		if r.coord.done {
			if r.offset == r.coord.fileSize {
				return 0, io.EOF
			}
			logger.S.Errorw("读取器检测到下载虽然结束但数据不完整",
				"offset", r.offset,
				"size", r.coord.fileSize)
			return 0, io.ErrUnexpectedEOF
		}

		// 5. 阻塞等待新数据通知
		//logger.S.Debugw("Reader blocking for data", "offset", r.offset, "safeOffset", safeOffset)
		r.coord.cond.Wait()
		//logger.S.Debugw("Reader unblocked", "offset", r.offset)
	}
}

// Close 实现 io.Closer 接口方法
func (r *StreamReader) Close() error {
	r.coord.DecRef()
	return nil
}

// Size 返回文件总大小
func (r *StreamReader) Size() int64 {
	return r.coord.Size()
}

// Size 返回文件总大小
func (c *Coordinator) Size() int64 {
	return c.fileSize
}

// IsDownloaded 检查指定区间是否已下载完成
func (c *Coordinator) IsDownloaded(start, end int64) bool {
	return c.intervalMgr.Includes(start, end)
}
