package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"oci-puller/pkg/config"
	"oci-puller/pkg/coordinator"
	"oci-puller/pkg/downloader"
	"oci-puller/pkg/interval"
	"oci-puller/pkg/logger"
	"oci-puller/pkg/storage"
)

// DownloadManager 管理所有的下载任务
type DownloadManager struct {
	cacheDir string
	active   sync.Map   // map[string]*coordinator.Coordinator (digest -> coordinator)
	mu       sync.Mutex // 用于串行化创建过程，防止重复启动
	cleanMu  sync.Mutex // 用于串行化手动清理与后台 GC，避免并发交叉
	store    *storage.Store
}

func NewManager(cacheDir string) *DownloadManager {
	logger.S.Debugw("正在初始化下载管理器", "cacheDir", cacheDir)
	os.MkdirAll(cacheDir, 0755)

	// 创建子目录
	blobsDir := filepath.Join(cacheDir, "blobs")
	tempDir := filepath.Join(cacheDir, "temp")
	os.MkdirAll(blobsDir, 0755)
	os.MkdirAll(tempDir, 0755)

	// 创建 oci-layout 文件
	layoutPath := filepath.Join(cacheDir, "oci-layout")
	if _, err := os.Stat(layoutPath); os.IsNotExist(err) {
		if err := os.WriteFile(layoutPath, []byte(`{"imageLayoutVersion": "1.0.0"}`), 0644); err != nil {
			logger.S.Warnw("创建 oci-layout 文件失败", "error", err)
		}
	}

	// 初始化存储
	storePath := filepath.Join(cacheDir, "cache.db")
	store, err := storage.NewStore(storePath)
	if err != nil {
		logger.S.Errorw("初始化缓存存储失败", "error", err)
		// 我们应该 panic 还是在没有缓存管理的情况下继续运行？
		// 目前先记录错误并继续（需要检查 store 是否为 nil）
	}

	mgr := &DownloadManager{
		cacheDir: cacheDir,
		store:    store,
	}

	if store != nil {
		go mgr.backgroundGC()
	}

	return mgr
}

// CacheStats 缓存统计信息
type CacheStats struct {
	TotalSize       int64   `json:"total_size"`
	BlobCount       int     `json:"blob_count"`
	MaxSize         int64   `json:"max_size"`
	UsagePercent    float64 `json:"usage_percent"`
	ActiveDownloads int     `json:"active_downloads"`
}

// GetCacheStats 返回缓存统计信息
func (m *DownloadManager) GetCacheStats() (*CacheStats, error) {
	stats := &CacheStats{}

	// 获取配置的最大大小
	cfg := config.GlobalConfig
	maxSize, err := cfg.Cache.ParseMaxSize()
	if err != nil {
		maxSize = 10 * 1024 * 1024 * 1024 // 10GB 默认值
	}
	stats.MaxSize = maxSize

	// 获取活跃下载数量
	activeCount := 0
	m.active.Range(func(key, value interface{}) bool {
		activeCount++
		return true
	})
	stats.ActiveDownloads = activeCount

	// 如果没有存储系统，返回基本信息
	if m.store == nil {
		return stats, nil
	}

	// 获取总大小和项目数
	totalSize, err := m.store.GetTotalSize()
	if err != nil {
		return nil, fmt.Errorf("获取缓存总大小失败: %w", err)
	}
	stats.TotalSize = totalSize

	blobCount, err := m.store.GetBlobCount()
	if err != nil {
		return nil, fmt.Errorf("获取缓存项目数失败: %w", err)
	}
	stats.BlobCount = blobCount

	// 计算使用百分比
	if stats.MaxSize > 0 {
		stats.UsagePercent = float64(stats.TotalSize) / float64(stats.MaxSize) * 100
	}

	return stats, nil
}

// Close 关闭底层存储
func (m *DownloadManager) Close() error {
	if m.store != nil {
		return m.store.Close()
	}
	return nil
}

// CleanupCache 手动清理缓存，删除指定数量的最旧项目
func (m *DownloadManager) CleanupCache(count int) error {
	if m.store == nil {
		return fmt.Errorf("缓存存储系统未初始化")
	}

	if count <= 0 {
		return fmt.Errorf("清理数量必须大于0")
	}

	logger.S.Infow("开始手动缓存清理", "count", count)
	result, err := m.runCleanup("manual", cleanupOptions{count: count})
	if err != nil {
		return err
	}

	logger.S.Infow("手动缓存清理完成",
		"requested", count,
		"actualDeleted", result.deletedCount,
		"freedSize", result.freedSize,
		"skippedActive", result.skippedActive)

	return nil
}

func (m *DownloadManager) backgroundGC() {
	// 获取配置的清理间隔
	cfg := config.GlobalConfig
	interval, err := cfg.Cache.ParseCleanupInterval()
	if err != nil {
		logger.S.Warnw("解析清理间隔配置失败，使用默认值", "error", err)
		interval = 5 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.S.Infow("启动后台垃圾回收", "interval", interval)

	for range ticker.C {
		m.runGC()
	}
}

func (m *DownloadManager) runGC() {
	if m.store == nil {
		return
	}

	// 获取配置
	cfg := config.GlobalConfig
	maxSize, err := cfg.Cache.ParseMaxSize()
	if err != nil {
		logger.S.Warnw("解析缓存最大大小配置失败，使用默认值", "error", err)
		maxSize = 10 * 1024 * 1024 * 1024 // 10GB
	}

	// 获取当前缓存总大小
	currentSize, err := m.store.GetTotalSize()
	if err != nil {
		logger.S.Warnw("获取缓存总大小失败", "error", err)
		return
	}

	blobCount, err := m.store.GetBlobCount()
	if err != nil {
		logger.S.Warnw("获取缓存项目数失败", "error", err)
		return
	}

	logger.S.Debugw("缓存状态检查",
		"currentSize", currentSize,
		"maxSize", maxSize,
		"blobCount", blobCount,
		"usagePercent", float64(currentSize)/float64(maxSize)*100)

	// 如果缓存大小超过限制，开始清理
	if currentSize > maxSize {
		needToFree := currentSize - maxSize + (maxSize / 10) // 多清理10%以避免频繁GC

		logger.S.Infow("开始缓存清理",
			"needToFree", needToFree,
			"currentSize", currentSize,
			"maxSize", maxSize)

		result, err := m.runCleanup("gc", cleanupOptions{needToFree: needToFree, batchSize: 10})
		if err != nil {
			logger.S.Warnw("缓存清理失败", "error", err)
			return
		}

		logger.S.Infow("缓存清理完成",
			"freedSize", result.freedSize,
			"evictedCount", result.deletedCount,
			"skippedActive", result.skippedActive,
			"newSize", currentSize-result.freedSize)
	}
}

type cleanupOptions struct {
	count      int
	needToFree int64
	batchSize  int
}

type cleanupResult struct {
	freedSize     int64
	deletedCount  int
	skippedActive int
}

func (m *DownloadManager) runCleanup(reason string, opts cleanupOptions) (*cleanupResult, error) {
	if m.store == nil {
		return nil, fmt.Errorf("缓存存储系统未初始化")
	}

	m.cleanMu.Lock()
	defer m.cleanMu.Unlock()

	result := &cleanupResult{}
	skipActive := m.snapshotActiveDigests()
	result.skippedActive = len(skipActive)

	if opts.count > 0 {
		deleted, freedSize, err := m.evictBatch(opts.count, skipActive)
		if err != nil {
			return nil, fmt.Errorf("执行 %s 清理失败: %w", reason, err)
		}
		result.deletedCount = deleted
		result.freedSize = freedSize
		return result, nil
	}

	batchSize := opts.batchSize
	if batchSize <= 0 {
		batchSize = 10
	}

	for result.freedSize < opts.needToFree {
		deleted, freedSize, err := m.evictBatch(batchSize, skipActive)
		if err != nil {
			return nil, fmt.Errorf("执行 %s 清理失败: %w", reason, err)
		}
		if deleted == 0 {
			break
		}

		result.deletedCount += deleted
		result.freedSize += freedSize

		if deleted < batchSize {
			break
		}
	}

	return result, nil
}

func (m *DownloadManager) evictBatch(count int, skipActive map[string]struct{}) (int, int64, error) {
	evictedDigests, err := m.store.EvictOldestExcept(count, skipActive)
	if err != nil {
		return 0, 0, fmt.Errorf("从存储中驱逐项目失败: %w", err)
	}

	var freedSize int64
	var deletedCount int

	for _, digest := range evictedDigests {
		finalPath := m.getFinalPath(digest)
		tempPath := m.getTempPath(digest)
		metaPath := tempPath + ".meta"

		if info, err := os.Stat(finalPath); err == nil {
			freedSize += info.Size()
			deletedCount++

			if err := os.Remove(finalPath); err != nil {
				logger.S.Warnw("删除缓存文件失败", "path", finalPath, "error", err)
			} else {
				logger.S.Debugw("已删除缓存文件", "digest", digest, "path", finalPath)
			}
		}

		if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
			logger.S.Warnw("删除临时文件失败", "path", tempPath, "error", err)
		}
		if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
			logger.S.Warnw("删除临时元数据文件失败", "path", metaPath, "error", err)
		}
	}

	return deletedCount, freedSize, nil
}

func (m *DownloadManager) snapshotActiveDigests() map[string]struct{} {
	activeDigests := make(map[string]struct{})
	m.active.Range(func(key, value interface{}) bool {
		digest, ok := key.(string)
		if ok {
			activeDigests[digest] = struct{}{}
		}
		return true
	})
	return activeDigests
}

// GetBlobStream 获取 Blob 的读取流
// 返回 io.ReadCloser，调用者不再使用时需关闭
func (m *DownloadManager) GetBlobStream(digest string, url string, headers http.Header) (io.ReadCloser, error) {
	// 1. 检查最终缓存
	finalPath := m.getFinalPath(digest)
	if info, err := os.Stat(finalPath); err == nil && !info.IsDir() {
		// 更新 LRU
		if m.store != nil {
			go func() {
				if err := m.store.RecordAccess(digest, info.Size(), finalPath); err != nil {
					logger.S.Warnw("记录访问失败", "digest", digest, "error", err)
				}
			}()
		}

		// 修复僵尸任务：如果缓存命中但存在活跃下载，则是冗余的。将其取消。
		if v, ok := m.active.Load(digest); ok {
			logger.S.Infow("缓存命中但发现活跃的僵尸下载任务，正在取消", "digest", digest)
			coord := v.(*coordinator.Coordinator)
			coord.Cancel()
		}

		logger.S.Infow("缓存命中", "digest", digest, "path", finalPath)
		return os.Open(finalPath)
	}

	// 2. 检查活跃下载
	if v, ok := m.active.Load(digest); ok {
		logger.S.Infow("正在加入现有下载任务", "digest", digest)
		coord := v.(*coordinator.Coordinator)
		return coord.NewReader(), nil
	}

	// 3. 启动新下载 (加锁防止并发启动)
	m.mu.Lock()
	// 双重检查
	if v, ok := m.active.Load(digest); ok {
		m.mu.Unlock()
		logger.S.Infow("正在加入现有下载任务 (二次检查)", "digest", digest)
		coord := v.(*coordinator.Coordinator)
		return coord.NewReader(), nil
	}

	logger.S.Infow("正在启动新下载任务", "digest", digest, "url", url)
	coord, err := m.startDownload(digest, url, headers)
	m.mu.Unlock()

	if err != nil {
		logger.S.Errorw("无法启动下载任务", "digest", digest, "error", err)
		return nil, err
	}
	return coord.NewReader(), nil
}

// startDownload 内部启动下载逻辑
func (m *DownloadManager) startDownload(digest, url string, headers http.Header) (*coordinator.Coordinator, error) {
	// 1. 获取大小
	logger.S.Debugw("正在获取 Blob 大小", "url", url)
	size, supportsRange, err := m.getSize(url, headers)
	if err != nil {
		return nil, err
	}
	logger.S.Debugw("获取到 Blob 信息", "digest", digest, "size", size, "supportsRange", supportsRange)

	// 2. 准备临时文件
	tempPath := m.getTempPath(digest)
	metaPath := tempPath + ".meta"

	// 检查是否可以断点续传
	var existingSize int64
	info, err := os.Stat(tempPath)
	if err == nil && !info.IsDir() {
		existingSize = info.Size()
		// 只有当文件大小没有超过目标大小时才尝试恢复（防止下载了错误的大文件）
		// 实际上更好的做法是校验已有的块，这里简化处理
		if existingSize <= size {
			logger.S.Infow("发现部分下载的文件", "digest", digest, "existingSize", existingSize)
		} else {
			logger.S.Warnw("部分下载文件大于目标大小，正在重置", "digest", digest)
			os.Remove(tempPath)
			os.Remove(metaPath)
			existingSize = 0
		}
	}

	// 打开文件 (O_RDWR 用于读写, O_CREATE 用于创建)
	// 注意不要使用 O_TRUNC，除非我们需要重置
	flags := os.O_RDWR | os.O_CREATE
	if existingSize == 0 {
		flags |= os.O_TRUNC
	}

	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(tempPath), 0755); err != nil {
		return nil, fmt.Errorf("创建临时目录失败: %w", err)
	}

	f, err := os.OpenFile(tempPath, flags, 0644)
	if err != nil {
		return nil, err
	}

	// 如果是从头开始 (existingSize == 0 或 刚 Truncate), 确保大小正确
	// 注意：如果是恢复，我们假设文件已经是稀疏文件或者已经有部分内容，
	// 调用 Truncate 确保它至少有这么大（Go 的 Truncate 可以扩展文件大小）
	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(tempPath)
		return nil, err
	}

	// 3. 准备 Context (用于取消)
	// 默认 1小时超时，Coordinator 可提前取消
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)

	// 4. 创建组件
	mgr := interval.NewManager()
	coord := coordinator.NewCoordinator(f, size, mgr, cancel, metaPath)

	// 将文件句柄的所有权转移给 coordinator
	// Manager 不再直接负责关闭 f。
	// 只有当 RefCount 变为 0 时，coordinator 才会关闭它。

	// 尝试加载状态
	if existingSize > 0 {
		if err := coord.LoadState(); err != nil {
			logger.S.Warnw("无法加载状态，正在重置下载任务", "error", err)
			// 重置区间管理器
			// 文件可能已经有脏数据，为了安全我们可能需要重新 Truncate?
			// 这里简单起见，如果加载失败，就当作什么都没发生，下载器会尝试下载缺失部分
			// 但 Coordinator 的区间树是空的，所以下载器会认为整个文件都没下载
			// 这没问题，只是浪费了之前的 IO
		} else {
			logger.S.Infow("已从记录的状态点恢复下载", "digest", digest)
		}
	}

	dl := downloader.New(url, f, coord, headers)

	// 应用配置文件中的下载器参数（此前这里被硬编码值覆盖，config.yaml 中的
	// workers/chunk_size/min_speed/stall_timeout/max_retries 实际上从未生效）。
	if cfg := config.GlobalConfig; cfg != nil {
		if cfg.Downloader.Workers > 0 {
			dl.SetWorkers(cfg.Downloader.Workers)
		}
		if chunkSize, err := cfg.Downloader.ParseChunkSize(); err == nil {
			dl.SetChunkSize(chunkSize)
		} else {
			logger.S.Warnw("解析 chunk_size 配置失败，使用默认值", "error", err)
		}
		if minSpeed, err := cfg.Downloader.ParseMinSpeed(); err == nil {
			dl.SetMinSpeed(minSpeed)
		} else {
			logger.S.Warnw("解析 min_speed 配置失败，使用默认值", "error", err)
		}
		if stallTimeout, err := cfg.Downloader.ParseStallTimeout(); err == nil {
			dl.SetStallTimeout(stallTimeout)
		} else {
			logger.S.Warnw("解析 stall_timeout 配置失败，使用默认值", "error", err)
		}
		if cfg.Downloader.MaxRetries > 0 {
			dl.SetMaxRetries(cfg.Downloader.MaxRetries)
		}
	}

	if !supportsRange {
		// 别管,强制性用多线程拉
		logger.S.Warnw("上游不支持 Range 请求，由于策略强制，仍将使用多线程下载", "url", url)
	}

	// 4. 注册到 Active Map
	m.active.Store(digest, coord)

	// 5. 异步启动
	go func() {
		startTime := time.Now()
		logger.S.Infow("下载任务线程已经启动", "digest", digest)

		// 确保退出时释放资源
		defer cancel()

		// Downloader 工作线程自身的引用计数
		// 确保下载过程中文件不会被关闭
		coord.IncRef()
		// 退出时释放引用计数会触发 Close()（如果没有剩余的读取者）
		defer coord.DecRef()

		err := dl.Start(ctx)

		// f.Close() // 已移除：现在由 Coordinator 的引用计数管理
		duration := time.Since(startTime)

		if err == nil {
			// 下载成功，移动到最终目录
			finalPath := m.getFinalPath(digest)
			// 创建父目录（如果是 sha256/xx/yy 结构）
			os.MkdirAll(filepath.Dir(finalPath), 0755)

			renameErr := os.Rename(tempPath, finalPath)
			if renameErr != nil {
				logger.S.Errorw("将临时文件重命名为最终路径时出错",
					"digest", digest,
					"tempPath", tempPath,
					"finalPath", finalPath,
					"error", renameErr)
			} else {
				logger.S.Infow("完成下载并已缓存",
					"digest", digest,
					"duration", duration,
					"size", size)

				// 记录访问
				if m.store != nil {
					if recErr := m.store.RecordAccess(digest, size, finalPath); recErr != nil {
						logger.S.Warnw("记录访问失败", "digest", digest, "error", recErr)
					}
				}
			}
		} else {
			// 失败
			logger.S.Errorw("下载失败", "digest", digest, "error", err, "duration", duration)

			// 如果是 Context Canceled，我们保留文件以便续传
			if errors.Is(err, context.Canceled) {
				logger.S.Infow("下载已取消，保留部分下载的文件以便续传", "digest", digest)
			} else {
				// 其他错误（如网络错误重试过多），保留还是删除？
				// 策略：如果是网络原因导致彻底失败，也保留。
				// 只有在确定文件损坏时才删除。
				// 这里暂时保留，除非显式执行清理 (Cleanup)
			}
		}

		// 从活跃列表移除
		m.active.Delete(digest)
		logger.S.Debugw("下载任务线程已退出", "digest", digest)
	}()

	return coord, nil
}

func (m *DownloadManager) getSize(url string, headers http.Header) (int64, bool, error) {
	// 用于发起请求的辅助函数
	makeReq := func(method string) (*http.Response, error) {
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			req.Header[k] = v
		}
		// 如果降级到 GET，我们只需要获取第一个字节来检查 Range 支持情况和文件大小
		if method == http.MethodGet {
			req.Header.Set("Range", "bytes=0-0")
		}
		return http.DefaultClient.Do(req)
	}

	// 1. 先尝试 HEAD
	resp, err := makeReq("HEAD")
	if err != nil {
		return 0, false, err
	}

	// 如果不允许使用 HEAD 或请求失败，尝试使用 GET
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		logger.S.Debugw("HEAD 请求失败，正在尝试使用 GET", "url", url, "status", resp.StatusCode)
		resp, err = makeReq(http.MethodGet)
		if err != nil {
			return 0, false, err
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return 0, false, fmt.Errorf("请求 %s 返回了 %d", url, resp.StatusCode)
	}

	// 处理 Content-Length
	cl := resp.Header.Get("Content-Length")
	// 如果我们使用带 Range 0-0 的 GET 请求（返回 206），Content-Length 可能是 1。
	// 我们需要使用 Content-Range: bytes 0-0/12345 来获取完整大小。
	var size int64 = -1

	if cr := resp.Header.Get("Content-Range"); cr != "" {
		// 格式: bytes 0-0/12345
		parts := strings.Split(cr, "/")
		if len(parts) == 2 && parts[1] != "*" {
			if s, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
				size = s
			}
		}
	}

	if size == -1 && cl != "" {
		if s, err := strconv.ParseInt(cl, 10, 64); err == nil {
			// 如果是 200 OK，Content-Length 就是完整大小。
			// 如果是 206 Partial Content 且我们请求了 bytes=0-0，Content-Length 很可能为 1。
			// 虽然如果没有 Content-Range 我们处理 206 会有问题，但通常 206 都会带上 Content-Range。
			if resp.StatusCode == http.StatusOK {
				size = s
			} else if resp.StatusCode == http.StatusPartialContent {
				// 我们已经尝试解析了 Content-Range。如果失败了，通常不能信任 Content-Length 作为完整大小。
				// 但让我们检查一下 Content-Range 是否缺失。
			}
		}
	}

	if size == -1 {
		return 0, false, fmt.Errorf("无法确定 %s 的文件大小", url)
	}

	supportsRange := resp.Header.Get("Accept-Ranges") == "bytes" || resp.StatusCode == http.StatusPartialContent
	return size, supportsRange, nil
}

func (m *DownloadManager) getFinalPath(digest string) string {
	parts := strings.Split(digest, ":")
	if len(parts) != 2 {
		// 对无效 Digest 的降级处理
		return filepath.Join(m.cacheDir, "blobs", "unknown", strings.ReplaceAll(digest, ":", "_"))
	}
	alg := parts[0]
	hash := parts[1]
	return filepath.Join(m.cacheDir, "blobs", alg, hash)
}

func (m *DownloadManager) getTempPath(digest string) string {
	safeName := strings.ReplaceAll(digest, ":", "_")
	return filepath.Join(m.cacheDir, "temp", safeName)
}
