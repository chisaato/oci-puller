package downloader

import (
	"net/http"
	"time"
)

// newClient 创建一个标准的 HTTP Client，强制使用 HTTP/1.1
// 每次调用都会返回一个新的 Client 实例，拥有独立的 Transport
func newClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			//TLSClientConfig: &tls.Config{
			//	// 强制仅使用 HTTP/1.1
			//	NextProtos: []string{"http/1.1"},
			//},
			// 禁用 HTTP/2
			ForceAttemptHTTP2:   false,
			IdleConnTimeout:     90 * time.Second,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
		},
	}
}

// ClientPool 管理多个 HTTP Client 实例，实现“多物理连接”并发下载
type ClientPool struct {
	clients []*http.Client
	size    int
	current uint64 // 用于 Round-Robin
}

// NewClientPool 创建指定大小的 Client 池
func NewClientPool(size int) *ClientPool {
	pool := &ClientPool{
		clients: make([]*http.Client, size),
		size:    size,
	}
	for i := 0; i < size; i++ {
		pool.clients[i] = newClient()
	}
	return pool
}

// Get 以 Round-Robin 方式获取一个 Client
// 实际上在 Downloader 中，每个 Worker 固定持有一个 Client 效果最好
func (p *ClientPool) Get() *http.Client {
	// 简单的并发安全 Round-Robin 不太重要，因为我们主要是为了分配给 Worker
	// 这里简单实现
	idx := p.current % uint64(p.size)
	p.current++
	return p.clients[idx]
}

// GetWorkerClient 直接获取指定 Worker ID 对应的 Client
// 这样能保证 Worker 1 永远用 Connection 1，避免频繁切换
func (p *ClientPool) GetWorkerClient(workerID int) *http.Client {
	if workerID < 0 || workerID >= p.size {
		return p.clients[0]
	}
	return p.clients[workerID]
}
