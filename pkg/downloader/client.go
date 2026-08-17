package downloader

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// newClient 创建一个标准的 HTTP Client，强制使用 HTTP/1.1
// 每次调用都会返回一个新的 Client 实例，拥有独立的 Transport
func newClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			// 1. 基础网络配置 (手动复刻 http.DefaultTransport 的默认值)
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			// 响应头超时：连接建立成功后，如果对端迟迟不返回响应头（例如被中间设备静默挂起），
			// 之前完全没有超时保护，会一直卡到外层 1 小时的总 Context 超时才会暴露问题。
			ResponseHeaderTimeout: 20 * time.Second,

			// 2. HTTP/2 禁用配置
			// 关键：设置 TLSNextProto 为非 nil 的空 map，彻底禁用 HTTP/2 自动升级
			TLSNextProto:      make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
			ForceAttemptHTTP2: false,

			// 3. 自定义连接池配置
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
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
