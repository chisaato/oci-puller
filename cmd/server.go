package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"oci-puller/pkg/config"
	"oci-puller/pkg/coordinator"
	"oci-puller/pkg/logger"
	"oci-puller/pkg/manager"

	"github.com/spf13/cobra"
)

// serverCmd 代表 server 命令
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "启动 OCI Puller 代理服务器",
	Run:   runServer,
}

func init() {
	rootCmd.AddCommand(serverCmd)
}

func runServer(cmd *cobra.Command, args []string) {
	cfg := config.GlobalConfig

	logger.S.Infof("正在 %s 启动 OCI Puller (缓存目录: %s)", cfg.Server.Addr, cfg.Server.CacheDir)

	// 初始化管理器
	mgr := manager.NewManager(cfg.Server.CacheDir)

	// 准备上游映射表
	upstreams := make(map[string]*upstreamPool)
	for _, reg := range cfg.Registries {
		if len(reg.URLs) > 0 {
			urls := append([]string(nil), reg.URLs...)
			upstreams[reg.Host] = &upstreamPool{urls: urls}
			logger.S.Debugw("已注册上游池",
				"host", reg.Host,
				"urls", urls,
				"count", len(urls),
			)
		}
	}

	handler := &ProxyHandler{
		manager:   mgr,
		upstreams: upstreams,
	}

	srv := &http.Server{
		Addr:    cfg.Server.Addr,
		Handler: handler,
	}

	// 优雅停机
	go func() {
		logger.S.Info("正在监听 http://" + cfg.Server.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.S.Fatalw("服务器运行失败", "error", err)
		}
	}()

	// 等待中断信号以优雅地关闭服务器，超时时间为 5 秒。
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.S.Info("正在关闭服务器...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.S.Fatalw("服务器被强制关闭", "error", err)
	}

	logger.S.Info("服务器正在退出")
}

// ------ ProxyHandler 逻辑 (从 main.go 迁移) ------

type ProxyHandler struct {
	manager   *manager.DownloadManager
	upstreams map[string]*upstreamPool
}

type upstreamPool struct {
	urls    []string
	counter uint64
}

func (p *upstreamPool) candidates() []string {
	if len(p.urls) == 0 {
		return nil
	}
	if len(p.urls) == 1 {
		return append([]string(nil), p.urls[0])
	}

	start := atomic.AddUint64(&p.counter, 1) - 1
	ordered := make([]string, 0, len(p.urls))
	for i := range p.urls {
		idx := (int(start) + i) % len(p.urls)
		ordered = append(ordered, p.urls[idx])
	}

	return ordered
}

func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger.S.Debugw("收到请求",
		"method", r.Method,
		"host", r.Host,
		"path", r.URL.Path,
		"remote", r.RemoteAddr,
	)

	// 1. 基于主机的路由
	hostname := r.Host
	if host, _, err := net.SplitHostPort(hostname); err == nil {
		hostname = host
	}

	pool, ok := h.upstreams[hostname]
	if !ok {
		logger.S.Warnw("该主机未配置上游服务", "host", hostname, "path", r.URL.Path)
		http.Error(w, "未为该主机配置匹配的上游服务: "+hostname, http.StatusNotFound)
		return
	}

	candidates := pool.candidates()
	if len(candidates) == 0 {
		logger.S.Warnw("该主机未配置可用上游", "host", hostname, "path", r.URL.Path)
		http.Error(w, "该主机未配置可用上游: "+hostname, http.StatusBadGateway)
		return
	}

	logger.S.Debugw("已为请求选择上游候选序列",
		"host", hostname,
		"upstreams", candidates,
		"method", r.Method,
		"path", r.URL.Path,
	)

	// 2. 路径处理
	proxyPath := r.URL.Path

	// 3. 检查是否为 Blob 请求
	if strings.Contains(proxyPath, "/blobs/sha256:") && r.Method == http.MethodGet {
		digest := path.Base(proxyPath)
		if !strings.HasPrefix(digest, "sha256:") {
			logger.S.Errorw("无效的 Digest 格式", "digest", digest)
			http.Error(w, "无效的 Digest 格式", http.StatusBadRequest)
			return
		}

		var lastErr error
		for idx, upstreamBase := range candidates {
			logger.S.Debugw("[Blob] 正在尝试上游",
				"digest", digest,
				"upstream", upstreamBase,
				"attempt", idx+1,
				"total", len(candidates),
			)

			upstreamURL := buildUpstreamURL(upstreamBase, proxyPath, r.URL.RawQuery)
			logger.S.Infow("[Blob] 正在拦截请求",
				"digest", digest,
				"upstreamURL", upstreamURL,
			)

			if err := h.handleBlob(w, r, digest, upstreamURL); err != nil {
				lastErr = err
				logger.S.Warnw("Blob 加速失败，正在尝试下一个上游",
					"digest", digest,
					"upstream", upstreamBase,
					"error", err,
				)
				continue
			}

			logger.S.Infow("[Blob] 加速成功，已使用上游",
				"digest", digest,
				"upstream", upstreamBase,
			)
			return
		}

		logger.S.Warnw("Blob 加速全部失败，正在回退到普通代理", "digest", digest, "error", lastErr)
		if h.handleProxyWithFailover(w, r, candidates, proxyPath) {
			return
		}

		logger.S.Errorw("Blob 请求的所有上游都不可用", "digest", digest, "path", proxyPath, "error", lastErr)
		http.Error(w, "上游服务不可用", http.StatusBadGateway)
		return
	}

	// 4. Check for Manifest Request
	if strings.Contains(proxyPath, "/manifests/") && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		// Example: /v2/library/ubuntu/manifests/latest
		// We want to capture the Tag name (latest) or Digest if provided.
		tagOrDigest := path.Base(proxyPath)
		logger.S.Infow("[Manifest] 正在拦截请求",
			"tag", tagOrDigest,
			"path", proxyPath,
		)

		if h.handleManifestWithFailover(w, r, candidates, proxyPath, tagOrDigest) {
			return
		}

		logger.S.Errorw("Manifest 请求的所有上游都不可用", "tag", tagOrDigest, "path", proxyPath)
		http.Error(w, "上游服务不可用", http.StatusBadGateway)
		return
	}

	// 5. 默认代理
	logger.S.Debugw("[Proxy] 直接透传", "path", proxyPath)
	if h.handleProxyWithFailover(w, r, candidates, proxyPath) {
		return
	}

	logger.S.Errorw("代理请求的所有上游都不可用", "host", hostname, "path", proxyPath)
	http.Error(w, "上游服务不可用", http.StatusBadGateway)
}

func (h *ProxyHandler) handleManifestWithFailover(w http.ResponseWriter, r *http.Request, candidates []string, proxyPath, tag string) bool {
	for idx, upstreamBase := range candidates {
		logger.S.Debugw("[Manifest] 正在尝试上游",
			"tag", tag,
			"path", proxyPath,
			"upstream", upstreamBase,
			"attempt", idx+1,
			"total", len(candidates),
		)

		result, err := h.fetchUpstream(r, upstreamBase, proxyPath)
		if err != nil {
			logger.S.Warnw("上游 Manifest 请求失败，正在尝试下一个上游",
				"upstream", upstreamBase,
				"path", proxyPath,
				"error", err,
			)
			continue
		}

		if result.resp.StatusCode >= http.StatusInternalServerError {
			logger.S.Warnw("上游 Manifest 返回 5xx，正在尝试下一个上游",
				"upstream", upstreamBase,
				"path", proxyPath,
				"statusCode", result.resp.StatusCode,
			)
			result.resp.Body.Close()
			continue
		}

		defer result.resp.Body.Close()
		copyHeaders(w.Header(), result.resp.Header)
		w.WriteHeader(result.resp.StatusCode)
		logger.S.Infow("[Manifest] 请求成功，已使用上游",
			"tag", tag,
			"path", proxyPath,
			"upstream", upstreamBase,
			"statusCode", result.resp.StatusCode,
		)

		if r.Method == http.MethodHead || result.resp.StatusCode != http.StatusOK {
			_, _ = io.Copy(w, result.resp.Body)
			return true
		}

		// TeeReader to capture body
		// 注意：如果 Body 很大，全部读入内存可能会有问题。但 Manifest 通常很小（几 KB）。
		// 所以我们可以直接 ReadAll，然后再 Write 给 Client。
		// 或者用 TeeReader 边写边算。

		// 为了确保我们能获取完整的 body 用于计算 hash，必须确保 Client 读取完毕或者我们主动读取完毕。
		// 这里选择 ReadAll 方案，因为 Manifest 很小，这样最安全。
		bodyBytes, err := io.ReadAll(result.resp.Body)
		if err != nil {
			logger.S.Errorw("读取 Manifest Body 失败", "error", err)
			return true
		}

		// Write response to client immediately
		if _, err := w.Write(bodyBytes); err != nil {
			logger.S.Warnw("向客户端写入 Manifest 失败", "error", err)
			return true
		}

		// Async Side Effect: Save to Index
		// Make sure we don't save error responses (already checked status 200)
		// Make sure the request was for a Tag, not a digest (though if it is a digest, saving it is harmless)
		// If path ends with sha256:..., then tag is actually a digest.

		contentType := result.resp.Header.Get("Content-Type")
		// If no content type, default to manifest v2?
		if contentType == "" {
			contentType = "application/vnd.docker.distribution.manifest.v2+json"
		}

		go func() {
			// 1. Save Blob (Manifest itself)
			digest, size, err := h.manager.SaveBytes(bodyBytes)
			if err != nil {
				logger.S.Errorw("保存 Manifest Blob 失败", "error", err)
				return
			}

			// 2. Update Index
			// Only if "tag" doesn't look like a digest (start with sha256:) ?
			// Actually, even if it is a digest, updating index might confuse skopeo if we map digest->digest.
			// Skopeo `index.json` expects `ref.name` to be a Tag usually.
			if !strings.HasPrefix(tag, "sha256:") {
				if err := h.manager.UpdateIndex(tag, digest, size, contentType); err != nil {
					logger.S.Errorw("更新 index.json 失败", "tag", tag, "error", err)
				} else {
					logger.S.Infow("已更新本地索引", "tag", tag, "digest", digest)
				}
			}
		}()

		return true
	}

	return false
}

func (h *ProxyHandler) handleBlob(w http.ResponseWriter, r *http.Request, digest, url string) error {
	// 将配置传递给管理器/下载器
	// 注意：管理器目前不接收配置，我们可能需要传递它，或者在下载器中依赖全局配置

	stream, err := h.manager.GetBlobStream(digest, url, r.Header)
	if err != nil {
		return err
	}
	defer stream.Close()

	var size int64 = -1
	if sr, ok := stream.(*coordinator.StreamReader); ok {
		size = sr.Size()
	}
	if f, ok := stream.(*os.File); ok {
		if info, err := f.Stat(); err == nil {
			size = info.Size()
		}
	}

	if size >= 0 {
		w.Header().Set("Content-Length", fmt.Sprint(size))
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", digest)

	logger.S.Debugw("正在向客户端流式传输 Blob", "digest", digest, "size", size)
	written, copyErr := io.Copy(w, stream)
	if copyErr != nil {
		logger.S.Warnw("流传输错误", "digest", digest, "error", copyErr, "written", written)
	} else {
		logger.S.Infow("流传输完成", "digest", digest, "written", written)
	}
	return nil
}

func (h *ProxyHandler) handleProxyWithFailover(w http.ResponseWriter, r *http.Request, candidates []string, proxyPath string) bool {
	if requestHasReplayableBody(r) {
		logger.S.Warnw("请求 Body 不可重放，跳过多上游 failover，仅使用第一个上游",
			"method", r.Method,
			"path", proxyPath,
			"upstream", candidates[0],
		)
		return h.handleProxy(w, r, candidates[0], proxyPath)
	}

	for idx, upstreamBase := range candidates {
		logger.S.Debugw("[Proxy] 正在尝试上游",
			"path", proxyPath,
			"upstream", upstreamBase,
			"attempt", idx+1,
			"total", len(candidates),
		)

		if h.handleProxy(w, r, upstreamBase, proxyPath) {
			return true
		}

		logger.S.Warnw("普通代理失败，正在尝试下一个上游",
			"upstream", upstreamBase,
			"method", r.Method,
			"path", proxyPath,
		)
	}

	return false
}

func (h *ProxyHandler) handleProxy(w http.ResponseWriter, r *http.Request, upstreamBase, proxyPath string) bool {
	if requestHasReplayableBody(r) {
		h.handleProxySingleAttempt(w, r, upstreamBase, proxyPath)
		return true
	}

	result, err := h.fetchUpstream(r, upstreamBase, proxyPath)
	if err != nil {
		logger.S.Warnw("普通代理上游请求失败", "upstream", upstreamBase, "error", err)
		return false
	}
	defer result.resp.Body.Close()

	copyHeaders(w.Header(), result.resp.Header)
	w.WriteHeader(result.resp.StatusCode)
	logger.S.Infow("[Proxy] 透传成功，已使用上游",
		"upstream", upstreamBase,
		"path", proxyPath,
		"statusCode", result.resp.StatusCode,
	)
	_, _ = io.Copy(w, result.resp.Body)
	return true
}

func (h *ProxyHandler) handleProxySingleAttempt(w http.ResponseWriter, r *http.Request, upstreamBase, proxyPath string) {
	remote, err := url.Parse(upstreamBase)
	if err != nil {
		logger.S.Errorw("上游 URL 无效", "url", upstreamBase, "error", err)
		http.Error(w, "上游 URL 无效", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(remote)
	proxy.Rewrite = func(pr *httputil.ProxyRequest) {
		pr.SetURL(remote)
		pr.SetXForwarded()
		pr.Out.URL.Path = proxyPath
		pr.Out.URL.RawPath = ""
		pr.Out.URL.RawQuery = pr.In.URL.RawQuery
		pr.Out.Host = remote.Host // 对于 Registry 很重要

		logger.S.Debugw("[Proxy] 已重写出站请求",
			"method", pr.Out.Method,
			"path", pr.Out.URL.Path,
			"rawQuery", pr.Out.URL.RawQuery,
			"upstream", upstreamBase,
			"host", pr.Out.Host,
		)
	}

	proxy.ServeHTTP(w, r)
}

type upstreamResponse struct {
	resp *http.Response
}

func (h *ProxyHandler) fetchUpstream(r *http.Request, upstreamBase, proxyPath string) (*upstreamResponse, error) {
	upstreamURL := buildUpstreamURL(upstreamBase, proxyPath, r.URL.RawQuery)
	req, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL, nil)
	if err != nil {
		return nil, err
	}

	copyHeaders(req.Header, r.Header)
	req.Host = req.URL.Host

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	return &upstreamResponse{resp: resp}, nil
}

func buildUpstreamURL(upstreamBase, proxyPath, rawQuery string) string {
	if rawQuery == "" {
		return upstreamBase + proxyPath
	}

	return upstreamBase + proxyPath + "?" + rawQuery
}

func copyHeaders(dst, src http.Header) {
	maps.Copy(dst, src)
}

func requestHasReplayableBody(r *http.Request) bool {
	return r.Method != http.MethodGet && r.Method != http.MethodHead
}
