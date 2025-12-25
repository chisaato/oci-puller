package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
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
	upstreams := make(map[string]string)
	for _, reg := range cfg.Registries {
		if len(reg.URLs) > 0 {
			// 目前简化为使用第一个 URL
			// 理想情况下，我们应该支持故障转移或负载均衡
			upstreams[reg.Host] = reg.URLs[0]
			logger.S.Debugf("已注册上游: %s -> %s", reg.Host, reg.URLs[0])
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
	upstreams map[string]string
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

	upstreamBase, ok := h.upstreams[hostname]
	if !ok {
		logger.S.Warnw("该主机未配置上游服务", "host", hostname, "path", r.URL.Path)
		http.Error(w, "未为该主机配置匹配的上游服务: "+hostname, http.StatusNotFound)
		return
	}

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

		upstreamURL := upstreamBase + proxyPath
		logger.S.Infow("[Blob] 正在拦截请求",
			"digest", digest,
			"upstreamURL", upstreamURL,
		)

		if err := h.handleBlob(w, r, digest, upstreamURL); err != nil {
			logger.S.Warnw("Blob 加速失败，正在回退到代理模式", "digest", digest, "error", err)
			h.handleProxy(w, r, upstreamBase, proxyPath)
		}
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

		h.handleManifest(w, r, upstreamBase, proxyPath, tagOrDigest)
		return
	}

	// 5. 默认代理
	logger.S.Debugw("[Proxy] 直接透传", "path", proxyPath)
	h.handleProxy(w, r, upstreamBase, proxyPath)
}

func (h *ProxyHandler) handleManifest(w http.ResponseWriter, r *http.Request, upstreamBase, proxyPath, tag string) {
	// Full Upstream URL
	upstreamURL := upstreamBase + proxyPath

	req, err := http.NewRequest(r.Method, upstreamURL, nil)
	if err != nil {
		logger.S.Errorw("创建上游请求失败", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Copy headers
	for k, v := range r.Header {
		req.Header[k] = v
	}

	// Do Request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.S.Warnw("上游 Manifest 请求失败", "error", err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy Response Headers
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)

	// If not 200 OK, just stream back and exit
	if resp.StatusCode != http.StatusOK {
		io.Copy(w, resp.Body)
		return
	}

	// TeeReader to capture body
	// 注意：如果 Body 很大，全部读入内存可能会有问题。但 Manifest 通常很小（几 KB）。
	// 所以我们可以直接 ReadAll，然后再 Write 给 Client。
	// 或者用 TeeReader 边写边算。

	// 为了确保我们能获取完整的 body 用于计算 hash，必须确保 Client 读取完毕或者我们主动读取完毕。
	// 这里选择 ReadAll 方案，因为 Manifest 很小，这样最安全。
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.S.Errorw("读取 Manifest Body 失败", "error", err)
		return
	}

	// Write response to client immediately
	if _, err := w.Write(bodyBytes); err != nil {
		logger.S.Warnw("向客户端写入 Manifest 失败", "error", err)
		return
	}

	// Async Side Effect: Save to Index
	// Make sure we don't save error responses (already checked status 200)
	// Make sure the request was for a Tag, not a digest (though if it is a digest, saving it is harmless)
	// If path ends with sha256:..., then tag is actually a digest.

	contentType := resp.Header.Get("Content-Type")
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

func (h *ProxyHandler) handleProxy(w http.ResponseWriter, r *http.Request, upstreamBase, proxyPath string) {
	remote, err := url.Parse(upstreamBase)
	if err != nil {
		logger.S.Errorw("上游 URL 无效", "url", upstreamBase, "error", err)
		http.Error(w, "上游 URL 无效", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(remote)
	r.URL.Path = proxyPath
	r.Host = remote.Host

	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.URL.Path = proxyPath
		req.Host = remote.Host // 对于 Registry 很重要
	}

	proxy.ServeHTTP(w, r)
}
