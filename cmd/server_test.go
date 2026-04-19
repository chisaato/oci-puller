package cmd

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProxyHandlerServeHTTP_SingleHostMultipleUpstreamsRoundRobin(t *testing.T) {
	t.Parallel()

	upstreamA := newTestUpstreamServer(t, "upstream-a")
	defer upstreamA.Close()

	upstreamB := newTestUpstreamServer(t, "upstream-b")
	defer upstreamB.Close()

	handler := &ProxyHandler{
		upstreams: map[string]*upstreamPool{
			"registry.example.com": {
				urls: []string{upstreamA.URL, upstreamB.URL},
			},
		},
	}

	proxy := httptest.NewServer(handler)
	defer proxy.Close()

	assertProxyResponseUpstream(t, proxy.URL, "registry.example.com", "/v2/test/tags/list", "upstream-a")
	assertProxyResponseUpstream(t, proxy.URL, "registry.example.com", "/v2/test/tags/list", "upstream-b")
	assertProxyResponseUpstream(t, proxy.URL, "registry.example.com", "/v2/test/tags/list", "upstream-a")
}

func TestProxyHandlerServeHTTP_SingleHostSingleUpstreamCompatible(t *testing.T) {
	t.Parallel()

	upstream := newTestUpstreamServer(t, "single-upstream")
	defer upstream.Close()

	handler := &ProxyHandler{
		upstreams: map[string]*upstreamPool{
			"registry.example.com": {
				urls: []string{upstream.URL},
			},
		},
	}

	proxy := httptest.NewServer(handler)
	defer proxy.Close()

	assertProxyResponseUpstream(t, proxy.URL, "registry.example.com", "/v2/test/tags/list", "single-upstream")
	assertProxyResponseUpstream(t, proxy.URL, "registry.example.com", "/v2/test/tags/list", "single-upstream")
}

func newTestUpstreamServer(t *testing.T, upstreamID string) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream-ID", upstreamID)
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(upstreamID))
		if err != nil {
			t.Fatalf("写入测试响应失败: %v", err)
		}
	}))
}

func assertProxyResponseUpstream(t *testing.T, proxyURL, host, path, wantUpstream string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, proxyURL+path, nil)
	if err != nil {
		t.Fatalf("创建请求失败: %v", err)
	}
	req.Host = host

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			t.Fatalf("读取错误响应失败: %v", readErr)
		}
		t.Fatalf("响应状态码不符合预期: got=%d body=%s", resp.StatusCode, string(body))
	}

	gotUpstream := resp.Header.Get("X-Upstream-ID")
	if gotUpstream != wantUpstream {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			t.Fatalf("读取响应体失败: %v", readErr)
		}
		t.Fatalf("命中的上游不符合预期: got=%q want=%q body=%s", gotUpstream, wantUpstream, string(body))
	}
}
