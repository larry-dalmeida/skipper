package cache

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zalando/skipper/eskip"
	"github.com/zalando/skipper/filters"
	"github.com/zalando/skipper/filters/builtin"
	skipperproxy "github.com/zalando/skipper/proxy"
	"github.com/zalando/skipper/proxy/proxytest"
)

func TestProxy_CacheVary304PreservesBodyMetadata(t *testing.T) {
	for _, mode := range []string{"rfc-loopback", "force-loopback", "force-direct"} {
		for _, contentLength := range []bool{false, true} {
			for _, status := range []int{http.StatusOK, http.StatusNotFound} {
				t.Run(fmt.Sprintf("%s/length=%t/status=%d", mode, contentLength, status), func(t *testing.T) {
					testProxyCacheVary304(t, mode, contentLength, status, false)
				})
			}
		}
	}
}

func TestProxy_CacheVary304EntryEvicted(t *testing.T) {
	for _, mode := range []string{"rfc-loopback", "force-loopback", "force-direct"} {
		t.Run(mode, func(t *testing.T) {
			testProxyCacheVary304(t, mode, true, http.StatusOK, true)
		})
	}
}

func testProxyCacheVary304(t *testing.T, mode string, contentLength bool, status int, evict bool) {
	t.Helper()
	const lastModified = "Mon, 02 Nov 2026 12:00:00 GMT"
	payload := strings.Repeat("representation", 200)
	var calls, conditional atomic.Int64
	var evictEntry func()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		language := r.Header.Get("Accept-Language")
		w.Header().Set("Vary", "Accept-Language")
		w.Header().Set("ETag", `"`+language+`"`)
		w.Header().Set("Cache-Control", "public, max-age=15")
		if r.Header.Get("If-None-Match") == `"`+language+`"` {
			conditional.Add(1)
			if r.Header.Get("If-Modified-Since") != lastModified {
				t.Error("refresh lost Last-Modified validator")
			}
			if evict {
				evictEntry()
			}
			if contentLength {
				w.Header().Set("Content-Length", fmt.Sprint(len(language+payload)))
			}
			w.Header().Set("X-Revalidated", "yes")
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", fmt.Sprint(len(language+payload)))
		w.Header().Set("Last-Modified", lastModified)
		w.Header().Set("X-Representation", language)
		w.WriteHeader(status)
		fmt.Fprint(w, language+payload)
	}))
	defer backend.Close()
	spec := NewCacheFilter(Options{MaxBytes: 8 << 20}).(*cacheSpec)
	defer spec.Close()
	registry := make(filters.Registry)
	registry.Register(spec)
	registry.Register(builtin.NewSetDynamicBackendUrl())
	args := []any{"15s", "15s", "15s", "24h", "Authorization"}
	if mode == "rfc-loopback" {
		args = []any{"Authorization"}
	}
	route := &eskip.Route{
		Id: "conditional", Path: "/variants", Backend: backend.URL,
		Filters: []*eskip.Filter{{Name: spec.Name(), Args: args}},
	}
	if mode == "force-loopback" {
		route.Backend = ""
		route.BackendType = eskip.DynamicBackend
		route.Filters = append(route.Filters, &eskip.Filter{Name: "setDynamicBackendUrl", Args: []any{backend.URL}})
	}
	p := proxytest.WithParams(registry, skipperproxy.Params{Flags: skipperproxy.PreserveOriginal}, route)
	defer p.Close()
	client := p.Client()
	client.Timeout = 10 * time.Second
	created, err := spec.CreateFilter(args)
	if err != nil {
		t.Fatal(err)
	}
	f := created.(*cacheFilter)
	f.listenAddr = strings.TrimPrefix(p.URL, "http://")
	request := func(language string, refreshed bool) *http.Request {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, p.URL+"/variants", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "fixture")
		req.Header.Set("Accept-Language", language)
		rsp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(rsp.Body)
		rsp.Body.Close()
		if err != nil || rsp.StatusCode != status || string(body) != language+payload || rsp.ContentLength != int64(len(body)) {
			t.Fatalf("language=%s status=%d bytes=%d length=%d err=%v", language, rsp.StatusCode, len(body), rsp.ContentLength, err)
		}
		if refreshed && (rsp.Header.Get(cacheStatusHeader) != cacheStatusHit || rsp.Header.Get("X-Revalidated") != "yes") {
			t.Fatalf("refreshed representation not served as HIT: %v", rsp.Header)
		}
		keyReq := req.Clone(context.Background())
		keyReq.URL.Scheme = ""
		return keyReq
	}
	for _, language := range []string{"en", "de"} {
		request(language, false)
	}
	for _, language := range []string{"en", "de"} {
		req, err := http.NewRequest(http.MethodGet, p.URL+"/variants", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.URL.Scheme = ""
		req.Header.Set("Authorization", "fixture")
		req.Header.Set("Accept-Language", language)
		base := cacheKey("conditional", req, f.keyHeaders)
		key := varyKey(base, req, []string{"Accept-Language"})
		before, err := f.storage.GetSilent(context.Background(), key)
		if err != nil || before == nil {
			t.Fatalf("missing %s seed: %v", language, err)
		}
		evictEntry = func() {
			if err := f.storage.Delete(context.Background(), key); err != nil {
				t.Error(err)
			}
		}
		dispatch := ""
		if mode == "force-direct" {
			dispatch = backend.URL
		}
		f.revalidate(key, base, req, snapshotCacheIdentity(req), dispatch, "")
		after, err := f.storage.GetSilent(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if evict {
			if after != nil {
				t.Fatalf("304 recreated evicted representation: status=%d bytes=%d", after.StatusCode, len(after.Payload))
			}
			continue
		}
		if after == nil || after.StatusCode != before.StatusCode || string(after.Payload) != string(before.Payload) {
			t.Fatalf("304 replaced %d representation (%d bytes)", before.StatusCode, len(before.Payload))
		}
		if after.Header.Get("Content-Type") != before.Header.Get("Content-Type") ||
			after.Header.Get("X-Representation") != language || after.LastModified != lastModified ||
			after.ETag != before.ETag || after.Header.Get("X-Revalidated") != "yes" ||
			len(after.VaryHeaders) != 1 || after.VaryHeaders[0] != "Accept-Language" {
			t.Fatalf("304 lost representation metadata: %+v", after)
		}
		if !after.CreatedAt.After(before.CreatedAt) || after.ResponseTime != after.CreatedAt || after.TTL != 15*time.Second {
			t.Fatalf("304 did not refresh freshness: %+v", after)
		}
		request(language, true)
	}
	if calls.Load() != 4 || conditional.Load() != 2 {
		t.Fatalf("origin calls=%d conditional=%d, want 4/2", calls.Load(), conditional.Load())
	}
}
