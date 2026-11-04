package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/zalando/skipper/eskip"
	"github.com/zalando/skipper/filters"
	"github.com/zalando/skipper/filters/builtin"
	"github.com/zalando/skipper/metrics/metricstest"
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

func TestCacheFilter_ConditionalRefreshSilentReads(t *testing.T) {
	for _, tt := range []struct {
		name       string
		validator  string
		merge      string
		warm       bool
		wantFaults int64
	}{
		{name: "L2 representation"},
		{name: "L1 warming", warm: true},
		{name: "validator miss", validator: "miss"},
		{name: "validator transport error", validator: "transport"},
		{name: "validator decode error", validator: "decode", wantFaults: 2},
		{name: "merge miss", merge: "miss"},
		{name: "merge transport error", merge: "transport"},
		{name: "merge decode error", merge: "decode", wantFaults: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const key = "conditional-representation"
			stub := newStubValkeyClient()
			m := &metricstest.MockMetrics{}
			l1TTL := time.Duration(0)
			if tt.warm {
				l1TTL = time.Minute
			}
			spec := NewCacheFilter(Options{
				MaxBytes: 8 << 20, Metrics: m, L2Client: stub,
				L1TTL: l1TTL, IsNoL2Err: valkey.IsValkeyNil,
			}).(*cacheSpec)
			defer spec.Close()
			created, err := spec.CreateFilter([]any{"1m", "1s", "1m"})
			if err != nil {
				t.Fatal(err)
			}
			f := created.(*cacheFilter)
			entry := &Entry{
				StatusCode: http.StatusOK, Payload: []byte("representation"),
				Header: http.Header{"Etag": {`"v1"`}, "Last-Modified": {"Mon, 02 Nov 2026 12:00:00 GMT"}},
				ETag:   `"v1"`, LastModified: "Mon, 02 Nov 2026 12:00:00 GMT",
				CreatedAt: time.Now(), TTL: time.Minute,
			}
			data, err := json.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			if err := stub.SetWithExpire(context.Background(), key, string(data), time.Minute); err != nil {
				t.Fatal(err)
			}
			change := func(kind string) {
				stub.mu.Lock()
				defer stub.mu.Unlock()
				switch kind {
				case "miss":
					delete(stub.data, key)
				case "transport":
					stub.broken = true
				case "decode":
					stub.data[key] = "{invalid JSON"
				}
			}
			change(tt.validator)
			var fetches int
			f.fetch = func(req *http.Request) (*http.Response, error) {
				fetches++
				if tt.validator == "" {
					if req.Header.Get("If-None-Match") != entry.ETag || req.Header.Get("If-Modified-Since") != entry.LastModified {
						t.Error("internal validator lookup lost conditional headers")
					}
				} else if req.Header.Get("If-None-Match") != "" || req.Header.Get("If-Modified-Since") != "" {
					t.Error("failed validator read invented conditional headers")
				}
				change(tt.merge)
				return &http.Response{
					StatusCode: http.StatusNotModified,
					Header:     http.Header{"X-Revalidated": {"yes"}}, Body: http.NoBody,
				}, nil
			}
			req := httptest.NewRequest(http.MethodGet, "http://origin.example/variants", nil)
			f.revalidate(key, key, req, snapshotCacheIdentity(req), "http://origin.example", "")
			if fetches != 1 {
				t.Fatalf("origin fetches=%d, want 1", fetches)
			}
			wantReads := 2
			if tt.warm {
				wantReads = 1
			}
			stub.mu.Lock()
			getCalls := stub.getCalls
			refreshed := stub.data[key]
			stub.mu.Unlock()
			if getCalls != wantReads {
				t.Errorf("physical L2 reads=%d, want %d", getCalls, wantReads)
			}
			assertL2GetMeasurements(t, m, wantReads)
			assertSilentLookupOutcomes(t, m)
			if faults, _ := m.StorageErrorCount("l2", "get"); faults != tt.wantFaults {
				t.Errorf("L2 decode fault hooks=%d, want %d", faults, tt.wantFaults)
			}
			if faults, _ := m.Counter("cache.storage_error"); faults != tt.wantFaults {
				t.Errorf("storage errors=%d, want %d", faults, tt.wantFaults)
			}
			if faults, _ := m.CacheResultCount("storage_error", "n/a"); faults != tt.wantFaults {
				t.Errorf("labelled storage errors=%d, want %d", faults, tt.wantFaults)
			}
			if tt.validator != "" || tt.merge != "" {
				m.WithMeasures(func(measures map[string][]time.Duration) {
					if len(measures["cache.l2_set_duration"]) != 0 {
						t.Error("failed retained-representation read still wrote a refresh")
					}
				})
				return
			}
			var stored Entry
			if err := json.Unmarshal([]byte(refreshed), &stored); err != nil {
				t.Fatal(err)
			}
			if string(stored.Payload) != "representation" || stored.ETag != entry.ETag || stored.Header.Get("X-Revalidated") != "yes" {
				t.Fatalf("silent merge lost representation: %+v", stored)
			}
			m.WithMeasures(func(measures map[string][]time.Duration) {
				if len(measures["cache.l2_set_duration"]) != 1 {
					t.Error("refresh lost L2 write timing")
				}
			})
			warmed, err := spec.lruStorage.GetSilent(context.Background(), key)
			if err != nil || (warmed != nil) != tt.warm {
				t.Fatalf("L1 warming changed: entry=%+v err=%v", warmed, err)
			}
			if _, err := f.storage.Get(context.Background(), key); err != nil {
				t.Fatal(err)
			}
			result := "l2_hit"
			if tt.warm {
				result = "l1_hit"
			}
			if got, _ := m.Counter("cache." + result); got != 1 {
				t.Errorf("ordinary lookup %s=%d, want 1", result, got)
			}
			if got, _ := m.CacheResultCount(result, "2xx"); got != 1 {
				t.Errorf("ordinary labelled lookup %s=%d, want 1", result, got)
			}
		})
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
