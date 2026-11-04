package skipper

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/zalando/skipper/filters"
	"github.com/zalando/skipper/filters/cache"
	"github.com/zalando/skipper/filters/filtertest"
	"github.com/zalando/skipper/metrics"
	"github.com/zalando/skipper/metrics/metricstest"
	"github.com/zalando/skipper/proxy"
)

func TestCacheNetOptions_BackendTimeoutMapping(t *testing.T) {
	for _, tt := range []struct {
		name                                    string
		o                                       Options
		header, handshake, expectContinue, idle time.Duration
		maxIdle                                 int
	}{
		{
			name: "configured",
			o: Options{
				TimeoutBackend: time.Second, KeepAliveBackend: 6 * time.Second,
				ResponseHeaderTimeoutBackend: 2 * time.Second, TLSHandshakeTimeoutBackend: 3 * time.Second,
				ExpectContinueTimeoutBackend: 4 * time.Second, CloseIdleConnsPeriod: 5 * time.Second,
				IdleConnectionsPerHost: 7,
			},
			header: 2 * time.Second, handshake: 3 * time.Second,
			expectContinue: 4 * time.Second, idle: 5 * time.Second, maxIdle: 7,
		},
		{
			name: "defaults", header: proxy.DefaultResponseHeaderTimeout,
			expectContinue: proxy.DefaultExpectContinueTimeout, idle: proxy.DefaultCloseIdleConnsPeriod,
		},
		{
			name:   "disabled header timeout",
			o:      Options{ResponseHeaderTimeoutBackend: -time.Second},
			header: -time.Second, expectContinue: proxy.DefaultExpectContinueTimeout,
			idle: proxy.DefaultCloseIdleConnsPeriod,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.o.cacheNetOptions()
			require.Zero(t, opts.Timeout, "dial budget must not become a whole-request/body deadline")
			require.Equal(t, tt.idle, opts.IdleConnTimeout)
			tr := opts.Transport
			require.NotNil(t, tr)
			require.NotNil(t, tr.DialContext)
			require.NotNil(t, tr.Proxy)
			require.Equal(t, tt.header, tr.ResponseHeaderTimeout)
			require.Equal(t, tt.handshake, tr.TLSHandshakeTimeout)
			require.Equal(t, tt.expectContinue, tr.ExpectContinueTimeout)
			require.Equal(t, tt.idle, tr.IdleConnTimeout)
			require.Equal(t, tt.maxIdle, tr.MaxIdleConnsPerHost)
			require.False(t, tr.DisableCompression)
			require.False(t, tr.DisableKeepAlives)
			require.Nil(t, tr.TLSClientConfig, "keep system trust without proxy TLS customization")
			require.Nil(t, opts.CheckRedirect)
		})
	}
}

// timeoutTestL2 records the single retained representation and its writes.
type timeoutTestL2 struct {
	mu    sync.Mutex
	value string
	sets  int
}

func (s *timeoutTestL2) Get(_ context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.HasPrefix(key, "vary:") || s.value == "" {
		return "", redis.Nil
	}
	return s.value, nil
}

func (s *timeoutTestL2) SetWithExpire(_ context.Context, _ string, value string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value = value
	s.sets++
	return nil
}

func (s *timeoutTestL2) Del(context.Context, string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value = ""
	return 1, nil
}

// Exercise the cache registered by production startup, stopping before a listener opens.
func withRegisteredTimeoutCache(t *testing.T, o Options, exercise func(filters.Filter)) {
	t.Helper()
	previousMetrics := metrics.Default
	defer func() { metrics.Default = previousMetrics }()
	o.MetricsBackend = &metricstest.MockMetrics{}
	o.InlineRoutes = `r: * -> <shunt>;`
	o.Address = "invalid-address"
	o.ResponseCacheMaxMemoryBytes = 1 << 20
	called := false
	o.RegisterFilters = func(registry filters.Registry) {
		called = true
		filter, err := registry[cache.Name].CreateFilter([]any{"5m", "15s", "5m", "24h"})
		require.NoError(t, err)
		exercise(filter)
	}
	err := run(o, nil, nil)
	require.ErrorContains(t, err, "missing port in address")
	require.True(t, called)
}

func timeoutCacheRequest(t *testing.T, filter filters.Filter, backendURL string) *filtertest.Context {
	t.Helper()
	ctx := &filtertest.Context{
		FRequest:  httptest.NewRequest(http.MethodGet, "http://client.example/timeout", nil),
		FStateBag: make(map[string]any), FRouteId: "timeout", FBackendUrl: backendURL,
	}
	filter.Request(ctx)
	return ctx
}

func timeoutCacheBody(t *testing.T, ctx *filtertest.Context) string {
	t.Helper()
	require.True(t, ctx.FServed)
	require.NotNil(t, ctx.FResponse)
	body, err := io.ReadAll(ctx.FResponse.Body)
	require.NoError(t, err)
	require.NoError(t, ctx.FResponse.Body.Close())
	return string(body)
}

func TestCacheBackendHeaderTimeout_PreservesStaleAndRecovers(t *testing.T) {
	var recovering atomic.Bool
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !recovering.Load() {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.Header().Set("ETag", `"recovered"`)
		_, err := io.WriteString(w, "recovered")
		if err != nil && r.Context().Err() == nil {
			t.Errorf("write origin body: %v", err)
		}
	}))
	defer backend.Close()
	stale := &cache.Entry{
		StatusCode: http.StatusOK, Header: http.Header{"Etag": {`"last-good"`}},
		Payload: []byte("last-good"), CreatedAt: time.Now().Add(-11 * time.Minute),
		ResponseTime: time.Now().Add(-11 * time.Minute), TTL: 5 * time.Minute,
		StaleWhileRevalidate: 5 * time.Minute, StaleIfError: 24 * time.Hour,
	}
	data, err := json.Marshal(stale)
	require.NoError(t, err)
	store := &timeoutTestL2{value: string(data)}
	withRegisteredTimeoutCache(t, Options{
		ResponseHeaderTimeoutBackend: 25 * time.Millisecond, TimeoutBackend: time.Second,
		EnableL2Cache: true, L2CacheClient: store,
	}, func(filter filters.Filter) {
		for i := range 2 {
			start := time.Now()
			ctx := timeoutCacheRequest(t, filter, backend.URL)
			require.Less(t, time.Since(start), 200*time.Millisecond, "25ms header timeout ignored")
			require.Equal(t, "last-good", timeoutCacheBody(t, ctx))
			require.Equal(t, "STALE", ctx.FResponse.Header.Get("X-Cache-Status"))
			require.Equal(t, `"last-good"`, ctx.FResponse.Header.Get("ETag"))
			require.Equal(t, int64(i+1), calls.Load())
			store.mu.Lock()
			value, sets := store.value, store.sets
			store.mu.Unlock()
			require.Equal(t, string(data), value, "timeout changed payload or timestamps")
			require.Zero(t, sets)
		}
		recovering.Store(true)
		ctx := timeoutCacheRequest(t, filter, backend.URL)
		require.Equal(t, "recovered", timeoutCacheBody(t, ctx))
		require.Equal(t, "MISS", ctx.FResponse.Header.Get("X-Cache-Status"))
		store.mu.Lock()
		value, sets := store.value, store.sets
		store.mu.Unlock()
		var fresh cache.Entry
		require.NoError(t, json.Unmarshal([]byte(value), &fresh))
		require.Equal(t, 1, sets)
		require.Equal(t, []byte("recovered"), fresh.Payload)
		require.Equal(t, http.StatusOK, fresh.StatusCode)
		require.Equal(t, `"recovered"`, fresh.Header.Get("ETag"))
		require.True(t, fresh.CreatedAt.After(stale.CreatedAt))
		require.True(t, fresh.ResponseTime.After(stale.ResponseTime))
		ctx = timeoutCacheRequest(t, filter, backend.URL)
		require.Equal(t, "recovered", timeoutCacheBody(t, ctx))
		require.Equal(t, "HIT", ctx.FResponse.Header.Get("X-Cache-Status"))
		require.Equal(t, int64(3), calls.Load())
		store.mu.Lock()
		require.Equal(t, 1, store.sets)
		store.mu.Unlock()
	})
}

func TestCacheBackendHeaderTimeout_WithoutStale(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(250 * time.Millisecond):
			_, _ = io.WriteString(w, "delayed")
		}
	}))
	defer backend.Close()
	withRegisteredTimeoutCache(t, Options{ResponseHeaderTimeoutBackend: 25 * time.Millisecond}, func(filter filters.Filter) {
		start := time.Now()
		ctx := timeoutCacheRequest(t, filter, backend.URL)
		require.Less(t, time.Since(start), 200*time.Millisecond)
		require.False(t, ctx.FServed, "a timed-out origin without SIE must not be served as success")
		require.Nil(t, ctx.FResponse)
	})
}

func TestCacheBackendTimeout_AllowsSlowBody(t *testing.T) {
	headersFlushed := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(headersFlushed)
		select {
		case <-r.Context().Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
		_, err := io.WriteString(w, "complete slow body")
		if err != nil && r.Context().Err() == nil {
			t.Errorf("write slow origin body: %v", err)
		}
	}))
	defer backend.Close()
	withRegisteredTimeoutCache(t, Options{
		TimeoutBackend: 25 * time.Millisecond, ResponseHeaderTimeoutBackend: 25 * time.Millisecond,
	}, func(filter filters.Filter) {
		start := time.Now()
		ctx := timeoutCacheRequest(t, filter, backend.URL)
		<-headersFlushed
		require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
		require.Equal(t, "complete slow body", timeoutCacheBody(t, ctx))
		require.Equal(t, "MISS", ctx.FResponse.Header.Get("X-Cache-Status"))
	})
}
