package cache

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zalando/skipper/metrics/metricstest"
)

func TestCacheFilter_QueuedRefresh(t *testing.T) {
	for _, failure := range []string{"none", "503", "transport", "incomplete_body"} {
		t.Run(failure, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m := &metricstest.MockMetrics{}
				spec := NewCacheFilter(Options{MaxBytes: 1 << 20, ListenAddr: "localhost:9090", Metrics: m}).(*cacheSpec)
				defer spec.Close()
				filter, err := spec.CreateFilter([]any{"4s", "2s", "4s", "12s", "Authorization"})
				if err != nil {
					t.Fatal(err)
				}
				f := filter.(*cacheFilter)
				jobs := make(chan revalJob, 8)
				f.revalJobs = jobs
				calls := 0
				f.fetch = func(req *http.Request) (*http.Response, error) {
					calls++
					if req.Header.Get("Authorization") != "fixture" || req.Header.Get("Accept-Language") != "en" {
						t.Errorf("request identity lost: %+v", req.Header)
					}
					if calls == 2 && failure == "transport" {
						return nil, context.DeadlineExceeded
					}
					status, body := http.StatusOK, "B"
					if calls == 1 {
						body = "A"
					} else if calls == 2 && failure == "503" {
						status, body = http.StatusServiceUnavailable, "failure"
					}
					resp := &http.Response{
						StatusCode: status, Header: http.Header{"Vary": {"Accept-Language"}, "Etag": {`"original"`}},
						Body: io.NopCloser(strings.NewReader(body)),
					}
					if calls == 2 && failure == "incomplete_body" {
						resp.Body = io.NopCloser(sieTimeoutReader{})
					}
					return resp, nil
				}
				request := func(wantBody, wantStatus string) {
					t.Helper()
					ctx := newCtx(http.MethodGet, "http://origin.example/queued", "fixture")
					ctx.Request().Header.Set("Accept-Language", "en")
					f.Request(ctx)
					if !ctx.FServed {
						t.Fatal("not served")
					}
					body, err := io.ReadAll(ctx.FResponse.Body)
					ctx.FResponse.Body.Close()
					if err != nil {
						t.Fatal(err)
					}
					if string(body) != wantBody || ctx.FResponse.Header.Get(cacheStatusHeader) != wantStatus {
						t.Fatalf("body=%q cache status=%s, want %q %s", body, ctx.FResponse.Header.Get(cacheStatusHeader), wantBody, wantStatus)
					}
				}
				run := func(job revalJob) {
					spec.revalJobs <- job
					synctest.Wait()
				}
				request("A", cacheStatusMiss)
				// Advance across the measured freshness boundary, not worker readiness.
				time.Sleep(4*time.Second + time.Millisecond)
				request("A", cacheStatusStale)
				request("A", cacheStatusStale)
				if len(jobs) != 2 {
					t.Fatalf("queued jobs=%d, want 2", len(jobs))
				}
				first, second := <-jobs, <-jobs
				if first.key != second.key || first.key == first.baseKey || first.enqueuedAt.IsZero() {
					t.Fatalf("expected timestamped jobs for the same isolated variant: %+v %+v", first, second)
				}
				before, err := f.storage.GetSilent(context.Background(), first.key)
				if err != nil || before == nil {
					t.Fatalf("stored variant=%+v err=%v", before, err)
				}
				run(first)
				if failure != "none" {
					after, err := f.storage.GetSilent(context.Background(), first.key)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("failed refresh changed retained representation: before=%+v after=%+v err=%v", before, after, err)
					}
					request("A", cacheStatusStale)
				} else {
					request("B", cacheStatusHit)
				}
				run(second)
				request("B", cacheStatusHit)
				for len(jobs) > 0 {
					run(<-jobs)
				}
				wantCalls := 2
				if failure != "none" {
					wantCalls = 3
				}
				if calls != wantCalls {
					t.Fatalf("origin calls=%d, want %d", calls, wantCalls)
				}
				wantErrors := int64(0)
				if failure != "none" {
					wantErrors = 1
				}
				if n, _ := m.Counter("cache.reval_error"); n != wantErrors {
					t.Fatalf("refresh errors=%d, want %d", n, wantErrors)
				}
			})
		})
	}
}

func TestCacheFilter_QueuedRefreshFreshness(t *testing.T) {
	for _, tt := range []struct {
		name        string
		createdAgo  time.Duration
		ttl         time.Duration
		missing     bool
		other       bool
		immediate   bool
		noQueueTime bool
		wantCalls   int
	}{
		{name: "newer_fresh", createdAgo: time.Second, wantCalls: 0},
		{name: "created_at_enqueue", createdAgo: 2 * time.Second, wantCalls: 0},
		{name: "created_before_enqueue", createdAgo: 3 * time.Second, wantCalls: 1},
		{name: "freshness_endpoint", createdAgo: time.Second, ttl: time.Second, wantCalls: 1},
		{name: "expired", createdAgo: 1500 * time.Millisecond, ttl: time.Second, wantCalls: 1},
		{name: "missing", missing: true, wantCalls: 1},
		{name: "different_variant", other: true, wantCalls: 1},
		{name: "immediate_refresh", createdAgo: time.Second, immediate: true, wantCalls: 1},
		{name: "untimestamped_job", createdAgo: time.Second, noQueueTime: true, wantCalls: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m := &metricstest.MockMetrics{}
				spec := NewCacheFilter(Options{MaxBytes: 1 << 20, ListenAddr: "localhost:9090", Metrics: m}).(*cacheSpec)
				defer spec.Close()
				filter, err := spec.CreateFilter([]any{"4s", "2s", "4s", "12s"})
				if err != nil {
					t.Fatal(err)
				}
				f := filter.(*cacheFilter)
				jobs := make(chan revalJob, 1)
				f.revalJobs = jobs
				ctx := newCtx(http.MethodGet, "http://origin.example/variants", "")
				ctx.Request().Header.Set("Accept-Language", "en")
				identity := snapshotCacheIdentity(ctx.Request())
				base := cacheKey(ctx.RouteId(), ctx.Request(), f.keyHeaders)
				key := varyKey(base, identity, []string{"Accept-Language"})
				f.enqueueRevalidation(key, base, ctx.Request(), identity, "http://origin.example", "virtual.example")
				job := <-jobs
				if tt.noQueueTime {
					job.enqueuedAt = time.Time{}
				}
				time.Sleep(2 * time.Second)
				ttl := tt.ttl
				if ttl == 0 {
					ttl = 4 * time.Second
				}
				entry := &Entry{
					StatusCode: 200, Payload: []byte("newer"), Header: http.Header{"Vary": {"Accept-Language"}},
					CreatedAt: time.Now().Add(-tt.createdAgo), ResponseTime: time.Now().Add(-tt.createdAgo),
					TTL: ttl, StaleWhileRevalidate: 4 * time.Second, StaleIfError: 12 * time.Second,
				}
				storedKey := key
				if tt.other {
					other := ctx.Request().Clone(context.Background())
					other.Header.Set("Accept-Language", "de")
					storedKey = varyKey(base, other, []string{"Accept-Language"})
				}
				if !tt.missing {
					if err := f.storage.Set(context.Background(), storedKey, entry); err != nil {
						t.Fatal(err)
					}
				}
				calls := 0
				f.fetch = func(req *http.Request) (*http.Response, error) {
					calls++
					if req.Header.Get("Accept-Language") != "en" || req.Host != "virtual.example" {
						t.Errorf("variant/dispatch identity lost: %+v", req)
					}
					resp := upstreamResponse(200, "refreshed")
					resp.Header.Set("Vary", "Accept-Language")
					return resp, nil
				}
				if tt.immediate {
					f.revalidate(key, base, job.req, job.identity, job.backendURL, job.outgoingHost)
				} else {
					spec.revalJobs <- job
					synctest.Wait()
				}
				if calls != tt.wantCalls {
					t.Fatalf("origin calls=%d, want %d", calls, tt.wantCalls)
				}
				if n, _ := m.Counter("cache.reval_backend_dispatch"); n != int64(tt.wantCalls) {
					t.Fatalf("dispatches=%d, want %d", n, tt.wantCalls)
				}
				after, err := f.storage.GetSilent(context.Background(), storedKey)
				if err != nil {
					t.Fatal(err)
				}
				if (tt.wantCalls == 0 || tt.other) && !reflect.DeepEqual(entry, after) {
					t.Fatalf("unrelated or superseding representation changed: before=%+v after=%+v", entry, after)
				}
				if tt.wantCalls == 1 {
					refreshed, err := f.storage.GetSilent(context.Background(), key)
					if err != nil || refreshed == nil || string(refreshed.Payload) != "refreshed" {
						t.Fatalf("refresh not stored under target variant: %+v err=%v", refreshed, err)
					}
				}
			})
		})
	}
}

func TestCacheFilter_QueuedRefreshWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := &metricstest.MockMetrics{}
		spec := NewCacheFilter(Options{MaxBytes: 1 << 20, ListenAddr: "localhost:9090", Metrics: m}).(*cacheSpec)
		defer spec.Close()
		filter, err := spec.CreateFilter([]any{"1m", "1s", "1m"})
		if err != nil {
			t.Fatal(err)
		}
		f := filter.(*cacheFilter)
		arrived, release := make(chan struct{}), make(chan struct{})
		calls := 0
		f.fetch = func(req *http.Request) (*http.Response, error) {
			calls++
			if req.URL.Path == "/block" {
				close(arrived)
				<-release
			}
			return upstreamResponse(200, "obsolete"), nil
		}
		block := newCtx(http.MethodGet, "http://origin.example/block", "")
		f.enqueueRevalidation("block", "block", block.Request(), snapshotCacheIdentity(block.Request()), "http://origin.example", "")
		<-arrived
		target := newCtx(http.MethodGet, "http://origin.example/target", "")
		key := cacheKey(target.RouteId(), target.Request(), f.keyHeaders)
		f.enqueueRevalidation(key, key, target.Request(), snapshotCacheIdentity(target.Request()), "http://origin.example", "")
		f.enqueueRevalidation(key, key, target.Request(), snapshotCacheIdentity(target.Request()), "http://origin.example", "")
		entry := &Entry{
			StatusCode: 200, Payload: []byte("newer-fresh"), Header: http.Header{},
			CreatedAt: time.Now(), ResponseTime: time.Now(), TTL: time.Minute,
		}
		if err := f.storage.Set(context.Background(), key, entry); err != nil {
			t.Fatal(err)
		}
		close(release)
		synctest.Wait()
		if calls != 1 {
			t.Fatalf("origin calls=%d, want only the blocker", calls)
		}
		after, err := f.storage.GetSilent(context.Background(), key)
		if err != nil || !reflect.DeepEqual(entry, after) {
			t.Fatalf("queued jobs replaced newer response: before=%+v after=%+v err=%v", entry, after, err)
		}
		if n, _ := m.Counter("cache.reval_backend_dispatch"); n != 1 {
			t.Fatalf("dispatches=%d, want only the blocker", n)
		}
	})
}

func TestCacheFilter_QueuedRefreshGuards(t *testing.T) {
	for _, method := range []string{"missing_request", http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m := &metricstest.MockMetrics{}
				spec := NewCacheFilter(Options{MaxBytes: 1 << 20, Metrics: m}).(*cacheSpec)
				defer spec.Close()
				filter, err := spec.CreateFilter([]any{"1m", "1s", "1m"})
				if err != nil {
					t.Fatal(err)
				}
				f := filter.(*cacheFilter)
				f.fetch = func(*http.Request) (*http.Response, error) {
					t.Error("invalid queued refresh fetched origin")
					return upstreamResponse(200, "unexpected"), nil
				}
				job := revalJob{filter: f, key: "key", baseKey: "key", enqueuedAt: time.Now()}
				if method == http.MethodHead {
					job.req = newCtx(method, "http://origin.example/queued", "").Request()
				}
				spec.revalJobs <- job
				synctest.Wait()
				if n, _ := m.Counter("cache.reval_dropped"); n != 1 {
					t.Fatalf("invalid refresh drops=%d, want 1", n)
				}
			})
		})
	}
}
