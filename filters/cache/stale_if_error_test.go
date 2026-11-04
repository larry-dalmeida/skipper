package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/zalando/skipper/eskip"
	"github.com/zalando/skipper/filters"
	"github.com/zalando/skipper/filters/builtin"
	"github.com/zalando/skipper/metrics/metricstest"
	skipperproxy "github.com/zalando/skipper/proxy"
	"github.com/zalando/skipper/proxy/proxytest"
)

type sieTimeoutReader struct{}

func (sieTimeoutReader) Read([]byte) (int, error) { return 0, context.DeadlineExceeded }

func newSIEFilter(t *testing.T, window time.Duration) (*cacheFilter, *metricstest.MockMetrics) {
	t.Helper()
	m := &metricstest.MockMetrics{}
	spec := NewCacheFilter(Options{MaxBytes: 1 << 20, ListenAddr: "localhost:9090", Metrics: m}).(*cacheSpec)
	t.Cleanup(func() { spec.Close() })
	filter, err := spec.CreateFilter([]any{"5m", "15s", "5m", window.String(), "Authorization"})
	if err != nil {
		t.Fatal(err)
	}
	return filter.(*cacheFilter), m
}

func seedSIEStale(t *testing.T, f *cacheFilter, age time.Duration, status int) string {
	t.Helper()
	ctx := newCtx(http.MethodGet, "http://origin.example/retention", "fixture")
	key := cacheKey(ctx.RouteId(), ctx.Request(), f.keyHeaders)
	created := time.Now().Add(-age)
	entry := &Entry{
		StatusCode: status, Header: http.Header{"Etag": {`"original"`}},
		Payload: []byte("last-good"), CreatedAt: created, ResponseTime: created,
		TTL: 5 * time.Minute, StaleWhileRevalidate: 5 * time.Minute,
		StaleIfError: 24 * time.Hour, ETag: `"original"`, CorrectedInitialAge: time.Second,
	}
	if err := f.storage.Set(context.Background(), key, entry); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestCacheFilter_SIE_WindowExpiresDuringFetch(t *testing.T) {
	for _, failure := range []string{"503", "transport", "incomplete body"} {
		t.Run(failure, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newSIEFilter(t, 24*time.Hour)
				seedSIEStale(t, f, 24*time.Hour+5*time.Minute-time.Second, http.StatusOK)
				f.fetch = func(*http.Request) (*http.Response, error) {
					// Advance the fake clock across the measured SIE endpoint.
					time.Sleep(2 * time.Second)
					switch failure {
					case "transport":
						return nil, context.DeadlineExceeded
					case "incomplete body":
						return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(sieTimeoutReader{})}, nil
					default:
						return upstreamResponse(503, "unavailable"), nil
					}
				}
				ctx := newCtx(http.MethodGet, "http://origin.example/retention", "fixture")
				f.Request(ctx)
				if failure == "503" {
					if !ctx.FServed || ctx.FResponse.StatusCode != 503 {
						t.Fatal("want origin 503 after SIE expires during fetch")
					}
					ctx.FResponse.Body.Close()
				} else if ctx.FServed {
					ctx.FResponse.Body.Close()
					t.Fatal("stale response served beyond SIE window")
				}
			})
		})
	}
}

func TestCacheFilter_SIE_FailureClassesPreserveStoredResponse(t *testing.T) {
	for _, failure := range []struct {
		name      string
		status    int
		err       error
		bodyError bool
	}{
		{name: "500", status: 500}, {name: "502", status: 502},
		{name: "503", status: 503}, {name: "504", status: 504},
		{name: "network", err: errors.New("connection reset by peer")},
		{name: "timeout", err: context.DeadlineExceeded},
		{name: "body timeout", status: 200, bodyError: true},
		{name: "incomplete body", status: 200, err: io.ErrUnexpectedEOF, bodyError: true},
	} {
		for _, background := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/background=%t", failure.name, background), func(t *testing.T) {
				f, m := newSIEFilter(t, 24*time.Hour)
				key := seedSIEStale(t, f, 11*time.Minute, http.StatusOK)
				before, err := f.storage.GetSilent(context.Background(), key)
				if err != nil || before == nil {
					t.Fatalf("seed absent: %v", err)
				}
				fetches := 0
				f.fetch = func(*http.Request) (*http.Response, error) {
					fetches++
					if failure.bodyError {
						body := io.ReadCloser(io.NopCloser(sieTimeoutReader{}))
						if failure.err != nil {
							body = &bypassResponseBody{reader: strings.NewReader("partial"), err: failure.err}
						}
						return &http.Response{StatusCode: failure.status, Header: http.Header{}, Body: body}, nil
					}
					if failure.err != nil {
						return nil, failure.err
					}
					return upstreamResponse(failure.status, "unavailable"), nil
				}
				for range 2 {
					ctx := newCtx(http.MethodGet, "http://origin.example/retention", "fixture")
					if background {
						f.doRevalidate(key, ctx.Request(), "http://origin.example", "")
					} else {
						f.Request(ctx)
						if !ctx.FServed || ctx.FResponse.StatusCode != http.StatusOK {
							t.Fatal("eligible stale response not served")
						}
						body, err := io.ReadAll(ctx.FResponse.Body)
						ctx.FResponse.Body.Close()
						if err != nil || string(body) != "last-good" || ctx.FResponse.Header.Get(cacheStatusHeader) != cacheStatusStale ||
							ctx.FResponse.Header.Get("Age") != "661" {
							t.Fatalf("unexpected stale response: %q, %v, %v", body, ctx.FResponse.Header, err)
						}
					}
					after, err := f.storage.GetSilent(context.Background(), key)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("stored response changed: before=%+v after=%+v err=%v", before, after, err)
					}
				}
				if fetches != 2 {
					t.Fatalf("want two origin attempts, got %d", fetches)
				}
				if !background {
					if got, _ := m.Counter("cache.stale"); got != 2 {
						t.Errorf("stale counter=%d, want 2", got)
					}
					if got, _ := m.CacheResultCount("stale", "2xx"); got != 2 {
						t.Errorf("labelled stale counter=%d, want 2", got)
					}
				}
			})
		}
	}
}

func TestCacheFilter_SIE_EligibilityAndFailureBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name       string
		age        time.Duration
		storedCode int
		status     int
		disabled   bool
		transport  bool
	}{
		{name: "expired", age: 25 * time.Hour, storedCode: 200, status: 503},
		{name: "expired transport", age: 25 * time.Hour, storedCode: 200, transport: true},
		{name: "disabled", age: 11 * time.Minute, storedCode: 200, status: 503, disabled: true},
		{name: "cached error", age: 11 * time.Minute, storedCode: 503, status: 503},
		{name: "501 excluded", age: 11 * time.Minute, storedCode: 200, status: 501},
		{name: "505 excluded", age: 11 * time.Minute, storedCode: 200, status: 505},
		{name: "healthy origin", age: 11 * time.Minute, storedCode: 200, status: 200},
	} {
		t.Run(tt.name, func(t *testing.T) {
			window := 24 * time.Hour
			if tt.disabled {
				window = 0
			}
			f, _ := newSIEFilter(t, window)
			key := seedSIEStale(t, f, tt.age, tt.storedCode)
			f.fetch = func(*http.Request) (*http.Response, error) {
				if tt.transport {
					return nil, errors.New("origin down")
				}
				return upstreamResponse(tt.status, "origin"), nil
			}
			ctx := newCtx(http.MethodGet, "http://origin.example/retention", "fixture")
			f.Request(ctx)
			if tt.transport {
				if ctx.FServed {
					ctx.FResponse.Body.Close()
					t.Fatal("ineligible stale response served on transport error")
				}
				return
			}
			if !ctx.FServed || ctx.FResponse.StatusCode != tt.status || ctx.FResponse.Header.Get(cacheStatusHeader) != cacheStatusMiss {
				t.Fatal("want original origin response as MISS")
			}
			ctx.FResponse.Body.Close()
			stored, err := f.storage.GetSilent(context.Background(), key)
			wantTTL := 15 * time.Second
			if tt.status == 200 {
				wantTTL = 5 * time.Minute
			}
			if err != nil || stored == nil || stored.StatusCode != tt.status || stored.TTL != wantTTL || string(stored.Payload) != "origin" {
				t.Fatalf("normal origin caching changed: %+v, %v", stored, err)
			}
		})
	}
}

func TestCacheFilter_SIE_EligibilityWindow(t *testing.T) {
	for _, tt := range []struct {
		name        string
		filterLimit time.Duration
		entryLimit  time.Duration
		staleness   time.Duration
		wantStale   bool
	}{
		{name: "filter endpoint inclusive", filterLimit: time.Minute, entryLimit: 24 * time.Hour, staleness: time.Minute, wantStale: true},
		{name: "filter endpoint exceeded", filterLimit: time.Minute, entryLimit: 24 * time.Hour, staleness: time.Minute + time.Nanosecond},
		{name: "entry endpoint inclusive", filterLimit: 24 * time.Hour, entryLimit: time.Minute, staleness: time.Minute, wantStale: true},
		{name: "entry endpoint exceeded", filterLimit: 24 * time.Hour, entryLimit: time.Minute, staleness: time.Minute + time.Nanosecond},
		{name: "legacy entry uses filter limit", filterLimit: time.Minute, staleness: time.Minute, wantStale: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := newSIEFilter(t, tt.filterLimit)
				key := seedSIEStale(t, f, 5*time.Minute+tt.staleness, 200)
				entry, err := f.storage.GetSilent(context.Background(), key)
				if err != nil || entry == nil {
					t.Fatalf("seed absent: %v", err)
				}
				entry.StaleIfError = tt.entryLimit
				if err := f.storage.Set(context.Background(), key, entry); err != nil {
					t.Fatal(err)
				}
				f.fetch = func(*http.Request) (*http.Response, error) { return upstreamResponse(503, "failure"), nil }
				ctx := newCtx(http.MethodGet, "http://origin.example/retention", "fixture")
				// Force origin revalidation of the retained entry, independently of SWR.
				f.coalesce(ctx, key)
				if !ctx.FServed {
					t.Fatal("response not served")
				}
				ctx.FResponse.Body.Close()
				wantCode := 503
				if tt.wantStale {
					wantCode = 200
				}
				if ctx.FResponse.StatusCode != wantCode {
					t.Fatalf("status=%d, want %d at staleness %v", ctx.FResponse.StatusCode, wantCode, tt.staleness)
				}
			})
		})
	}
}

func TestCacheFilter_SIE_CoalescedFailure(t *testing.T) {
	for _, failure := range []string{"503", "transport", "incomplete body"} {
		t.Run(failure, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const participants = 8
				f, m := newSIEFilter(t, 24*time.Hour)
				key := seedSIEStale(t, f, 11*time.Minute, 200)
				before, err := f.storage.GetSilent(context.Background(), key)
				if err != nil || before == nil {
					t.Fatalf("seed absent: %v", err)
				}
				release := make(chan struct{})
				var fetches atomic.Int64
				failedBody := &bypassCountingBody{ReadCloser: &bypassResponseBody{reader: strings.NewReader("partial"), err: io.ErrUnexpectedEOF}}
				f.fetch = func(*http.Request) (*http.Response, error) {
					fetches.Add(1)
					<-release
					if failure == "transport" {
						return nil, context.DeadlineExceeded
					}
					if failure == "incomplete body" {
						return &http.Response{StatusCode: 200, Header: http.Header{}, Body: failedBody}, nil
					}
					return upstreamResponse(503, "failure"), nil
				}
				var joined, completed sync.WaitGroup
				joined.Add(participants)
				completed.Add(participants)
				for range participants {
					ctx := newCtx(http.MethodGet, "http://origin.example/retention", "fixture")
					ctx.FRequest = ctx.FRequest.WithContext(&coalesceBarrierContext{Context: ctx.Request().Context(), joined: &joined})
					go func() {
						defer completed.Done()
						f.Request(ctx)
						if !ctx.FServed {
							t.Error("waiter did not receive last-good response")
							return
						}
						body, err := io.ReadAll(ctx.FResponse.Body)
						ctx.FResponse.Body.Close()
						if err != nil || string(body) != "last-good" || ctx.FResponse.Header.Get(cacheStatusHeader) != cacheStatusStale {
							t.Errorf("waiter body=%q headers=%v err=%v", body, ctx.FResponse.Header, err)
						}
					}()
				}
				joined.Wait()
				close(release)
				completed.Wait()
				if fetches.Load() != 1 {
					t.Fatalf("origin calls=%d, want 1", fetches.Load())
				}
				if failure == "incomplete body" && failedBody.closes.Load() != 1 {
					t.Errorf("failed stream closes=%d, want 1", failedBody.closes.Load())
				}
				if got, _ := m.Counter("cache.stale"); got != participants {
					t.Errorf("stale outcomes=%d, want %d", got, participants)
				}
				if got, _ := m.CacheResultCount("stale", "2xx"); got != participants {
					t.Errorf("labelled stale outcomes=%d, want %d", got, participants)
				}
				if got, _ := m.Counter("cache.miss"); got != 0 {
					t.Errorf("SIE fallback attributed as %d misses", got)
				}
				after, err := f.storage.GetSilent(context.Background(), key)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("shared fetch changed retained entry: %+v, %v", after, err)
				}
			})
		})
	}
}

func TestCacheFilter_SIE_RetentionAfterProcessAndVolatileL2Restart(t *testing.T) {
	for _, resetL2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("resetL2=%t", resetL2), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l2 := newStubValkeyClient()
				newFilter := func() (*cacheFilter, func()) {
					// Warming metadata separation is owned by PR08; exercise retained L2 here.
					spec := NewCacheFilter(Options{MaxBytes: 1 << 20, L2Client: l2, IsNoL2Err: valkey.IsValkeyNil}).(*cacheSpec)
					filter, err := spec.CreateFilter([]any{"3s", "1s", "3s", "24s", "Authorization"})
					if err != nil {
						t.Fatal(err)
					}
					return filter.(*cacheFilter), func() { spec.Close() }
				}
				f, closeSeed := newFilter()
				f.fetch = func(*http.Request) (*http.Response, error) {
					resp := upstreamResponse(200, "A")
					resp.Header.Set("Vary", "Accept-Language")
					return resp, nil
				}
				newRequest := func() *http.Request {
					req := httptest.NewRequest(http.MethodGet, "http://origin.example/retention", nil)
					req.Header.Set("Authorization", "fixture")
					req.Header.Set("Accept-Language", "en")
					return req
				}
				seed := newCtx(http.MethodGet, "http://origin.example/retention", "fixture")
				seed.FRequest, seed.FOriginalRequest = newRequest(), newRequest()
				f.Request(seed)
				if !seed.FServed {
					t.Fatal("seed not served")
				}
				seed.FResponse.Body.Close()
				base := cacheKey(seed.RouteId(), seed.Request(), f.keyHeaders)
				key := varyKey(base, seed.Request(), []string{"Accept-Language"})
				before, err := f.storage.GetSilent(context.Background(), key)
				if err != nil || before == nil || l2.lastExpire != 27*time.Second {
					t.Fatalf("wrong initial retention: %+v err=%v TTL=%v", before, err, l2.lastExpire)
				}
				closeSeed()
				if resetL2 {
					l2.mu.Lock()
					clear(l2.data)
					l2.mu.Unlock()
				}
				f, closeProcess := newFilter()
				defer closeProcess()
				// Advance past TTL+SWR, while remaining within the original SIE window.
				time.Sleep(6*time.Second + time.Millisecond)
				fetches := 0
				f.fetch = func(*http.Request) (*http.Response, error) {
					fetches++
					resp := upstreamResponse(500, "failure")
					resp.Header.Set("Vary", "Accept-Language")
					return resp, nil
				}
				ctx := newCtx(http.MethodGet, "http://origin.example/retention", "fixture")
				ctx.FRequest, ctx.FOriginalRequest = newRequest(), newRequest()
				f.Request(ctx)
				wantCode, wantCache := 200, cacheStatusStale
				if resetL2 {
					wantCode, wantCache = 500, cacheStatusMiss
				}
				if !ctx.FServed || ctx.FResponse.StatusCode != wantCode || ctx.FResponse.Header.Get(cacheStatusHeader) != wantCache || fetches != 1 {
					t.Fatalf("resetL2=%t fetches=%d response=%+v", resetL2, fetches, ctx.FResponse)
				}
				body, err := io.ReadAll(ctx.FResponse.Body)
				ctx.FResponse.Body.Close()
				wantBody := "A"
				if resetL2 {
					wantBody = "failure"
				}
				if err != nil || string(body) != wantBody {
					t.Fatalf("body=%q err=%v", body, err)
				}
				if !resetL2 {
					after, err := f.storage.GetSilent(context.Background(), key)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("retained entry mutated: %+v, %v", after, err)
					}
				}
			})
		})
	}
}

func TestProxy_CacheLoopbackSIEPreservesVariant(t *testing.T) {
	for _, failure := range []string{"503", "transport", "incomplete body"} {
		for _, background := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/background=%t", failure, background), func(t *testing.T) {
				var recovering atomic.Bool
				var calls atomic.Int64
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Vary", "Accept-Language")
					if !recovering.Load() {
						if failure == "transport" {
							conn, _, err := w.(http.Hijacker).Hijack()
							if err != nil {
								t.Error(err)
								return
							}
							conn.Close()
							return
						}
						if failure == "incomplete body" {
							w.Header().Set("Content-Length", "100")
							io.WriteString(w, "partial")
							return
						}
						w.WriteHeader(http.StatusServiceUnavailable)
						io.WriteString(w, "unavailable")
						return
					}
					io.WriteString(w, r.Header.Get("Accept-Language")+"-recovered")
				}))
				defer backend.Close()
				spec := NewCacheFilter(Options{MaxBytes: 1 << 20}).(*cacheSpec)
				defer spec.Close()
				registry := make(filters.Registry)
				registry.Register(spec)
				registry.Register(builtin.NewSetDynamicBackendUrl())
				args := []any{"5m", "15s", "5m", "24h", "Authorization"}
				p := proxytest.WithParams(registry, skipperproxy.Params{Flags: skipperproxy.PreserveOriginal}, &eskip.Route{
					Id: "loopback-sie", Path: "/variants", BackendType: eskip.DynamicBackend,
					Filters: []*eskip.Filter{
						{Name: spec.Name(), Args: args},
						{Name: "setDynamicBackendUrl", Args: []any{backend.URL}},
					},
				})
				defer p.Close()
				client := p.Client()
				client.Timeout = 10 * time.Second
				filter, err := spec.CreateFilter(args)
				if err != nil {
					t.Fatal(err)
				}
				f := filter.(*cacheFilter)
				f.listenAddr = strings.TrimPrefix(p.URL, "http://")
				req := httptest.NewRequest(http.MethodGet, p.URL+"/variants", nil)
				req.Header.Set("Authorization", "fixture")
				req.Header.Set("Accept-Language", "en")
				keyReq := req.Clone(context.Background())
				keyReq.URL.Scheme = ""
				base := cacheKey("loopback-sie", keyReq, f.keyHeaders)
				key := varyKey(base, keyReq, []string{"Accept-Language"})
				seed := &Entry{
					StatusCode: 200, Header: http.Header{"Vary": {"Accept-Language"}},
					Payload: []byte("last-good"), CreatedAt: time.Now().Add(-11 * time.Minute),
					TTL: 5 * time.Minute, StaleWhileRevalidate: 5 * time.Minute, StaleIfError: 24 * time.Hour,
				}
				if err := f.storeEntry(context.Background(), base, key, keyReq, seed); err != nil {
					t.Fatal(err)
				}
				before, err := f.storage.GetSilent(context.Background(), key)
				if err != nil || before == nil {
					t.Fatalf("seed absent: %v", err)
				}
				otherReq := keyReq.Clone(context.Background())
				otherReq.Header.Set("Accept-Language", "de")
				otherKey := varyKey(base, otherReq, []string{"Accept-Language"})
				otherSeed := *before
				otherSeed.CreatedAt = time.Now()
				otherSeed.Payload = []byte("de-last-good")
				if err := f.storage.Set(context.Background(), otherKey, &otherSeed); err != nil {
					t.Fatal(err)
				}
				otherBefore, err := f.storage.GetSilent(context.Background(), otherKey)
				if err != nil || otherBefore == nil {
					t.Fatalf("other variant absent: %v", err)
				}
				request := func(wantBody, wantCache string) {
					t.Helper()
					out := req.Clone(context.Background())
					out.RequestURI = ""
					resp, err := client.Do(out)
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(resp.Body)
					resp.Body.Close()
					if err != nil || resp.StatusCode != 200 || string(body) != wantBody || resp.Header.Get(cacheStatusHeader) != wantCache {
						t.Fatalf("status=%d body=%q cache=%q err=%v", resp.StatusCode, body, resp.Header.Get(cacheStatusHeader), err)
					}
				}
				for range 2 {
					if background {
						f.revalidate(key, base, keyReq, snapshotCacheIdentity(keyReq), "", "")
					} else {
						request("last-good", cacheStatusStale)
					}
					after, err := f.storage.GetSilent(context.Background(), key)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("failed loopback refresh overwrote variant: before=%+v after=%+v err=%v", before, after, err)
					}
					otherAfter, err := f.storage.GetSilent(context.Background(), otherKey)
					if err != nil || !reflect.DeepEqual(otherBefore, otherAfter) {
						t.Fatalf("failure changed other variant: %+v, %v", otherAfter, err)
					}
				}
				recovering.Store(true)
				if background {
					f.revalidate(key, base, keyReq, snapshotCacheIdentity(keyReq), "", "")
				} else {
					request("en-recovered", cacheStatusMiss)
				}
				request("en-recovered", cacheStatusHit)
				if calls.Load() != 3 {
					t.Fatalf("want two failures and one recovery fetch, got %d", calls.Load())
				}
			})
		}
	}
}
