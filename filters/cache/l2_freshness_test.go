package cache

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/zalando/skipper/metrics"
	"github.com/zalando/skipper/metrics/metricstest"
)

func TestL2StorageL1FreshnessBoundaries(t *testing.T) {
	const ttl = time.Minute
	for _, tt := range []struct {
		name       string
		ttl        time.Duration
		swr        time.Duration
		sie        time.Duration
		age        time.Duration
		wantL1     bool
		wantRetain bool
	}{
		{"fresh", ttl, 0, 0, ttl - time.Nanosecond, true, true},
		{"ttl boundary", ttl, 0, 0, ttl, false, true},
		{"expired", ttl, 0, 0, ttl + time.Nanosecond, false, false},
		{"swr fresh", ttl, ttl, 0, ttl - time.Nanosecond, true, true},
		{"swr ttl boundary", ttl, ttl, 0, ttl, false, true},
		{"swr stale", ttl, ttl, 0, ttl + time.Nanosecond, false, true},
		{"swr before expiry", ttl, ttl, 0, 2*ttl - time.Nanosecond, false, true},
		{"swr expiry boundary", ttl, ttl, 0, 2 * ttl, false, true},
		{"swr expired", ttl, ttl, 0, 2*ttl + time.Nanosecond, false, false},
		{"sie fresh", ttl, 0, ttl, ttl - time.Nanosecond, true, true},
		{"sie ttl boundary", ttl, 0, ttl, ttl, false, true},
		{"sie retained", ttl, 0, ttl, ttl + time.Nanosecond, false, true},
		{"sie before expiry", ttl, 0, ttl, 2*ttl - time.Nanosecond, false, true},
		{"sie expiry boundary", ttl, 0, ttl, 2 * ttl, false, true},
		{"sie expired", ttl, 0, ttl, 2*ttl + time.Nanosecond, false, false},
		{"sie beyond swr", ttl, ttl, 2 * ttl, 2*ttl + time.Nanosecond, false, true},
		{"swr beyond sie", ttl, 2 * ttl, ttl, 2*ttl + time.Nanosecond, false, true},
		{"zero ttl", 0, 0, 0, ttl, true, true},
		{"zero ttl swr stale", 0, ttl, 0, ttl / 2, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				client := newStubValkeyClient()
				m := &metricstest.MockMetrics{}
				l1 := NewLRUStorage(1<<20, nil, metrics.Void)
				s := NewL2Storage(client, l1, m, 0, valkey.IsValkeyNil)
				old := &Entry{
					StatusCode: http.StatusOK, Payload: []byte("old L1"), CreatedAt: time.Now(),
					TTL: tt.ttl, StaleWhileRevalidate: tt.swr, StaleIfError: tt.sie,
				}
				if err := l1.Set(ctx, "key", old); err != nil {
					t.Fatal(err)
				}
				time.Sleep(tt.age)
				fresh := &Entry{StatusCode: http.StatusOK, Payload: []byte("fresh L2"), CreatedAt: time.Now(), TTL: ttl}
				data, err := json.Marshal(fresh)
				if err != nil {
					t.Fatal(err)
				}
				if err := client.SetWithExpire(ctx, "key", string(data), ttl); err != nil {
					t.Fatal(err)
				}
				got, err := s.Get(ctx, "key")
				if err != nil || got == nil {
					t.Fatalf("Get: entry=%+v, err=%v", got, err)
				}
				wantPayload, wantGets := "fresh L2", 1
				wantL1Hits, wantL2Hits := int64(0), int64(1)
				if tt.wantL1 {
					wantPayload, wantGets = "old L1", 0
					wantL1Hits, wantL2Hits = 1, 0
				}
				if string(got.Payload) != wantPayload || client.getCalls != wantGets {
					t.Errorf("Get: payload=%q, L2 gets=%d; want %q, %d", got.Payload, client.getCalls, wantPayload, wantGets)
				}
				for name, want := range map[string]int64{"cache.l1_hit": wantL1Hits, "cache.l2_hit": wantL2Hits} {
					if got, _ := m.Counter(name); got != want {
						t.Errorf("%s=%d, want %d", name, got, want)
					}
				}
				retained, err := l1.Get(ctx, "key")
				if err != nil {
					t.Fatal(err)
				}
				if (retained != nil) != tt.wantRetain {
					t.Errorf("L1 retained=%t, want %t", retained != nil, tt.wantRetain)
				}
			})
		})
	}
}

func TestL2StorageExpiredL1MissAndError(t *testing.T) {
	for _, broken := range []bool{false, true} {
		for _, silent := range []bool{false, true} {
			name := "miss"
			if broken {
				name = "error"
			}
			if silent {
				name += " silent"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				client := newStubValkeyClient()
				client.broken = broken
				m := &metricstest.MockMetrics{}
				l1 := NewLRUStorage(1<<20, nil, metrics.Void)
				s := NewL2Storage(client, l1, m, time.Minute, valkey.IsValkeyNil)
				old := &Entry{
					StatusCode: http.StatusOK, Payload: []byte("retained for errors"),
					TTL: time.Minute, StaleIfError: time.Hour, CreatedAt: time.Now().Add(-2 * time.Minute),
				}
				if err := l1.Set(ctx, "key", old); err != nil {
					t.Fatal(err)
				}
				get := s.Get
				if silent {
					get = s.GetSilent
				}
				got, err := get(ctx, "key")
				if (err != nil) != broken || (got != nil) != broken || client.getCalls != 1 {
					t.Fatalf("Get: entry=%+v, err=%v, L2 gets=%d; want candidate/error only on L2 failure", got, err, client.getCalls)
				}
				retained, err := l1.Get(ctx, "key")
				if err != nil || (retained != nil) != broken || retained != nil && string(retained.Payload) != string(old.Payload) {
					t.Fatalf("L1 retention: entry=%+v, err=%v", retained, err)
				}
				if !broken {
					client.broken = true
					if candidate, err := s.GetSilent(ctx, "key"); candidate != nil || err == nil {
						t.Fatalf("clean miss resurrected local candidate on later error: %+v, %v", candidate, err)
					}
				}
				for _, counter := range []string{"cache.l1_hit", "cache.l2_hit", "cache.l2_miss", "cache.l2_get_error"} {
					var want int64
					if !silent && ((!broken && counter == "cache.l2_miss") || (broken && counter == "cache.l2_get_error")) {
						want = 1
					}
					if got, _ := m.Counter(counter); got != want {
						t.Errorf("%s=%d, want %d", counter, got, want)
					}
				}
			})
		}
	}
}

func TestCacheFilterExpiredL1FreshL2(t *testing.T) {
	for _, onlyIfCached := range []bool{false, true} {
		name := "normal"
		if onlyIfCached {
			name = "only-if-cached"
		}
		t.Run(name, func(t *testing.T) {
			client := newStubValkeyClient()
			m := &metricstest.MockMetrics{}
			spec := NewCacheFilter(Options{MaxBytes: 1 << 20, ListenAddr: "localhost:9090", L2Client: client, L1TTL: time.Minute, IsNoL2Err: valkey.IsValkeyNil, Metrics: m}).(*cacheSpec)
			defer spec.Close()
			filter, err := spec.CreateFilter([]any{"1m", "1s", "1m"})
			if err != nil {
				t.Fatal(err)
			}
			f := filter.(*cacheFilter)
			ctx := newCtx(http.MethodGet, "http://cache.test/resource", "")
			if onlyIfCached {
				ctx.FRequest.Header.Set("Cache-Control", "only-if-cached")
			}
			key := cacheKey(ctx.RouteId(), ctx.Request(), f.keyHeaders)
			old := &Entry{
				StatusCode: http.StatusOK, Payload: []byte("old L1"), Header: make(http.Header),
				TTL: time.Minute, StaleIfError: time.Hour, CreatedAt: time.Now().Add(-2 * time.Minute),
			}
			if err := f.lruStorage.Set(context.Background(), key, old); err != nil {
				t.Fatal(err)
			}
			fresh := &Entry{StatusCode: http.StatusOK, Payload: []byte("fresh L2"), Header: make(http.Header), TTL: time.Hour, CreatedAt: time.Now()}
			remote := NewL2Storage(client, NewLRUStorage(1<<20, nil, metrics.Void), metrics.Void, 0, valkey.IsValkeyNil)
			if err := remote.Set(context.Background(), key, fresh); err != nil {
				t.Fatal(err)
			}
			f.fetch = func(*http.Request) (*http.Response, error) {
				t.Error("fresh L2 response must not contact origin")
				return nil, context.Canceled
			}
			f.Request(ctx)
			if !ctx.FServed || ctx.FResponse == nil {
				t.Fatal("fresh L2 response was not served")
			}
			defer ctx.FResponse.Body.Close()
			if ctx.FResponse.StatusCode != http.StatusOK || ctx.FResponse.Header.Get(cacheStatusHeader) != cacheStatusHit {
				t.Errorf("response: status=%d, cache status=%q; want 200, HIT", ctx.FResponse.StatusCode, ctx.FResponse.Header.Get(cacheStatusHeader))
			}
			body, err := io.ReadAll(ctx.FResponse.Body)
			if err != nil || string(body) != "fresh L2" {
				t.Errorf("body=%q, err=%v; want fresh L2", body, err)
			}
			if got, _ := m.Counter("cache.l1_hit"); got != 0 {
				t.Errorf("cache.l1_hit=%d, want 0", got)
			}
			if got, _ := m.Counter("cache.l2_hit"); got != 1 {
				t.Errorf("cache.l2_hit=%d, want 1", got)
			}
			warmed, err := f.lruStorage.Get(context.Background(), key)
			if err != nil || warmed == nil || string(warmed.Payload) != "fresh L2" {
				t.Fatalf("L1 warming: entry=%+v, err=%v", warmed, err)
			}
		})
	}
}

// Blocking L2 I/O lets a concurrent Set replace or delete the local snapshot.
type blockingFreshnessClient struct {
	*stubValkeyClient
	started chan struct{}
	release chan struct{}
	decode  bool
}

func (c *blockingFreshnessClient) Get(context.Context, string) (string, error) {
	close(c.started)
	<-c.release
	if c.decode {
		return "invalid JSON", nil
	}
	return "", errors.New("L2 transport failure")
}

func TestL2StorageFailureConcurrentLocalUpdate(t *testing.T) {
	for _, decode := range []bool{false, true} {
		for _, update := range []string{"set", "delete"} {
			t.Run(update+map[bool]string{false: "/transport", true: "/decode"}[decode], func(t *testing.T) {
				ctx := context.Background()
				client := &blockingFreshnessClient{stubValkeyClient: newStubValkeyClient(), started: make(chan struct{}), release: make(chan struct{}), decode: decode}
				l1 := NewLRUStorage(1<<20, nil, metrics.Void)
				s := NewL2Storage(client, l1, metrics.Void, time.Second, valkey.IsValkeyNil)
				old := &Entry{StatusCode: 200, Payload: []byte("old"), CreatedAt: time.Now().Add(-2 * time.Minute), TTL: time.Minute, StaleWhileRevalidate: time.Hour, StaleIfError: time.Hour}
				if err := l1.Set(ctx, "key", old); err != nil {
					t.Fatal(err)
				}

				done := make(chan struct{})
				var got *Entry
				var getErr error
				go func() {
					got, getErr = s.Get(ctx, "key")
					close(done)
				}()
				<-client.started
				if update == "delete" {
					if err := l1.Delete(ctx, "key"); err != nil {
						t.Fatal(err)
					}
				} else {
					newer := *old
					newer.Payload, newer.CreatedAt = []byte("newer"), time.Now()
					if err := s.Set(ctx, "key", &newer); err != nil {
						t.Fatal(err)
					}
				}
				close(client.release)
				<-done
				if getErr == nil || update == "delete" && got != nil || update == "set" && (got == nil || string(got.Payload) != "newer") {
					t.Fatalf("concurrent %s lost: candidate=%+v err=%v", update, got, getErr)
				}
			})
		}
	}
}
