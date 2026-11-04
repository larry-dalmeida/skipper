package cache

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/zalando/skipper/metrics/metricstest"
	skpnet "github.com/zalando/skipper/net"
	"github.com/zalando/skipper/net/valkeytest"
)

func TestValkeyStorage_FreshnessAndLocalSIE(t *testing.T) {
	addr, stop := valkeytest.NewTestValkey(t)
	done := sync.OnceFunc(stop)
	defer done()
	ring, err := skpnet.NewValkeyRingClient(&skpnet.ValkeyOptions{
		Addrs: []string{addr}, ConnWriteTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()
	ctx := context.Background()
	m := &metricstest.MockMetrics{}
	l1 := NewLRUStorage(1<<20, nil, m)
	s := NewL2Storage(ring, l1, m, 10*time.Millisecond, valkey.IsValkeyNil)
	remote := NewL2Storage(ring, NewLRUStorage(1<<20, nil, m), m, 0, valkey.IsValkeyNil)
	entry := &Entry{
		StatusCode: 200, Payload: []byte("original"), Header: http.Header{"Etag": {`"original"`}},
		CreatedAt: time.Now(), ResponseTime: time.Now(), TTL: 2 * time.Second, StaleIfError: time.Minute,
	}
	waitForLocalDeadline := func() {
		t.Helper()
		local, err := l1.Get(ctx, "key")
		if err != nil || local == nil {
			t.Fatalf("local entry=%+v err=%v", local, err)
		}
		if local.L1FreshUntil.IsZero() {
			t.Fatal("missing local warming deadline")
		}
		time.Sleep(time.Until(local.L1FreshUntil))
	}
	if err := s.Set(ctx, "key", entry); err != nil {
		t.Fatal(err)
	}
	newer := *entry
	newer.Payload, newer.CreatedAt = []byte("fresh shared"), time.Now()
	if err := remote.Set(ctx, "key", &newer); err != nil {
		t.Fatal(err)
	}
	waitForLocalDeadline()
	got, err := s.Get(ctx, "key")
	if err != nil || got == nil || string(got.Payload) != "fresh shared" {
		t.Fatalf("fresh L2 precedence: entry=%+v err=%v", got, err)
	}
	local, err := l1.Get(ctx, "key")
	if err != nil || local == nil {
		t.Fatalf("read warming: entry=%+v err=%v", local, err)
	}
	if err := s.Set(ctx, "copy", local); err != nil {
		t.Fatal(err)
	}
	raw, err := ring.Get(ctx, "copy")
	if err != nil {
		t.Fatal(err)
	}
	var shared Entry
	if err := json.Unmarshal([]byte(raw), &shared); err != nil || !shared.L1FreshUntil.IsZero() ||
		!shared.CreatedAt.Equal(newer.CreatedAt) || shared.TTL != newer.TTL {
		t.Fatalf("shared metadata changed: %+v err=%v", shared, err)
	}
	if strings.Contains(raw, `"L1FreshUntil"`) {
		t.Fatal("shared Valkey JSON contains process-local warming metadata")
	}
	if err := ring.SetWithExpire(ctx, "key", "invalid JSON", time.Minute); err != nil {
		t.Fatal(err)
	}
	waitForLocalDeadline()
	got, err = s.GetSilent(ctx, "key")
	if err == nil || got == nil || string(got.Payload) != "fresh shared" {
		t.Fatalf("decode failure candidate/error: entry=%+v err=%v", got, err)
	}
	if _, err := ring.Del(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get(ctx, "key")
	if err != nil || got != nil {
		t.Fatalf("clean miss: entry=%+v err=%v", got, err)
	}
	if local, err := l1.Get(ctx, "key"); err != nil || local != nil {
		t.Fatalf("clean miss retained local seed: entry=%+v err=%v", local, err)
	}
	entry.CreatedAt, entry.ResponseTime = time.Now(), time.Now()
	entry.TTL, entry.StaleWhileRevalidate = 100*time.Millisecond, 10*time.Millisecond
	if err := s.Set(ctx, "key", entry); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(entry.CreatedAt.Add(entry.TTL + entry.StaleWhileRevalidate)))
	done()
	got, err = s.Get(ctx, "key")
	if err == nil || got == nil || string(got.Payload) != "original" ||
		!got.CreatedAt.Equal(entry.CreatedAt) || got.TTL != entry.TTL || got.StaleIfError != entry.StaleIfError {
		t.Fatalf("real L2 outage lost original SIE candidate: entry=%+v err=%v", got, err)
	}
	if n, _ := m.Counter("cache.l1_hit"); n != 0 {
		t.Fatalf("candidate counted as fresh local hit: %d", n)
	}
}
