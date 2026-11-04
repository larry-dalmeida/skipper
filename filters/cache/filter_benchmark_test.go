package cache

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/zalando/skipper/eskip"
	"github.com/zalando/skipper/filters"
	"github.com/zalando/skipper/logging"
	"github.com/zalando/skipper/logging/loggingtest"
	"github.com/zalando/skipper/metrics"
	"github.com/zalando/skipper/proxy"
	"github.com/zalando/skipper/routing"
	"github.com/zalando/skipper/routing/testdataclient"
)

func BenchmarkCacheHit(b *testing.B) {
	for _, size := range []int{0, 4 << 10, 64 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			body := strings.Repeat("x", size)
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Cache-Control", "public, max-age=3600")
				w.Header().Set("Content-Type", "text/plain")
				io.WriteString(w, body)
			}))
			defer backend.Close()

			spec := NewCacheFilter(Options{MaxBytes: 256 << 20})
			defer spec.(*cacheSpec).Close()
			fr := make(filters.Registry)
			fr.Register(spec)
			dc := testdataclient.New([]*eskip.Route{{
				Id:      "cache-hit",
				Filters: []*eskip.Filter{{Name: Name}},
				Backend: backend.URL,
			}})
			defer dc.Close()
			log := loggingtest.New()
			defer log.Close()
			rt := routing.New(routing.Options{FilterRegistry: fr, DataClients: []routing.DataClient{dc}, Log: log})
			defer rt.Close()
			p := proxy.WithParams(proxy.Params{
				Routing:              rt,
				CloseIdleConnsPeriod: -time.Second,
				AccessLogger:         logging.NewAccessLogger(logging.Options{AccessLogOutput: io.Discard}),
			})
			defer p.Close()
			if err := log.WaitFor("route settings applied", time.Second); err != nil {
				b.Fatal(err)
			}

			request := httptest.NewRequest(http.MethodGet, "http://example.org/resource", nil)
			warm := httptest.NewRecorder()
			p.ServeHTTP(warm, request.Clone(context.Background()))
			if warm.Code != http.StatusOK || warm.Body.String() != body {
				b.Fatalf("unexpected warm response: %d, %q", warm.Code, warm.Body.String())
			}

			b.ReportAllocs()
			var response *httptest.ResponseRecorder
			for b.Loop() {
				response = httptest.NewRecorder()
				p.ServeHTTP(response, request.Clone(context.Background()))
			}
			if response.Code != http.StatusOK || response.Body.String() != body || response.Header().Get(cacheStatusHeader) != cacheStatusHit {
				b.Fatalf("unexpected cache response: %d, %v", response.Code, response.Header())
			}
		})
	}
}

func BenchmarkL2StorageFreshLocalRead(b *testing.B) {
	for _, silent := range []bool{false, true} {
		name := "Get"
		if silent {
			name = "GetSilent"
		}
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			client := newStubValkeyClient()
			l1 := NewLRUStorage(1<<20, nil, metrics.Void)
			s := NewL2Storage(client, l1, metrics.Void, time.Hour, valkey.IsValkeyNil)
			entry := &Entry{StatusCode: 200, Payload: []byte("fresh"), Header: make(http.Header), CreatedAt: time.Now(), TTL: time.Hour}
			if err := s.Set(ctx, "key", entry); err != nil {
				b.Fatal(err)
			}
			get := s.Get
			if silent {
				get = s.GetSilent
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if entry, err := get(ctx, "key"); err != nil || entry == nil {
					b.Fatalf("entry=%+v err=%v", entry, err)
				}
			}
		})
	}
}
