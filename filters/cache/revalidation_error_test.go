package cache

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zalando/skipper/metrics/metricstest"
)

func TestCacheFilter_RevalidationErrorAccounting(t *testing.T) {
	for _, tt := range []struct {
		name        string
		status      int
		sie         bool
		rfc         bool
		noStore     bool
		readError   bool
		fetchError  error
		wantErrors  int64
		wantStored  int
		wantUpdated bool
	}{
		{name: "success", status: 200, wantStored: 200, wantUpdated: true},
		{name: "not modified", status: 304, wantStored: 200, wantUpdated: true},
		{name: "SIE 500", status: 500, sie: true, wantErrors: 1, wantStored: 200},
		{name: "SIE 502", status: 502, sie: true, wantErrors: 1, wantStored: 200},
		{name: "SIE 503", status: 503, sie: true, wantErrors: 1, wantStored: 200},
		{name: "SIE 504", status: 504, sie: true, wantErrors: 1, wantStored: 200},
		{name: "negative cached 503", status: 503, wantErrors: 1, wantStored: 503, wantUpdated: true},
		{name: "501 outside fallback", status: 501, sie: true, wantErrors: 1, wantStored: 501, wantUpdated: true},
		{name: "505 outside fallback", status: 505, sie: true, wantErrors: 1, wantStored: 505, wantUpdated: true},
		{name: "599 outside fallback", status: 599, sie: true, wantErrors: 1, wantStored: 599, wantUpdated: true},
		{name: "non-cacheable 503", status: 503, rfc: true, noStore: true, wantErrors: 1, wantStored: 200},
		{name: "transport error", fetchError: errors.New("origin unavailable"), wantErrors: 1, wantStored: 200},
		{name: "timeout", fetchError: context.DeadlineExceeded, wantErrors: 1, wantStored: 200},
		{name: "body error", status: 200, readError: true, wantErrors: 1, wantStored: 200},
		{name: "503 body error counted once", status: 503, readError: true, wantErrors: 1, wantStored: 200},
		{name: "SIE 503 body error counted once", status: 503, sie: true, readError: true, wantErrors: 1, wantStored: 200},
		{name: "501 body error counted once", status: 501, sie: true, readError: true, wantErrors: 1, wantStored: 200},
		{name: "404 not an origin failure", status: 404, wantStored: 404, wantUpdated: true},
		{name: "600 not a 5xx", status: 600, wantStored: 200},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := &metricstest.MockMetrics{}
			spec := NewCacheFilter(Options{MaxBytes: 1 << 20, Metrics: m}).(*cacheSpec)
			defer spec.Close()
			args := []any{"1m", "10s", "1m"}
			if tt.sie {
				args = append(args, "1m")
			}
			if tt.rfc {
				args = nil
			}
			filter, err := spec.CreateFilter(args)
			if err != nil {
				t.Fatal(err)
			}
			f := filter.(*cacheFilter)
			stored := &Entry{
				StatusCode: 200, Header: http.Header{"Cache-Control": {"max-age=60"}},
				Payload: []byte("last-good"), CreatedAt: time.Now().Add(-90 * time.Second),
				TTL: time.Minute, StaleWhileRevalidate: time.Minute, ETag: `"original"`,
			}
			if err := f.storage.Set(context.Background(), "key", stored); err != nil {
				t.Fatal(err)
			}
			before, err := f.storage.GetSilent(context.Background(), "key")
			if err != nil || before == nil {
				t.Fatalf("seed absent: %v", err)
			}
			var fetches int
			failedBody := &bypassCountingBody{ReadCloser: &bypassResponseBody{reader: strings.NewReader("partial"), err: io.ErrUnexpectedEOF}}
			f.fetch = func(req *http.Request) (*http.Response, error) {
				fetches++
				if req.Header.Get("If-None-Match") != `"original"` {
					t.Error("revalidation lost conditional validator")
				}
				if tt.fetchError != nil {
					return nil, tt.fetchError
				}
				resp := upstreamResponse(tt.status, "fresh")
				if tt.noStore {
					resp.Header.Set("Cache-Control", "no-store")
				}
				if tt.readError {
					resp.Body = failedBody
				}
				return resp, nil
			}
			req := httptest.NewRequest(http.MethodGet, "http://origin.example/retention", nil)
			f.revalidate("key", "key", req, snapshotCacheIdentity(req), "http://origin.example", "")
			if fetches != 1 {
				t.Errorf("origin calls=%d, want 1", fetches)
			}
			if got, _ := m.Counter("cache.reval_error"); got != tt.wantErrors {
				t.Errorf("revalidation errors=%d, want %d", got, tt.wantErrors)
			}
			if got, _ := m.Counter("cache.reval_dropped"); got != 0 {
				t.Errorf("origin failure counted as %d queue drops", got)
			}
			if tt.readError && failedBody.closes.Load() != 1 {
				t.Errorf("failed stream closes=%d, want 1", failedBody.closes.Load())
			}
			after, err := f.storage.GetSilent(context.Background(), "key")
			if err != nil || after == nil || after.StatusCode != tt.wantStored {
				t.Fatalf("stored=%+v err=%v, want status %d", after, err, tt.wantStored)
			}
			if tt.wantUpdated {
				if !after.CreatedAt.After(before.CreatedAt) {
					t.Error("successful or normally negative-cached response did not update freshness")
				}
				if tt.status != 304 && string(after.Payload) != "fresh" {
					t.Errorf("updated payload=%q", after.Payload)
				}
				if tt.wantStored >= 400 && after.TTL != 10*time.Second {
					t.Errorf("negative cache TTL=%v, want 10s", after.TTL)
				}
			} else if !reflect.DeepEqual(before, after) {
				t.Fatalf("failed refresh changed last-good response: before=%+v after=%+v", before, after)
			}
		})
	}
}
