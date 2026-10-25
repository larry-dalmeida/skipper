package metrics_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zalando/skipper/metrics"
)

func TestPrometheusCacheFamilyResult(t *testing.T) {
	pm := metrics.NewPrometheus(metrics.Options{})
	const path = "/cache-family-result"
	mux := http.NewServeMux()
	pm.RegisterHandler(path, mux)

	pm.IncCacheFamilyResult("product", "hit")
	pm.IncCacheFamilyResult("product", "miss")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	out := string(body)

	assert.Contains(t, out, `skipper_cache_family_result_total{family="product",result="hit"} 1`)
	assert.Contains(t, out, `skipper_cache_family_result_total{family="product",result="miss"} 1`)
}

func TestPrometheusCacheFirstHitSeconds(t *testing.T) {
	pm := metrics.NewPrometheus(metrics.Options{})
	const path = "/cache-first-hit"
	mux := http.NewServeMux()
	pm.RegisterHandler(path, mux)

	pm.IncCacheFamilyResult("product", "miss")
	pm.IncCacheFamilyResult("product", "hit")
	time.Sleep(10 * time.Millisecond)
	pm.IncCacheFamilyResult("product", "hit")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	assert.Regexp(t, `skipper_cache_first_hit_seconds [0-9.e+-]+\n`, string(body))
}
