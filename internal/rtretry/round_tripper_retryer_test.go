package rtretry_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/rtretry"
)

func TestRoundTripperRetryer(t *testing.T) {
	t.Parallel()

	client := http.Client{
		Transport: rtretry.NewRoundTripperRetryer(http.DefaultTransport),
	}

	t.Run("retry called", func(t *testing.T) {
		t.Parallel()

		attempt := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempt < 3 {
				w.WriteHeader(http.StatusTooManyRequests)
			} else {
				w.WriteHeader(http.StatusOK)
			}
			attempt++
		}))
		defer server.Close()

		reqCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		req, err := rtretry.NewRetryableRequestWithContext(reqCtx, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("respect context deadline", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(1 * time.Second)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		reqCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		req, err := rtretry.NewRetryableRequestWithContext(reqCtx, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("respect Retry-After on StatusTooManyRequests", func(t *testing.T) {
		t.Parallel()

		retryAfter := 2
		attempt := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			defer func() { attempt++ }()
			if attempt == 0 {
				w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		reqCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		startTime := time.Now()
		req, err := rtretry.NewRetryableRequestWithContext(reqCtx, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		took := time.Since(startTime)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.GreaterOrEqual(t, took, time.Duration(retryAfter)*time.Second)
	})

	t.Run("idempotent methods retry without being marked", func(t *testing.T) {
		t.Parallel()

		expectedAttempts := 3
		attempt := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			defer func() { attempt++ }()
			if attempt < expectedAttempts {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		reqCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, server.URL, http.NoBody)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, expectedAttempts+1, attempt)
	})

	t.Run("non-idempotent methods are not retried", func(t *testing.T) {
		t.Parallel()

		attempt := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempt++
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, http.NoBody)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		require.Equal(t, 1, attempt)
	})

	t.Run("max attempts reached", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		c := http.Client{
			Transport: rtretry.NewRoundTripperRetryer(http.DefaultTransport,
				rtretry.WithMaxRetries(2),
				rtretry.WithMinWaitTime(time.Millisecond),
			),
		}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, http.NoBody)
		require.NoError(t, err)
		_, err = c.Do(req) //nolint:bodyclose // no response is returned on error
		require.ErrorIs(t, err, rtretry.ErrMaxAttemptsReached)
	})

	t.Run("retried request body is replayed", func(t *testing.T) {
		t.Parallel()

		var bodies []string
		attempt := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() { attempt++ }()
			buf := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(buf)
			bodies = append(bodies, string(buf))
			if attempt == 0 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		c := http.Client{
			Transport: rtretry.NewRoundTripperRetryer(http.DefaultTransport, rtretry.WithMinWaitTime(time.Millisecond)),
		}
		req, err := rtretry.NewRetryableRequestWithContext(context.Background(), http.MethodPost, server.URL,
			strings.NewReader(`{"hello":"world"}`))
		require.NoError(t, err)
		resp, err := c.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, []string{`{"hello":"world"}`, `{"hello":"world"}`}, bodies)
	})
}
