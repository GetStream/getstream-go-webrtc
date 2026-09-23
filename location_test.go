package rtc_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rtc "github.com/GetStream/getstream-go-webrtc"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

type MockHTTPClient struct {
	DoFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.DoFunc(req)
}

func TestCloudFrontDiscoveryHappy(t *testing.T) {
	t.Parallel()

	mockClient := &MockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{rtc.HeaderCloudFrontPop: []string{"DFW3-C3"}},
				Body:       http.NoBody,
			}, nil
		},
	}

	cfd := rtc.NewCloudFrontDiscovery("http://example.com", 3, mockClient, logger.Noop{})
	result := cfd.Discover(context.Background())

	assert.Equal(t, "DFW", result)
}

func TestCloudFrontDiscoveryRetriesFallback(t *testing.T) {
	t.Parallel()

	var calls int

	mockClient := &MockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{
				StatusCode: http.StatusBadGateway,
				Header:     http.Header{rtc.HeaderCloudFrontPop: []string{"DFW3-C3"}},
				Body:       http.NoBody,
			}, nil
		},
	}

	cfd := rtc.NewCloudFrontDiscovery("http://example.com", 3, mockClient, logger.Noop{})
	result := cfd.Discover(context.Background())

	assert.Equal(t, 3, calls)
	assert.Equal(t, rtc.FallbackLocationName, result)
}

func TestCloudFrontDiscoveryRetriesOnErr(t *testing.T) {
	t.Parallel()

	var calls int

	mockClient := &MockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("oh no")
		},
	}

	cfd := rtc.NewCloudFrontDiscovery("http://example.com", 3, mockClient, logger.Noop{})
	result := cfd.Discover(context.Background())

	assert.Equal(t, 3, calls)
	assert.Equal(t, rtc.FallbackLocationName, result)
}

func TestCloudFrontDiscoveryRetriesWorks(t *testing.T) {
	t.Parallel()

	var calls int

	mockClient := &MockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			defer func() { calls++ }()
			if calls == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{rtc.HeaderCloudFrontPop: []string{"DFW3-C3"}},
					Body:       http.NoBody,
				}, nil
			}
			return nil, errors.New("oh no")
		},
	}

	cfd := rtc.NewCloudFrontDiscovery("http://example.com", 3, mockClient, logger.Noop{})
	result := cfd.Discover(context.Background())

	assert.Equal(t, 2, calls)
	assert.Equal(t, "DFW", result)
}

func TestCloudFrontDiscoveryBadResponse(t *testing.T) {
	t.Parallel()

	var calls int

	mockClient := &MockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			defer func() { calls++ }()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{rtc.HeaderCloudFrontPop: []string{""}},
				Body:       http.NoBody,
			}, nil
		},
	}

	cfd := rtc.NewCloudFrontDiscovery("http://example.com", 3, mockClient, logger.Noop{})
	result := cfd.Discover(context.Background())

	assert.Equal(t, 1, calls)
	assert.Equal(t, rtc.FallbackLocationName, result)
}

// TestCloudFrontDiscoveryOverHTTP covers what the upstream integration test
// covered - that the discovery works end to end over a real HTTP client - but
// against a local server instead of hint.stream-io-video.com, so it does not
// need network access.
func TestCloudFrontDiscoveryOverHTTP(t *testing.T) {
	t.Parallel()

	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Header().Set(rtc.HeaderCloudFrontPop, "AMS53-P4")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfd := rtc.NewCloudFrontDiscovery(srv.URL+"/", 3, srv.Client(), logger.Noop{})
	require.Equal(t, "AMS", cfd.Discover(context.Background()))

	assert.Equal(t, http.MethodHead, gotMethod, "location discovery must not download a body")
	assert.Equal(t, "/", gotPath)
}
