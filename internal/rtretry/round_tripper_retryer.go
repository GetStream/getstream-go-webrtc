// Package rtretry provides an http.RoundTripper that retries requests which
// are safe to replay: idempotent methods, plus requests explicitly marked with
// NewRetryableRequestWithContext.
package rtretry

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

const (
	DefaultMinWaitTime = 50 * time.Millisecond
	DefaultMaxAttempts = 5
)

// Logger is the subset of a logger this package needs. logger.ILogger
// satisfies it, so callers can pass their logger directly without this
// package depending on it.
type Logger interface {
	Warnf(format string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Warnf(string, ...any) {}

type retryContextKey struct{}

var ErrMaxAttemptsReached = errors.New("max attempts reached")

// net/http reports these three conditions as untyped errors, so they can only
// be recognised by matching the message.
var (
	redirectsErrorRe  = regexp.MustCompile(`stopped after \d+ redirects\z`)
	schemeErrorRe     = regexp.MustCompile(`unsupported protocol scheme`)
	notTrustedErrorRe = regexp.MustCompile(`certificate is not trusted`)
)

type options struct {
	maxRetries           int
	retryableStatusCodes []int
	minWaitTime          time.Duration
	logger               Logger
	idempotentMethods    []string
}

type Option func(*options)

func defaultOptions() options {
	return options{
		maxRetries: DefaultMaxAttempts,
		retryableStatusCodes: []int{
			http.StatusTooManyRequests,
			http.StatusServiceUnavailable,
			http.StatusInternalServerError,
			http.StatusGatewayTimeout,
			http.StatusLoopDetected,
		},
		logger:            noopLogger{},
		minWaitTime:       DefaultMinWaitTime,
		idempotentMethods: []string{http.MethodGet, http.MethodHead},
	}
}

func WithMinWaitTime(t time.Duration) Option {
	return func(o *options) { o.minWaitTime = t }
}

func WithMaxRetries(n int) Option {
	return func(o *options) { o.maxRetries = n }
}

func WithRetryableStatusCodes(codes ...int) Option {
	return func(o *options) { o.retryableStatusCodes = codes }
}

func WithLogger(l Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

func WithIdempotentMethods(methods ...string) Option {
	return func(o *options) { o.idempotentMethods = methods }
}

// NewRetryableRequestWithContext builds a request that RoundTripperRetryer will
// retry even when its method is not idempotent.
func NewRetryableRequestWithContext(ctx context.Context, method, inputURL string, body io.Reader) (*http.Request, error) {
	return http.NewRequestWithContext(context.WithValue(ctx, retryContextKey{}, struct{}{}), method, inputURL, body)
}

type RoundTripperRetryer struct {
	Rt http.RoundTripper
	options
}

func NewRoundTripperRetryer(rt http.RoundTripper, opts ...Option) *RoundTripperRetryer {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return &RoundTripperRetryer{Rt: rt, options: o}
}

func (rtr *RoundTripperRetryer) isRequestMarkedAsRetryable(req *http.Request) bool {
	for _, method := range rtr.idempotentMethods {
		if req.Method == method {
			return true
		}
	}
	return req.Context().Value(retryContextKey{}) != nil
}

func (rtr *RoundTripperRetryer) RoundTrip(req *http.Request) (*http.Response, error) {
	var (
		resp *http.Response
		err  error
	)
	ctx := req.Context()
	retryable := rtr.isRequestMarkedAsRetryable(req) && canReplayBody(req)

	attempt := 0
	for attempt < rtr.maxRetries {
		resp, err = rtr.Rt.RoundTrip(req)

		shouldRetry, sleep := rtr.shouldRetry(resp, err, retryable, attempt)
		if !shouldRetry {
			return resp, err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if resp != nil && resp.Body != nil {
			// The response is discarded, so the connection has to be drained
			// to be reusable.
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		if rewindErr := rewindBody(req); rewindErr != nil {
			return resp, err
		}
		rtr.logger.Warnf("request failed, retrying: method=%s url=%s attempts=%d", req.Method, req.URL, attempt)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleep):
		}
		attempt++
	}
	rtr.logger.Warnf("request failed, giving up: method=%s url=%s attempts=%d", req.Method, req.URL, attempt)
	return nil, ErrMaxAttemptsReached
}

// canReplayBody reports whether the request body can be sent more than once.
// Without this a retried POST would arrive at the server with an empty body,
// because the first attempt consumed the reader.
func canReplayBody(req *http.Request) bool {
	return req.Body == nil || req.Body == http.NoBody || req.GetBody != nil
}

func rewindBody(req *http.Request) error {
	if req.GetBody == nil {
		return nil
	}
	body, err := req.GetBody()
	if err != nil {
		return err
	}
	req.Body = body
	return nil
}

func isErrRetryable(err error) bool {
	if errors.Is(err, ErrMaxAttemptsReached) {
		return false
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		// Exhausted redirects, a bad scheme and an untrusted certificate are
		// all permanent; replaying them cannot help.
		if redirectsErrorRe.MatchString(urlErr.Error()) ||
			schemeErrorRe.MatchString(urlErr.Error()) ||
			notTrustedErrorRe.MatchString(urlErr.Error()) {
			return false
		}
		var unknownAuthority x509.UnknownAuthorityError
		if errors.As(urlErr.Err, &unknownAuthority) {
			return false
		}
	}

	return true
}

func (rtr *RoundTripperRetryer) shouldRetry(resp *http.Response, err error, wasReqRetryable bool, attempt int) (bool, time.Duration) {
	if !wasReqRetryable {
		return false, 0
	}

	if err != nil {
		if isErrRetryable(err) {
			return true, rtr.getWaitTime(attempt)
		}
		return false, 0
	}

	if resp == nil {
		return false, 0
	}

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusBadRequest {
		return false, 0
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		sleepTime := rtr.getWaitTime(attempt)
		if s := resp.Header.Get("Retry-After"); s != "" {
			if sleep, parseErr := strconv.ParseInt(s, 10, 64); parseErr == nil {
				sleepTime = time.Second * time.Duration(sleep)
			}
		}
		return true, sleepTime
	}
	if rtr.isStatusCodeRetryable(resp.StatusCode) {
		return true, rtr.getWaitTime(attempt)
	}
	return false, 0
}

func (rtr *RoundTripperRetryer) getWaitTime(attempt int) time.Duration {
	return rtr.minWaitTime << attempt
}

func (rtr *RoundTripperRetryer) isStatusCodeRetryable(statusCode int) bool {
	for _, c := range rtr.retryableStatusCodes {
		if c == statusCode {
			return true
		}
	}
	return false
}
