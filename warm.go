package rtc

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/GetStream/getstream-go-webrtc/internal/netdelay"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

const (
	// keepWarmInterval is how often an idle connection gets a request. It is half the
	// shortest idle timeout on the way: the Stream edge's envoy closes a connection that
	// has carried no request for 4 minutes (the Google load balancer in front of it, 350 s;
	// an SFU's envoy, 1 hour). HTTP/2 pings do not count as requests there.
	keepWarmInterval = 2 * time.Minute
	// idleConnTimeout outlives a keepWarmInterval, so a connection kept warm is never
	// closed by the client, and one that is not closes soon after.
	idleConnTimeout = 5 * time.Minute
	// warmSFUFor is how long after a join its SFU edges are kept warm.
	warmSFUFor = 30 * time.Minute
	// maxWarmSFUs bounds the SFU edges kept warm at once.
	maxWarmSFUs = 8
	// warmTimeout bounds one warming request.
	warmTimeout = 5 * time.Second
)

// newHTTPTransport is the transport a Client's coordinator requests, FastJoins and SFU
// RPCs share: HTTP/2 wherever the server offers it, TLS sessions resumed, idle
// connections kept for idleConnTimeout and pinged, so a dead one is noticed before a
// join sends on it.
func newHTTPTransport(rtt time.Duration) *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if rtt > 0 {
		tr.DialContext = netdelay.Dialer(rtt, nil)
	}
	tr.ForceAttemptHTTP2 = true
	tr.TLSClientConfig = &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ClientSessionCache: tls.NewLRUClientSessionCache(0),
	}
	tr.IdleConnTimeout = idleConnTimeout
	tr.HTTP2 = &http.HTTP2Config{SendPingTimeout: 30 * time.Second, PingTimeout: 15 * time.Second}
	return tr
}

// warmer keeps a Client's connections open between joins: the coordinator's from
// NewClient on, and those to the SFU edges of its recent joins.
type warmer struct {
	coordinator func(context.Context) error
	http        *http.Client
	interval    time.Duration
	logger      logger.ILogger
	// onCoordinatorRTT gets the round trip a fresh coordinator connection measured.
	onCoordinatorRTT func(time.Duration)

	mu sync.Mutex
	// sfus is when a join last used each SFU edge, by origin.
	sfus map[string]time.Time
	// closed stops preconnect from adding to running once close waits for it.
	closed bool

	// connected is closed once the first warm is over, with its error in connectErr.
	connected  chan struct{}
	connectErr error

	// ctx ends at close, which waits for running to finish.
	ctx     context.Context
	stop    context.CancelFunc
	running sync.WaitGroup
}

func newWarmer(tr http.RoundTripper, coordinator func(context.Context) error, interval time.Duration, l logger.ILogger) *warmer {
	ctx, stop := context.WithCancel(context.Background())
	return &warmer{
		coordinator: coordinator,
		http:        &http.Client{Transport: tr, Timeout: warmTimeout},
		interval:    interval,
		logger:      l,
		sfus:        map[string]time.Time{},
		connected:   make(chan struct{}),
		ctx:         ctx,
		stop:        stop,
	}
}

// start warms the connections now, and again every interval until close.
func (w *warmer) start() {
	w.running.Go(func() {
		w.connectErr = w.warm(w.ctx)
		close(w.connected)
		if w.connectErr != nil {
			w.logger.Debugf("connecting: %v", w.connectErr)
		}
		tick := time.NewTicker(w.interval)
		defer tick.Stop()
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-tick.C:
			}
			if err := w.warm(w.ctx); err != nil && w.ctx.Err() == nil {
				w.logger.Debugf("keeping connections warm: %v", err)
			}
		}
	})
}

// connect opens connections to the SFU edges at rawURLs, which stay warm from then on,
// and waits for the first warm, which opened the coordinator's.
func (w *warmer) connect(ctx context.Context, rawURLs []string) error {
	var origins []string
	for _, raw := range rawURLs {
		if origin := originOf(raw); !slices.Contains(origins, origin) {
			origins = append(origins, origin)
		}
	}
	errs := make([]error, len(origins)+1)
	var wg sync.WaitGroup
	for i, origin := range origins {
		wg.Go(func() { errs[i+1] = w.get(ctx, origin+"/") })
	}
	select {
	case <-w.connected:
		errs[0] = w.connectErr
	case <-ctx.Done():
		errs[0] = ctx.Err()
	}
	wg.Wait()
	w.remember(rawURLs...)
	return errors.Join(errs...)
}

// preconnect opens connections to the SFU edges at origins in the background.
func (w *warmer) preconnect(origins []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	for _, origin := range origins {
		w.running.Go(func() {
			if err := w.get(w.ctx, origin+"/"); err != nil && w.ctx.Err() == nil {
				w.logger.Debugf("preconnecting to %s: %v", origin, err)
			}
		})
	}
}

func (w *warmer) close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.stop()
	w.running.Wait()
}

// remember keeps the SFU edges serving rawURLs warm for warmSFUFor.
func (w *warmer) remember(rawURLs ...string) {
	now := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, raw := range rawURLs {
		if origin := originOf(raw); origin != "" {
			w.sfus[origin] = now
		}
	}
	for len(w.sfus) > maxWarmSFUs {
		oldest := ""
		for origin, at := range w.sfus {
			if oldest == "" || at.Before(w.sfus[oldest]) {
				oldest = origin
			}
		}
		delete(w.sfus, oldest)
	}
}

// warm sends one cheap request to the coordinator and to every SFU edge still to be
// kept warm, all at once, and waits for them.
func (w *warmer) warm(ctx context.Context) error {
	w.mu.Lock()
	origins := make([]string, 0, len(w.sfus))
	for origin, at := range w.sfus {
		if time.Since(at) > warmSFUFor {
			delete(w.sfus, origin)
			continue
		}
		origins = append(origins, origin)
	}
	w.mu.Unlock()

	errs := make([]error, len(origins)+1)
	var wg sync.WaitGroup
	if w.coordinator != nil {
		wg.Go(func() {
			rec := jointrace.NewRecorder(time.Now())
			errs[0] = w.coordinator(jointrace.WithStep(ctx, rec, "coord.warm", jointrace.PeerCoordinator))
			if rtt := rec.RTT(jointrace.PeerCoordinator); rtt > 0 && w.onCoordinatorRTT != nil {
				w.onCoordinatorRTT(rtt)
			}
		})
	}
	for i, origin := range origins {
		wg.Go(func() { errs[i+1] = w.get(ctx, origin+"/") })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// get requests rawURL and discards the answer, whatever it is: an SFU answers its root
// with a 404 without doing anything.
func (w *warmer) get(ctx context.Context, rawURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	resp, err := w.http.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

// originOf is rawURL's scheme and host, or "" if it has none.
func originOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

// keepSFUsWarm keeps the SFU edges a join was offered warm for warmSFUFor, and opens
// connections to the ones it did not use, off the join's path: the next call may land
// on any of them.
func (c *Client) keepSFUsWarm(rawURLs []string, used string) {
	if c.warm == nil {
		return
	}
	c.warm.remember(rawURLs...)
	var others []string
	for _, raw := range rawURLs {
		if origin := originOf(raw); origin != "" && origin != originOf(used) && !slices.Contains(others, origin) {
			others = append(others, origin)
		}
	}
	c.warm.preconnect(others)
}

// Preconnect opens the connections a join will use: to the SFU edges at sfuURLs (an
// SFU's URL, or any URL on its host), which stay warm from then on, and to the
// coordinator, which NewClient started opening. It returns once every one has answered
// or failed. An agent calls it before its first call, with the SFUs of its region, so
// that call starts warm too.
//
// It does nothing on a client built WithoutKeepWarm.
func (c *Client) Preconnect(ctx context.Context, sfuURLs ...string) error {
	if c.warm == nil {
		return nil
	}
	return c.warm.connect(ctx, sfuURLs)
}

// WithoutKeepWarm stops the client from opening its coordinator connection in NewClient
// and from keeping its connections open between joins with a cheap request every two
// minutes: each join then reuses only what is still open.
func WithoutKeepWarm() Option {
	return func(o *options) {
		o.keepWarm = false
	}
}
