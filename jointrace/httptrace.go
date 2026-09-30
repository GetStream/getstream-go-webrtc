package jointrace

import (
	"context"
	"crypto/tls"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"time"
)

type stepKey struct{}

// step is the network step a context's requests are part of.
type step struct {
	rec  *Recorder
	name string
	peer Peer

	mu        sync.Mutex
	dnsStart  time.Time
	connStart time.Time
	tlsStart  time.Time
	connReady time.Time
	wroteAt   time.Time
	firstByte time.Time
	reused    bool
}

// WithStep returns a context whose HTTP requests and websocket dials record their DNS,
// TCP, TLS, request and first-byte times as detail spans of the step called name, and
// whose fresh TCP connections give rec a round-trip time to peer.
func WithStep(ctx context.Context, rec *Recorder, name string, peer Peer) context.Context {
	if rec == nil {
		return ctx
	}
	s := &step{rec: rec, name: name, peer: peer}
	ctx = context.WithValue(ctx, stepKey{}, s)
	return httptrace.WithClientTrace(ctx, s.clientTrace())
}

// Reused reports whether the step's request went out on a connection that was already
// open, so it paid no TCP or TLS round trips.
func Reused(ctx context.Context) bool {
	s, _ := ctx.Value(stepKey{}).(*step)
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reused
}

// FirstByte is when the step's first response byte arrived, or zero.
func FirstByte(ctx context.Context) time.Time {
	s, _ := ctx.Value(stepKey{}).(*step)
	if s == nil {
		return time.Time{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstByte
}

func (s *step) detail(suffix string, start, end time.Time, kind Kind, note string) {
	s.rec.Add(Span{
		Name: s.name + suffix, Parent: s.name, Start: start, End: end,
		Kind: kind, Peer: s.peer, Note: note,
	})
}

func (s *step) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) {
			s.mu.Lock()
			s.dnsStart = time.Now()
			s.mu.Unlock()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			s.mu.Lock()
			start := s.dnsStart
			s.mu.Unlock()
			s.detail(DetailDNS, start, time.Now(), KindNet, "")
		},
		ConnectStart: func(string, string) {
			s.mu.Lock()
			if s.connStart.IsZero() {
				s.connStart = time.Now()
			}
			s.mu.Unlock()
		},
		ConnectDone: func(_, _ string, err error) {
			if err != nil {
				return
			}
			now := time.Now()
			s.mu.Lock()
			start, abandoned := s.connStart, !s.connReady.IsZero()
			s.mu.Unlock()
			s.rec.SetRTT(s.peer, now.Sub(start))
			// net/http finishes a dial it no longer needs when an idle connection freed up
			// first; the request did not wait for it.
			if !abandoned {
				s.detail(DetailTCP, start, now, KindNet, "")
			}
		},
		TLSHandshakeStart: func() {
			s.mu.Lock()
			s.tlsStart = time.Now()
			s.mu.Unlock()
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			if err != nil {
				return
			}
			s.mu.Lock()
			start, abandoned := s.tlsStart, !s.connReady.IsZero()
			s.mu.Unlock()
			if abandoned {
				return
			}
			note := ""
			if state.DidResume {
				note = "resumed"
			}
			s.detail(DetailTLS, start, time.Now(), KindNet, note)
		},
		GotConn: func(info httptrace.GotConnInfo) {
			s.mu.Lock()
			if s.connReady.IsZero() {
				s.connReady = time.Now()
				s.reused = info.Reused
			}
			s.mu.Unlock()
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err != nil {
				return
			}
			now := time.Now()
			s.mu.Lock()
			start := s.connReady
			if s.wroteAt.IsZero() {
				s.wroteAt = now
			}
			reused := s.reused
			s.mu.Unlock()
			note := ""
			if reused {
				note = "reused connection"
			}
			s.detail(DetailRequest, start, now, KindLocal, note)
		},
		GotFirstResponseByte: func() {
			now := time.Now()
			s.mu.Lock()
			start := s.wroteAt
			if s.firstByte.IsZero() {
				s.firstByte = now
			}
			s.mu.Unlock()
			s.detail(DetailFirstByte, start, now, KindNet, "")
		},
	}
}

// ServerTiming records the server's own time for the context's step from a
// Server-Timing response header, as a detail span centred in the wait for the first
// byte. It uses the "total" metric when the header has one, otherwise the longest.
func ServerTiming(ctx context.Context, header string) {
	if header == "" {
		return
	}
	dur, ok := parseServerTiming(header)
	if !ok {
		return
	}
	ServerDuration(ctx, dur, "server-timing: "+header)
}

// ServerDuration records dur, the server's own time for the context's step as the
// server reported it in the response body, as ServerTiming does from a header.
func ServerDuration(ctx context.Context, dur time.Duration, note string) {
	s, _ := ctx.Value(stepKey{}).(*step)
	if s == nil || dur <= 0 {
		return
	}
	s.mu.Lock()
	wrote, first := s.wroteAt, s.firstByte
	s.mu.Unlock()
	if wrote.IsZero() || first.IsZero() {
		return
	}
	wait := first.Sub(wrote)
	if dur > wait {
		dur = wait
	}
	start := wrote.Add((wait - dur) / 2)
	s.detail(DetailServer, start, start.Add(dur), KindLocal, note)
}

func parseServerTiming(header string) (time.Duration, bool) {
	var longest, total time.Duration
	haveTotal, found := false, false
	for _, metric := range strings.Split(header, ",") {
		fields := strings.Split(metric, ";")
		name := strings.TrimSpace(fields[0])
		for _, param := range fields[1:] {
			key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
			if !ok || !strings.EqualFold(key, "dur") {
				continue
			}
			v, err := strconv.ParseFloat(strings.Trim(value, `"`), 64)
			if err != nil || v < 0 {
				continue
			}
			d := time.Duration(v * float64(time.Millisecond))
			found = true
			if strings.EqualFold(name, "total") {
				total, haveTotal = d, true
			}
			if d > longest {
				longest = d
			}
		}
	}
	if haveTotal {
		return total, true
	}
	return longest, found
}
