package jointrace

import (
	"sort"
	"sync"
	"time"
)

// Recorder collects the spans of one join. It is safe for concurrent use, and every
// method is a no-op on a nil Recorder, so instrumented code need not check.
//
// The first recording of a span wins: a retry or a later renegotiation does not move a
// step that already happened. Seal stops all recording.
type Recorder struct {
	mu     sync.Mutex
	origin time.Time
	spans  map[string]*Span
	order  []string
	rtt    map[Peer]time.Duration
	sealed bool
}

// NewRecorder returns a recorder whose trace starts at origin, the moment the join began.
func NewRecorder(origin time.Time) *Recorder {
	return &Recorder{
		origin: origin,
		spans:  make(map[string]*Span, 48),
		rtt:    make(map[Peer]time.Duration, 4),
	}
}

// Add records a span unless one with the same name exists. A span with a zero Start or
// End is ignored.
func (r *Recorder) Add(s Span) {
	if r == nil || s.Start.IsZero() || s.End.IsZero() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addLocked(s)
}

func (r *Recorder) addLocked(s Span) {
	if r.sealed {
		return
	}
	if _, ok := r.spans[s.Name]; ok {
		return
	}
	if s.End.Before(s.Start) {
		s.End = s.Start
	}
	r.spans[s.Name] = &s
	r.order = append(r.order, s.Name)
}

// Extend records s, or, if a span of that name exists, moves its end to s.End when that
// is later. It is for steps made of several operations, such as a series of trickled
// candidates.
func (r *Recorder) Extend(s Span) {
	if r == nil || s.Start.IsZero() || s.End.IsZero() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return
	}
	if existing, ok := r.spans[s.Name]; ok {
		if s.End.After(existing.End) {
			existing.End = s.End
		}
		return
	}
	r.addLocked(s)
}

// Remove drops the named spans and their detail spans, for steps that are redone from
// scratch and must be recorded again.
func (r *Recorder) Remove(names ...string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return
	}
	drop := make(map[string]bool, len(names))
	for _, name := range names {
		drop[name] = true
	}
	kept := r.order[:0]
	for _, name := range r.order {
		if s := r.spans[name]; drop[name] || drop[s.Parent] {
			delete(r.spans, name)
			continue
		}
		kept = append(kept, name)
	}
	r.order = kept
}

// Scratch returns an empty recorder with r's origin, for an attempt whose spans belong
// in r only if it works out: Merge them then. It is nil when r is.
func (r *Recorder) Scratch() *Recorder {
	if r == nil {
		return nil
	}
	return NewRecorder(r.origin)
}

// Merge records o's spans and round-trip times into r, as Add and SetRTT would.
func (r *Recorder) Merge(o *Recorder) {
	if r == nil || o == nil || r == o {
		return
	}
	t := o.Trace()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range t.Spans {
		r.addLocked(s)
	}
	if r.sealed {
		return
	}
	for p, d := range t.RTT {
		if _, ok := r.rtt[p]; !ok {
			r.rtt[p] = d
		}
	}
}

// Has reports whether a span of that name was recorded.
func (r *Recorder) Has(name string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.spans[name]
	return ok
}

// Get returns the span of that name.
func (r *Recorder) Get(name string) (Span, bool) {
	if r == nil {
		return Span{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.spans[name]
	if !ok {
		return Span{}, false
	}
	return *s, true
}

// SetRTT records a round-trip time to peer, unless one is already known.
func (r *Recorder) SetRTT(peer Peer, rtt time.Duration) {
	if r == nil || rtt <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return
	}
	if _, ok := r.rtt[peer]; !ok {
		r.rtt[peer] = rtt
	}
}

// ReplaceRTT records rtt as the round-trip time to peer, over any recorded before: for
// a better measurement than the first one.
func (r *Recorder) ReplaceRTT(peer Peer, rtt time.Duration) {
	if r == nil || rtt <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.sealed {
		r.rtt[peer] = rtt
	}
}

// RTT is the round-trip time recorded for peer, or zero.
func (r *Recorder) RTT(peer Peer) time.Duration {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rtt[peer]
}

// Seal stops recording: everything after is ignored.
func (r *Recorder) Seal() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sealed = true
}

// Trace returns what has been recorded so far, spans ordered by start.
func (r *Recorder) Trace() Trace {
	if r == nil {
		return Trace{}
	}
	r.mu.Lock()
	t := Trace{
		JoinAt: r.origin,
		Spans:  make([]Span, 0, len(r.order)),
		RTT:    make(map[Peer]time.Duration, len(r.rtt)),
	}
	for _, name := range r.order {
		s := *r.spans[name]
		s.After = append([]string(nil), s.After...)
		t.Spans = append(t.Spans, s)
	}
	for p, d := range r.rtt {
		t.RTT[p] = d
	}
	r.mu.Unlock()

	sort.SliceStable(t.Spans, func(i, j int) bool { return t.Spans[i].Start.Before(t.Spans[j].Start) })
	t.Origin = t.JoinAt
	for _, s := range t.Spans {
		if t.Origin.IsZero() || s.Start.Before(t.Origin) {
			t.Origin = s.Start
		}
	}
	return t
}
