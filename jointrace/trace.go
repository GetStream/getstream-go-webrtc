package jointrace

import (
	"encoding/json"
	"time"
)

// Trace is the recorded join.
type Trace struct {
	// JoinAt is when Call.Join was called.
	JoinAt time.Time
	// Origin is when the first recorded step started: JoinAt, or earlier when the trace
	// includes the client's own connection to the coordinator.
	Origin time.Time
	// Spans are ordered by start.
	Spans []Span
	// RTT is the measured round-trip time to each peer.
	RTT map[Peer]time.Duration
}

// Span returns the span of that name.
func (t Trace) Span(name string) (Span, bool) {
	for _, s := range t.Spans {
		if s.Name == name {
			return s, true
		}
	}
	return Span{}, false
}

// RTTOf is the round-trip time spans to peer are counted in. The media path falls back to
// the SFU's signalling RTT when ICE has not measured one.
func (t Trace) RTTOf(peer Peer) time.Duration {
	if d := t.RTT[peer]; d > 0 {
		return d
	}
	if peer == PeerUDP {
		return t.RTT[PeerSFU]
	}
	return 0
}

// RTTs is d in round trips to peer, or zero when that RTT is unknown.
func (t Trace) RTTs(peer Peer, d time.Duration) float64 {
	rtt := t.RTTOf(peer)
	if rtt <= 0 {
		return 0
	}
	return float64(d) / float64(rtt)
}

// Step is one span on a path and what it adds to the path's length.
type Step struct {
	Span Span
	// Wait is the time between the previous step ending and this one starting: nothing on
	// the path accounts for it.
	Wait time.Duration
	// Cost is how much this step adds to the path: its duration, less any part that
	// overlaps the previous step.
	Cost time.Duration
	// RTTs is Cost in round trips of the span's peer; zero for local work and timers.
	RTTs float64
}

// Path is a dependency chain through the trace.
type Path struct {
	Steps []Step
	// Total is the sum of the steps' costs; Wait the sum of the gaps between them. Total
	// plus Wait is the time from the trace's origin to the last step's end.
	Total time.Duration
	Wait  time.Duration
	// RTTs is the network part of Total in round trips; Timers and Local the rest.
	RTTs   float64
	Net    time.Duration
	Timers time.Duration
	Local  time.Duration
}

// Names lists the path's steps.
func (p Path) Names() []string {
	names := make([]string, len(p.Steps))
	for i, s := range p.Steps {
		names[i] = s.Span.Name
	}
	return names
}

// Terminals are the steps a join is finished at: media flowing both ways.
var Terminals = []string{PubRTP, SubRTP}

// CriticalPath is the path to whichever terminal step finished last, or, when none was
// reached, to the step that finished last.
func (t Trace) CriticalPath() Path {
	var last *Span
	for _, name := range Terminals {
		if s, ok := t.Span(name); ok && (last == nil || s.End.After(last.End)) {
			last = &s
		}
	}
	if last == nil {
		for i := range t.Spans {
			s := t.Spans[i]
			if s.Parent == "" && (last == nil || s.End.After(last.End)) {
				last = &s
			}
		}
	}
	if last == nil {
		return Path{}
	}
	return t.PathTo(last.Name)
}

// PathTo walks back from the named step, at each step following the dependency that
// finished last: the one it was actually waiting for.
func (t Trace) PathTo(name string) Path {
	byName := make(map[string]Span, len(t.Spans))
	for _, s := range t.Spans {
		if s.Parent == "" {
			byName[s.Name] = s
		}
	}
	cur, ok := byName[name]
	if !ok {
		return Path{}
	}
	var chain []Span
	seen := map[string]bool{}
	for {
		chain = append(chain, cur)
		seen[cur.Name] = true
		var pred *Span
		for _, dep := range cur.After {
			s, ok := byName[dep]
			if !ok || seen[dep] {
				continue
			}
			if pred == nil || s.End.After(pred.End) {
				pred = &s
			}
		}
		if pred == nil {
			break
		}
		cur = *pred
	}

	var p Path
	prevEnd := t.Origin
	for i := len(chain) - 1; i >= 0; i-- {
		s := chain[i]
		step := Step{Span: s}
		begin := s.Start
		if gap := s.Start.Sub(prevEnd); gap > 0 {
			step.Wait = gap
		} else {
			begin = prevEnd
		}
		if s.End.After(begin) {
			step.Cost = s.End.Sub(begin)
		}
		switch s.Kind {
		case KindNet:
			step.RTTs = t.RTTs(s.Peer, step.Cost)
			p.Net += step.Cost
		case KindTimer:
			p.Timers += step.Cost
		default:
			p.Local += step.Cost
		}
		p.Steps = append(p.Steps, step)
		p.Total += step.Cost
		p.Wait += step.Wait
		p.RTTs += step.RTTs
		if s.End.After(prevEnd) {
			prevEnd = s.End
		}
	}
	return p
}

// PublishToMedia is from Join to the first RTP packet reaching the SFU: the first one
// sent, plus half an RTT in flight.
func (t Trace) PublishToMedia() (time.Duration, bool) {
	s, ok := t.Span(PubRTP)
	if !ok || t.JoinAt.IsZero() {
		return 0, false
	}
	return s.End.Sub(t.JoinAt) + t.RTTOf(PeerUDP)/2, true
}

// SubscribeToMedia is from Join to the first RTP packet received from the SFU.
func (t Trace) SubscribeToMedia() (time.Duration, bool) {
	s, ok := t.Span(SubRTP)
	if !ok || t.JoinAt.IsZero() {
		return 0, false
	}
	return s.End.Sub(t.JoinAt), true
}

// Report is the JSON form of a Trace, for benches and the agent. Times are milliseconds
// from Origin.
type Report struct {
	// Origin is the trace's origin as Unix milliseconds.
	OriginUnixMs float64 `json:"origin_unix_ms"`
	// JoinAtMs is when Join was called, relative to the origin.
	JoinAtMs float64 `json:"join_at_ms"`
	// RTTMs is the measured RTT per peer.
	RTTMs map[Peer]float64 `json:"rtt_ms"`
	Spans []ReportSpan     `json:"spans"`
	// CriticalPath names the steps of the critical path, in order.
	CriticalPath   []string `json:"critical_path"`
	CriticalMs     float64  `json:"critical_ms"`
	CriticalRTTs   float64  `json:"critical_rtts"`
	CriticalNetMs  float64  `json:"critical_net_ms"`
	CriticalTimeMs float64  `json:"critical_timer_ms"`
	CriticalLocal  float64  `json:"critical_local_ms"`
	CriticalWaitMs float64  `json:"critical_wait_ms"`
	// PublishToMediaMs and SubscribeToMediaMs are from Join; absent when not reached.
	PublishToMediaMs   *float64 `json:"publish_to_media_ms,omitempty"`
	SubscribeToMediaMs *float64 `json:"subscribe_to_media_ms,omitempty"`
}

// ReportSpan is one span of a Report.
type ReportSpan struct {
	Name     string   `json:"name"`
	After    []string `json:"after,omitempty"`
	Parent   string   `json:"parent,omitempty"`
	Kind     Kind     `json:"kind"`
	Peer     Peer     `json:"peer"`
	StartMs  float64  `json:"start_ms"`
	EndMs    float64  `json:"end_ms"`
	Ms       float64  `json:"ms"`
	RTTs     float64  `json:"rtts"`
	Critical bool     `json:"critical,omitempty"`
	// CostMs and WaitMs are set on critical spans: what the span adds to the critical
	// path and the unexplained wait before it.
	CostMs float64 `json:"cost_ms,omitempty"`
	WaitMs float64 `json:"wait_ms,omitempty"`
	Note   string  `json:"note,omitempty"`
}

// Report builds the JSON form of the trace.
func (t Trace) Report() Report {
	path := t.CriticalPath()
	critical := make(map[string]Step, len(path.Steps))
	for _, s := range path.Steps {
		critical[s.Span.Name] = s
	}
	r := Report{
		OriginUnixMs:   float64(t.Origin.UnixNano()) / 1e6,
		JoinAtMs:       ms(t.JoinAt.Sub(t.Origin)),
		RTTMs:          make(map[Peer]float64, len(t.RTT)),
		Spans:          make([]ReportSpan, 0, len(t.Spans)),
		CriticalPath:   path.Names(),
		CriticalMs:     ms(path.Total),
		CriticalRTTs:   round2(path.RTTs),
		CriticalNetMs:  ms(path.Net),
		CriticalTimeMs: ms(path.Timers),
		CriticalLocal:  ms(path.Local),
		CriticalWaitMs: ms(path.Wait),
	}
	if t.Origin.IsZero() {
		r.OriginUnixMs, r.JoinAtMs = 0, 0
	}
	for p, d := range t.RTT {
		r.RTTMs[p] = ms(d)
	}
	for _, s := range t.Spans {
		rs := ReportSpan{
			Name:    s.Name,
			After:   s.After,
			Parent:  s.Parent,
			Kind:    s.Kind,
			Peer:    s.Peer,
			StartMs: ms(s.Start.Sub(t.Origin)),
			EndMs:   ms(s.End.Sub(t.Origin)),
			Ms:      ms(s.Duration()),
			Note:    s.Note,
		}
		if s.Kind == KindNet {
			rs.RTTs = round2(t.RTTs(s.Peer, s.Duration()))
		}
		if step, ok := critical[s.Name]; ok && s.Parent == "" {
			rs.Critical = true
			rs.CostMs = ms(step.Cost)
			rs.WaitMs = ms(step.Wait)
		}
		r.Spans = append(r.Spans, rs)
	}
	if d, ok := t.PublishToMedia(); ok {
		v := ms(d)
		r.PublishToMediaMs = &v
	}
	if d, ok := t.SubscribeToMedia(); ok {
		v := ms(d)
		r.SubscribeToMediaMs = &v
	}
	return r
}

// MarshalJSON encodes the trace as its Report.
func (t Trace) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Report())
}

func ms(d time.Duration) float64 {
	return round2(float64(d) / float64(time.Millisecond))
}

func round2(v float64) float64 {
	if v < 0 {
		return -round2(-v)
	}
	return float64(int64(v*100+0.5)) / 100
}
