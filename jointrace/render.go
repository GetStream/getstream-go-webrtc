package jointrace

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const timelineWidth = 40

// String draws the trace: one row per step in start order, what it waited for, when it
// started and how long it took, in ms and in round trips of its peer, and a timeline
// that shows which steps ran in parallel. Critical-path steps are marked with "*" and
// drawn with "="; detail spans sit under their step. The critical path and the times to
// media follow.
func (t Trace) String() string {
	var b strings.Builder
	if len(t.Spans) == 0 {
		return "join trace: no steps recorded\n"
	}
	path := t.CriticalPath()
	critical := make(map[string]bool, len(path.Steps))
	for _, s := range path.Steps {
		critical[s.Span.Name] = true
	}

	fmt.Fprintf(&b, "join DAG (%s)\n", t.rttSummary())
	end := t.Origin
	for _, s := range t.Spans {
		if s.End.After(end) {
			end = s.End
		}
	}
	scale := end.Sub(t.Origin)

	children := make(map[string][]Span)
	for _, s := range t.Spans {
		if s.Parent != "" {
			children[s.Parent] = append(children[s.Parent], s)
		}
	}
	fmt.Fprintf(&b, "  %-22s %-36s %7s %8s %6s  %s\n", "step", "after", "start", "ms", "RTT", "timeline")
	for _, s := range t.Spans {
		if s.Parent != "" {
			continue
		}
		mark := " "
		if critical[s.Name] {
			mark = "*"
		}
		after := strings.Join(s.After, ",")
		if after == "" {
			after = "-"
		}
		fmt.Fprintf(&b, "%s %-22s %-36s %7.1f %8.1f %6s  %s%s\n", mark, s.Name, after,
			msf(s.Start.Sub(t.Origin)), msf(s.Duration()), t.rttCell(s),
			t.bar(s, scale, critical[s.Name]), noteSuffix(s.Note))
		kids := children[s.Name]
		sort.SliceStable(kids, func(i, j int) bool { return kids[i].Start.Before(kids[j].Start) })
		for _, c := range kids {
			name := "  " + strings.TrimPrefix(c.Name, s.Name)
			fmt.Fprintf(&b, "  %-22s %-36s %7.1f %8.1f %6s  %s%s\n", name, "",
				msf(c.Start.Sub(t.Origin)), msf(c.Duration()), t.rttCell(c),
				t.bar(c, scale, false), noteSuffix(c.Note))
		}
	}

	if len(path.Steps) > 0 {
		fmt.Fprintf(&b, "critical path: %s\n", strings.Join(path.Names(), " > "))
		fmt.Fprintf(&b, "  %.1f ms = %.2f RTT (%.1f ms network) + %.1f ms timers + %.1f ms local; %.1f ms unaccounted waiting\n",
			msf(path.Total), path.RTTs, msf(path.Net), msf(path.Timers), msf(path.Local), msf(path.Wait))
	}
	if d, ok := t.PublishToMedia(); ok {
		fmt.Fprintf(&b, "publish to media: %.1f ms from Join (first RTP sent + RTT/2)\n", msf(d))
	}
	if d, ok := t.SubscribeToMedia(); ok {
		fmt.Fprintf(&b, "subscribe to media: %.1f ms from Join\n", msf(d))
	}
	return b.String()
}

func (t Trace) rttSummary() string {
	if len(t.RTT) == 0 {
		return "no RTT measured"
	}
	peers := make([]string, 0, len(t.RTT))
	for p := range t.RTT {
		peers = append(peers, string(p))
	}
	sort.Strings(peers)
	parts := make([]string, 0, len(peers))
	for _, p := range peers {
		parts = append(parts, fmt.Sprintf("RTT %s %.1f ms", p, msf(t.RTT[Peer(p)])))
	}
	return strings.Join(parts, ", ")
}

func (t Trace) rttCell(s Span) string {
	if s.Kind != KindNet {
		return string(s.Kind)
	}
	if t.RTTOf(s.Peer) <= 0 {
		return "?"
	}
	return fmt.Sprintf("%.2f", t.RTTs(s.Peer, s.Duration()))
}

func (t Trace) bar(s Span, scale time.Duration, critical bool) string {
	cells := []byte(strings.Repeat(" ", timelineWidth))
	if scale > 0 {
		from := int(float64(s.Start.Sub(t.Origin)) / float64(scale) * timelineWidth)
		to := int(float64(s.End.Sub(t.Origin))/float64(scale)*timelineWidth + 0.5)
		from = clamp(from, 0, timelineWidth-1)
		to = clamp(to, from+1, timelineWidth)
		fill := byte('-')
		if critical {
			fill = '='
		}
		for i := from; i < to; i++ {
			cells[i] = fill
		}
	}
	return "|" + string(cells) + "|"
}

func noteSuffix(note string) string {
	if note == "" {
		return ""
	}
	return " " + note
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func msf(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
