package main

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/GetStream/getstream-go-webrtc/jointrace"
)

// groupSummary is the medians of one mode and scenario.
type groupSummary struct {
	Mode, Scenario string
	OK, Total      int
	// Errors are the failed runs' errors.
	Errors []string

	RTTcMs, RTTsMs, RTTudpMs float64

	Roles []roleSummary

	Publish, Subscribe *mediaSummary

	// Verdict is PASS or FAIL against the budget for a warm group, empty for cold.
	Verdict string
}

type roleSummary struct {
	Role, User string
	Nodes      []nodeSummary
	// Path is the most frequent critical path and PathRuns how many runs took it.
	Path       []string
	PathRuns   int
	CriticalMs float64
	CriticalRT float64
}

// nodeSummary is one step's medians. FromJoinMs is its start relative to Join, so the
// client's own coordinator connection in a cold run starts before zero.
type nodeSummary struct {
	Name       string
	Runs       int
	FromJoinMs float64
	Ms         float64
	RTTs       float64
	Net        bool
	Critical   int
}

type mediaSummary struct {
	Ms, MinMs, MaxMs float64
	RTTs, NetRTTs    float64
	TimerMs          float64
}

// summarize groups results by mode and scenario, in the order they first appear.
func summarize(results []runResult, budget float64) []groupSummary {
	var out []groupSummary
	index := map[string]int{}
	byGroup := map[string][]runResult{}
	for _, r := range results {
		key := r.Mode + "/" + r.Scenario
		if _, ok := index[key]; !ok {
			index[key] = len(out)
			out = append(out, groupSummary{Mode: r.Mode, Scenario: r.Scenario})
		}
		byGroup[key] = append(byGroup[key], r)
	}
	for key, i := range index {
		out[i] = summarizeGroup(out[i], byGroup[key], budget)
	}
	return out
}

func summarizeGroup(g groupSummary, runs []runResult, budget float64) groupSummary {
	g.Total = len(runs)
	var ok []runResult
	for _, r := range runs {
		if r.Error != "" {
			g.Errors = append(g.Errors, fmt.Sprintf("run %d: %s", r.Run, r.Error))
			continue
		}
		ok = append(ok, r)
	}
	g.OK = len(ok)
	if len(ok) == 0 {
		return g
	}
	g.RTTcMs = median(values(ok, func(r runResult) (float64, bool) { return r.RTTcMs, r.RTTcMs > 0 }))
	g.RTTsMs = median(values(ok, func(r runResult) (float64, bool) { return r.RTTsMs, r.RTTsMs > 0 }))
	g.RTTudpMs = median(values(ok, func(r runResult) (float64, bool) { return r.RTTudpMs, r.RTTudpMs > 0 }))
	g.Publish = summarizeMedia(ok, func(r runResult) *toMedia { return r.Publish })
	g.Subscribe = summarizeMedia(ok, func(r runResult) *toMedia { return r.Subscribe })

	var roles []string
	for _, r := range ok {
		for _, t := range r.Traces {
			if !slices.Contains(roles, t.Role) {
				roles = append(roles, t.Role)
			}
		}
	}
	for _, role := range roles {
		var reports []jointrace.Report
		user := ""
		for _, r := range ok {
			for _, t := range r.Traces {
				if t.Role == role {
					reports = append(reports, t.Trace)
					user = t.User
				}
			}
		}
		rs := summarizeRole(reports)
		rs.Role, rs.User = role, user
		g.Roles = append(g.Roles, rs)
	}

	if g.Mode == modeWarm {
		g.Verdict = "PASS"
		for _, m := range []*mediaSummary{g.Publish, g.Subscribe} {
			if m == nil || m.RTTs > budget {
				g.Verdict = "FAIL"
			}
		}
	}
	return g
}

func summarizeMedia(runs []runResult, get func(runResult) *toMedia) *mediaSummary {
	var all []*toMedia
	for _, r := range runs {
		if m := get(r); m != nil {
			all = append(all, m)
		}
	}
	if len(all) == 0 {
		return nil
	}
	pick := func(f func(*toMedia) float64) []float64 {
		v := make([]float64, len(all))
		for i, m := range all {
			v[i] = f(m)
		}
		return v
	}
	msv := pick(func(m *toMedia) float64 { return m.Ms })
	return &mediaSummary{
		Ms:      median(msv),
		MinMs:   slices.Min(msv),
		MaxMs:   slices.Max(msv),
		RTTs:    median(pick(func(m *toMedia) float64 { return m.RTTs })),
		NetRTTs: median(pick(func(m *toMedia) float64 { return m.NetRTTs })),
		TimerMs: median(pick(func(m *toMedia) float64 { return m.TimerMs })),
	}
}

func summarizeRole(reports []jointrace.Report) roleSummary {
	var rs roleSummary
	type acc struct {
		fromJoin, ms, rtts []float64
		net                bool
		critical           int
	}
	nodes := map[string]*acc{}
	paths := map[string]int{}
	var critMs, critRTTs []float64
	for _, rep := range reports {
		for _, s := range rep.Spans {
			if s.Parent != "" {
				continue
			}
			a := nodes[s.Name]
			if a == nil {
				a = &acc{}
				nodes[s.Name] = a
			}
			a.fromJoin = append(a.fromJoin, s.StartMs-rep.JoinAtMs)
			a.ms = append(a.ms, s.Ms)
			a.rtts = append(a.rtts, s.RTTs)
			a.net = s.Kind == jointrace.KindNet
			if s.Critical {
				a.critical++
			}
		}
		paths[strings.Join(rep.CriticalPath, " > ")]++
		critMs = append(critMs, rep.CriticalMs)
		critRTTs = append(critRTTs, rep.CriticalRTTs)
	}
	for name, a := range nodes {
		rs.Nodes = append(rs.Nodes, nodeSummary{
			Name: name, Runs: len(a.ms), FromJoinMs: median(a.fromJoin),
			Ms: median(a.ms), RTTs: median(a.rtts), Net: a.net, Critical: a.critical,
		})
	}
	sort.Slice(rs.Nodes, func(i, j int) bool {
		if rs.Nodes[i].FromJoinMs != rs.Nodes[j].FromJoinMs {
			return rs.Nodes[i].FromJoinMs < rs.Nodes[j].FromJoinMs
		}
		return rs.Nodes[i].Name < rs.Nodes[j].Name
	})
	best := ""
	for path, n := range paths {
		if n > rs.PathRuns || (n == rs.PathRuns && path < best) {
			best, rs.PathRuns = path, n
		}
	}
	if best != "" {
		rs.Path = strings.Split(best, " > ")
	}
	rs.CriticalMs, rs.CriticalRT = median(critMs), median(critRTTs)
	return rs
}

// printSummary writes the summaries as the table every task quotes.
func printSummary(w io.Writer, c config, groups []groupSummary) {
	for _, g := range groups {
		fmt.Fprintf(w, "\n== %s %s: %d/%d runs (env %s, flow %s, injected RTT %s)\n",
			g.Mode, g.Scenario, g.OK, g.Total, c.Env, c.Flow, c.RTT)
		for _, e := range g.Errors {
			fmt.Fprintf(w, "   %s\n", e)
		}
		if g.OK == 0 {
			continue
		}
		fmt.Fprintf(w, "RTT_c %.1f ms, RTT_s %.1f ms, RTT_udp %.1f ms (medians)\n", g.RTTcMs, g.RTTsMs, g.RTTudpMs)
		for _, r := range g.Roles {
			fmt.Fprintf(w, "\n%s (%s): medians per step over %d runs\n", r.Role, r.User, g.OK)
			fmt.Fprintf(w, "  %-22s %10s %9s %7s %9s\n", "step", "from Join", "ms", "RTT", "critical")
			for _, n := range r.Nodes {
				rtt := "local"
				if n.Net {
					rtt = fmt.Sprintf("%.2f", n.RTTs)
				} else if strings.HasSuffix(n.Name, "debounce") {
					rtt = "timer"
				}
				fmt.Fprintf(w, "  %-22s %10.1f %9.1f %7s %4d/%-4d\n", n.Name, n.FromJoinMs, n.Ms, rtt, n.Critical, n.Runs)
			}
			fmt.Fprintf(w, "  critical path (%d/%d runs): %s\n", r.PathRuns, g.OK, strings.Join(r.Path, " > "))
			fmt.Fprintf(w, "  critical total %.1f ms = %.2f RTT network, from the trace origin (medians)\n", r.CriticalMs, r.CriticalRT)
		}
		fmt.Fprintf(w, "\ntime to media from Join (median, [min, max]):\n")
		for _, m := range []struct {
			name string
			s    *mediaSummary
		}{{"publish", g.Publish}, {"subscribe", g.Subscribe}} {
			if m.s == nil {
				fmt.Fprintf(w, "  %-10s not reached\n", m.name)
				continue
			}
			fmt.Fprintf(w, "  %-10s %7.1f ms = %5.2f RTT  (path: %.2f RTT network + %.0f ms timers)  [%.0f, %.0f]\n",
				m.name, m.s.Ms, m.s.RTTs, m.s.NetRTTs, m.s.TimerMs, m.s.MinMs, m.s.MaxMs)
		}
		if g.Verdict != "" {
			fmt.Fprintf(w, "warm budget %.1f RTT: %s\n", c.Budget, g.Verdict)
		}
	}
}

func values(runs []runResult, get func(runResult) (float64, bool)) []float64 {
	var v []float64
	for _, r := range runs {
		if x, ok := get(r); ok {
			v = append(v, x)
		}
	}
	return v
}

// median of v, the mean of the two middle values for an even count; zero when empty.
func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := slices.Clone(v)
	sort.Float64s(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return round2((s[n/2-1] + s[n/2]) / 2)
}
