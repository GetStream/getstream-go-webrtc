// Command joinbench measures how long a Go client takes from Call.Join to audio both
// ways, through the coordinator and an SFU, and reports every step of the join in
// round trips (the 3RTT project's join DAG).
//
// Each run is a fresh call with two users. pubsub: alice joins and publishes (her
// publish time to media is measured), then bob joins and subscribes to her (his
// subscribe time is measured). one-to-one: alice joins and publishes, then bob joins,
// publishes and subscribes, and both of his times are measured: the agent case.
//
// cold builds new Clients for every run: new coordinator websocket, new HTTP
// transports, token not cached. warm keeps one Client per user across the runs,
// after one discarded call, so later joins reuse its connections and token.
//
//	eval "$(~/src/video-sfu/.factory/3rtt/tools/local-stack.sh env)"
//	go run ./cmd/joinbench -env local -mode cold,warm -rtt 100ms -runs 10 -out local.jsonl
//
//	set -a; . ~/.config/stream-3rtt/staging-video.env; set +a
//	go run ./cmd/joinbench -env staging -sfu <sfu id> -out staging.jsonl
//
// Every run is one JSON line (-out, "-" for stdout) with the full join trace of each
// measured client; the summary at the end has the medians per step, the critical path,
// and, for warm runs, PASS or FAIL against -budget round trips.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
)

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Getenv, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "joinbench:", err)
		os.Exit(2)
	}
	slog.SetLogLoggerLevel(slog.LevelWarn)
	if err := run(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "joinbench:", err)
		os.Exit(1)
	}
}

func run(cfg config, stdout io.Writer) error {
	var out io.Writer
	switch cfg.Out {
	case "":
	case "-":
		out = stdout
	default:
		f, err := os.OpenFile(cfg.Out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}

	b := newBench(cfg)
	fmt.Fprintf(stdout, "joinbench: env %s (%s), flow %s, modes %v, scenarios %v, runs %d, injected RTT %s, sfu %q, pin tag %q, location %q\n",
		cfg.Env, cfg.BaseURL, cfg.Flow, cfg.Modes, cfg.Scenarios, cfg.Runs, cfg.RTT, cfg.SFU, cfg.PinTag, cfg.Location)
	var results []runResult
	var writeErr error
	emit := func(r runResult) {
		results = append(results, r)
		fmt.Fprintln(stdout, runLine(r))
		if cfg.DAG {
			for _, d := range r.dags {
				fmt.Fprintln(stdout, d)
			}
		}
		if out != nil && writeErr == nil {
			line, err := json.Marshal(r)
			if err == nil {
				_, err = fmt.Fprintf(out, "%s\n", line)
			}
			writeErr = err
		}
	}
	for _, mode := range cfg.Modes {
		if err := b.runMode(mode, emit); err != nil {
			return fmt.Errorf("%s: %w", mode, err)
		}
	}
	if writeErr != nil {
		return fmt.Errorf("write %s: %w", cfg.Out, writeErr)
	}
	groups := summarize(results, cfg.Budget)
	printSummary(stdout, cfg, groups)
	for _, g := range groups {
		if g.OK > 0 {
			return nil
		}
	}
	return fmt.Errorf("no run succeeded")
}

func runLine(r runResult) string {
	if r.Error != "" {
		return fmt.Sprintf("%s %s run %d: %s", r.Mode, r.Scenario, r.Run, r.Error)
	}
	line := fmt.Sprintf("%s %s run %d on %s:", r.Mode, r.Scenario, r.Run, r.SFU)
	for _, m := range []struct {
		name string
		m    *toMedia
	}{{"publish", r.Publish}, {"subscribe", r.Subscribe}} {
		if m.m != nil {
			line += fmt.Sprintf(" %s %.0f ms (%.1f RTT),", m.name, m.m.Ms, m.m.RTTs)
		}
	}
	return line + fmt.Sprintf(" RTT_c %.1f ms, RTT_s %.1f ms, RTT_udp %.1f ms", r.RTTcMs, r.RTTsMs, r.RTTudpMs)
}
