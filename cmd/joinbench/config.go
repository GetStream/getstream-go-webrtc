package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"time"

	rtc "github.com/GetStream/getstream-go-webrtc"
)

// The local stack's coordinator, used when the environment names none.
const (
	localBaseURL = "http://127.0.0.1:3030"
	localWSURL   = "ws://127.0.0.1:8800/api/v2/connect"
)

// defaultLocalRTT is the round trip injected into local runs unless -rtt says otherwise.
const defaultLocalRTT = 100 * time.Millisecond

const (
	envLocal   = "local"
	envStaging = "staging"

	flowLegacy = "legacy"
	flowFast   = "fast"

	modeCold = "cold"
	modeWarm = "warm"

	scenarioPubSub   = "pubsub"
	scenarioOneToOne = "one-to-one"
)

type config struct {
	Env       string
	Flow      string
	Modes     []string
	Scenarios []string
	// RTT is the round trip WithNetworkDelay adds to every connection; zero adds none.
	RTT      time.Duration
	Runs     int
	SFU      string
	PinTag   string
	Location string
	Budget   float64
	Out      string
	Timeout  time.Duration
	CallType string
	Debug    bool
	DAG      bool
	// BreakCandidates is how many of each fast join's candidates get a broken setup
	// grant (-break-candidates, fastjoinfault builds only), so the join falls back.
	BreakCandidates int
	// SecondJoinDelay is how long after alice's Join bob joins, at the earliest once
	// she publishes; zero joins him as soon as she publishes.
	SecondJoinDelay time.Duration
	// AudioSlots is how many audio receive slots each fast join asks for.
	AudioSlots uint
	// Gap is the pause after each call before the next: in warm mode, how long the
	// clients' connections sit idle between joins.
	Gap time.Duration

	BaseURL   string
	WSURL     string
	APIKey    string
	APISecret string
}

// parseConfig reads the flags in args, and the app and its URLs from getenv:
// STREAM_BASE_URL, STREAM_WS_URL (local only), STREAM_API_KEY and STREAM_API_SECRET.
func parseConfig(args []string, getenv func(string) string, output io.Writer) (config, error) {
	var c config
	var modes, scenarios string
	rtt := time.Duration(-1)
	fs := flag.NewFlagSet("joinbench", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&c.Env, "env", envLocal, "local (the T03 stack) or staging (STREAM_* from the environment)")
	fs.StringVar(&c.Flow, "flow", flowLegacy, "join path: legacy (coordinator join, SFU websocket join) or fast (fast_join, FastJoin); a fast join that falls back to legacy fails the run")
	fs.StringVar(&modes, "mode", "cold,warm", "comma-separated: cold (new Client per run), warm (one Client, one discarded join)")
	fs.StringVar(&scenarios, "scenario", scenarioPubSub, "comma-separated: pubsub (alice publishes, bob subscribes), one-to-one (bob publishes and subscribes, timed)")
	fs.DurationVar(&rtt, "rtt", rtt, "round trip injected with WithNetworkDelay (default 100ms for local, 0 for staging)")
	fs.IntVar(&c.Runs, "runs", 10, "measured runs per mode and scenario")
	fs.StringVar(&c.SFU, "sfu", "", "SFU id to pin every join to (sfu_id)")
	fs.StringVar(&c.PinTag, "pin-tag", "", "SFU tag to pin every join to (pin_to_tag)")
	fs.StringVar(&c.Location, "location", rtc.LocationAuto, "location sent to the coordinator: auto (GeoIP on the client address) or an airport code")
	fs.Float64Var(&c.Budget, "budget", 3.5, "warm time to media budget in round trips, for PASS/FAIL")
	fs.StringVar(&c.Out, "out", "", "append one JSON line per run to this file")
	fs.DurationVar(&c.Timeout, "timeout", 60*time.Second, "time limit of one run")
	fs.StringVar(&c.CallType, "call-type", "default", "call type")
	fs.BoolVar(&c.Debug, "debug", false, "log the SDK at debug level to stderr")
	fs.BoolVar(&c.DAG, "dag", false, "draw every measured join's DAG")
	fs.DurationVar(&c.SecondJoinDelay, "second-join-delay", 0, "bob joins this long after alice's Join, at the earliest once she publishes (one-to-one: peer_subscribe then times a late joiner reaching a settled participant)")
	fs.UintVar(&c.AudioSlots, "audio-slots", rtc.DefaultAudioReceiveSlots, "audio receive slots each fast join asks for (0: a later publisher's audio needs a renegotiation)")
	fs.DurationVar(&c.Gap, "gap", 500*time.Millisecond, "pause after each call before the next; in warm mode, how long the clients sit idle between joins")
	faultFlags(fs, &c)
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	switch c.Flow {
	case flowLegacy, flowFast:
	default:
		return config{}, fmt.Errorf("-flow %q: want legacy or fast", c.Flow)
	}
	var err error
	if c.Modes, err = list("mode", modes, modeCold, modeWarm); err != nil {
		return config{}, err
	}
	if c.Scenarios, err = list("scenario", scenarios, scenarioPubSub, scenarioOneToOne); err != nil {
		return config{}, err
	}
	if c.Runs < 1 {
		return config{}, fmt.Errorf("-runs %d: want at least 1", c.Runs)
	}
	if c.Budget <= 0 {
		return config{}, fmt.Errorf("-budget %g: want more than 0", c.Budget)
	}
	if c.SecondJoinDelay < 0 {
		return config{}, fmt.Errorf("-second-join-delay %s: want 0 or more", c.SecondJoinDelay)
	}
	if c.Gap < 0 {
		return config{}, fmt.Errorf("-gap %s: want 0 or more", c.Gap)
	}
	if c.AudioSlots > math.MaxUint32 {
		return config{}, fmt.Errorf("-audio-slots %d: too many", c.AudioSlots)
	}
	if c.BreakCandidates < 0 {
		return config{}, fmt.Errorf("-break-candidates %d: want 0 or more", c.BreakCandidates)
	}
	if c.BreakCandidates > 0 && c.Flow != flowFast {
		return config{}, errors.New("-break-candidates needs -flow fast: only the fast join has candidates")
	}
	if c.SFU != "" && c.PinTag != "" {
		return config{}, errors.New("-sfu and -pin-tag both pin the join: pass one")
	}

	c.BaseURL, c.APIKey, c.APISecret = getenv("STREAM_BASE_URL"), getenv("STREAM_API_KEY"), getenv("STREAM_API_SECRET")
	switch c.Env {
	case envLocal:
		c.WSURL = getenv("STREAM_WS_URL")
		if c.BaseURL == "" {
			c.BaseURL = localBaseURL
		}
		if c.WSURL == "" {
			c.WSURL = localWSURL
		}
		if rtt < 0 {
			rtt = defaultLocalRTT
		}
	case envStaging:
		if c.BaseURL == "" || c.APIKey == "" || c.APISecret == "" {
			return config{}, errors.New("-env staging needs STREAM_BASE_URL, STREAM_API_KEY and STREAM_API_SECRET " +
				"(set -a; . ~/.config/stream-3rtt/staging-video.env; set +a)")
		}
		if c.SFU == "" && c.PinTag == "" {
			return config{}, errors.New("-env staging needs -sfu <id> or -pin-tag staging: the app's SFUs are reached only pinned")
		}
		if c.WSURL, err = websocketURL(c.BaseURL); err != nil {
			return config{}, err
		}
		if rtt < 0 {
			rtt = 0
		}
	default:
		return config{}, fmt.Errorf("-env %q: want local or staging", c.Env)
	}
	if c.APIKey == "" || c.APISecret == "" {
		return config{}, errors.New("-env local needs STREAM_API_KEY and STREAM_API_SECRET " +
			`(eval "$(.factory/3rtt/tools/local-stack.sh env)" in video-sfu)`)
	}
	c.RTT = rtt
	return c, nil
}

// list splits a comma-separated flag value, rejecting anything not in allowed.
func list(name, value string, allowed ...string) ([]string, error) {
	var out []string
	for _, v := range strings.Split(value, ",") {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		ok := false
		for _, a := range allowed {
			ok = ok || v == a
		}
		if !ok {
			return nil, fmt.Errorf("-%s %q: want %s", name, v, strings.Join(allowed, ", "))
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-%s: empty", name)
	}
	return out, nil
}

// websocketURL is the coordinator websocket of the REST base URL.
func websocketURL(base string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("STREAM_BASE_URL %q: %w", base, err)
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	default:
		return "", fmt.Errorf("STREAM_BASE_URL %q: want http or https", base)
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/api/v2/connect"
	return u.String(), nil
}

// joinQuery is what pins the coordinator's SFU choice.
func (c config) joinQuery() url.Values {
	q := url.Values{}
	if c.SFU != "" {
		q.Set("sfu_id", c.SFU)
	}
	if c.PinTag != "" {
		q.Set("pin_to_tag", c.PinTag)
	}
	return q
}
