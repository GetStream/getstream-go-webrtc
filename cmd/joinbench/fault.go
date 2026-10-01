//go:build fastjoinfault

package main

import (
	"flag"

	rtc "github.com/GetStream/getstream-go-webrtc"
)

func faultFlags(fs *flag.FlagSet, c *config) {
	fs.IntVar(&c.BreakCandidates, "break-candidates", 0,
		"break the setup grant of each fast join's first N candidates, so their SFUs refuse it and the join falls back to candidate N+1")
}

func applyFaults(c config) {
	rtc.BreakFastJoinGrants(c.BreakCandidates)
}
