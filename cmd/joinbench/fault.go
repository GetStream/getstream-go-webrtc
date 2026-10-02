//go:build fastjoinfault

package main

import (
	"flag"

	rtc "github.com/GetStream/getstream-go-webrtc"
)

func faultFlags(fs *flag.FlagSet, c *config) {
	fs.IntVar(&c.BreakCandidates, "break-candidates", 0,
		"break the setup grant of each fast join's first N candidates, so their SFUs refuse it and the join falls back to candidate N+1")
	fs.IntVar(&c.BreakRounds, "break-rounds", 0,
		"break the setup grant of every candidate of each fast join's first fast_join answer (1), so the join asks fast_join again with the SFUs that failed")
}

func applyFaults(c config) {
	rtc.BreakFastJoinGrants(c.BreakCandidates)
	rtc.BreakFastJoinRounds(c.BreakRounds)
}
