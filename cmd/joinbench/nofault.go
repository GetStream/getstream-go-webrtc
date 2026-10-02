//go:build !fastjoinfault

package main

import "flag"

// faultFlags adds no flag: -break-candidates and -break-rounds exist only in builds with the
// fastjoinfault tag (fault.go).
func faultFlags(*flag.FlagSet, *config) {}

func applyFaults(config) {}
