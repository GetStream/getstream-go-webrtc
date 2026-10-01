//go:build !fastjoinfault

package main

import "flag"

// faultFlags adds no flag: -break-candidates exists only in builds with the
// fastjoinfault tag (fault.go).
func faultFlags(*flag.FlagSet, *config) {}

func applyFaults(config) {}
