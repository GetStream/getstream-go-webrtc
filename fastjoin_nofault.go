//go:build !fastjoinfault

package rtc

import "github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"

// breakFastJoinGrant does nothing: only builds with the fastjoinfault tag break grants
// (fastjoin_fault.go).
func breakFastJoinGrant(int, int, *signal_rpc.FastJoinRequest) {}
