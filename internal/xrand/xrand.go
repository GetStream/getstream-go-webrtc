// Package xrand generates random identifiers used for WebRTC track and
// session IDs.
package xrand

import "crypto/rand"

const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// NewRandom returns a random alphanumeric string of length n.
func NewRandom(n int) string {
	if n <= 0 {
		return ""
	}
	out := make([]byte, 0, n)
	// 6-bit samples cover 0..63; values outside the alphabet are rejected so
	// every character is uniformly distributed.
	buf := make([]byte, n+n/2+8)
	for len(out) < n {
		if _, err := rand.Read(buf); err != nil {
			panic("xrand: crypto/rand unavailable: " + err.Error())
		}
		for _, b := range buf {
			idx := b & 0x3f
			if int(idx) >= len(alphabet) {
				continue
			}
			out = append(out, alphabet[idx])
			if len(out) == n {
				break
			}
		}
	}
	return string(out)
}
