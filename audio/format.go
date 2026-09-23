// Package audio provides a PCM audio buffer and the conversions that voice
// pipelines need: sample-rate and channel conversion, chunking for VAD and
// STT, WAV and G.711 encoding, and streaming adapters.
//
// The central type is [PCM], an interleaved buffer of either signed 16-bit or
// 32-bit float samples tagged with its sample rate and channel count. It is a
// value type: conversions return a new PCM sharing nothing with the original
// unless documented otherwise.
//
// This package depends only on the standard library. Opus encoding lives in
// audio/opus and the WebRTC glue lives in audio/rtc, so a program that only
// manipulates buffers does not pay for a codec.
package audio

import (
	"fmt"
	"math"
)

// Format is the sample encoding of a [PCM] buffer.
type Format uint8

const (
	// FormatS16 is signed 16-bit PCM, the range [-32768, 32767].
	FormatS16 Format = iota
	// FormatF32 is 32-bit float PCM, nominally the range [-1.0, 1.0].
	FormatF32
)

// String implements [fmt.Stringer]. The names match the "s16"/"f32" spelling
// used by the Python SDK and by ffmpeg.
func (f Format) String() string {
	switch f {
	case FormatS16:
		return "s16"
	case FormatF32:
		return "f32"
	default:
		return fmt.Sprintf("Format(%d)", uint8(f))
	}
}

// BytesPerSample is the size of a single sample of one channel.
func (f Format) BytesPerSample() int {
	switch f {
	case FormatS16:
		return 2
	case FormatF32:
		return 4
	default:
		return 0
	}
}

// Valid reports whether f is a known format.
func (f Format) Valid() bool {
	return f == FormatS16 || f == FormatF32
}

// ParseFormat maps a format name to a [Format]. It accepts the spellings used
// across the Stream SDKs and ffmpeg: "s16"/"int16" and "f32"/"float32"/"flt".
func ParseFormat(s string) (Format, error) {
	switch s {
	case "s16", "int16", "s16le":
		return FormatS16, nil
	case "f32", "float32", "flt", "f32le":
		return FormatF32, nil
	default:
		return 0, fmt.Errorf("audio: unknown format %q (want \"s16\" or \"f32\")", s)
	}
}

// Sample scaling between the two formats. Both directions use 32768 and the
// float side is clamped to the int16 range, which makes s16 -> f32 -> s16 an
// exact round trip for every input. (The Python SDK scales by 32767 on the way
// back and loses the top LSB; there is no reason to copy that here.)
const (
	scaleToFloat = 1.0 / 32768.0
	scaleToInt   = 32768.0
)

func s16ToF32(v int16) float32 {
	return float32(v) * scaleToFloat
}

func f32ToS16(v float32) int16 {
	scaled := float64(v) * scaleToInt
	switch {
	case math.IsNaN(scaled):
		return 0
	case scaled >= math.MaxInt16:
		return math.MaxInt16
	case scaled <= math.MinInt16:
		return math.MinInt16
	}
	return int16(math.Round(scaled))
}
