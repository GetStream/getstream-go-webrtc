// Package rtc connects the audio package to WebRTC tracks.
//
// [TrackWriter] takes PCM and publishes it: it implements
// [track.AudioSampleProvider], so it plugs straight into
// [track.NewAudioTrack]. [TrackReader] does the reverse, turning a remote
// track's RTP into PCM.
//
// Between them a voice agent needs no codec or RTP code of its own: write what
// the text-to-speech engine produced, read what the participant said.
package rtc

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/gammazero/deque"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/GetStream/getstream-go-webrtc/audio"
	"github.com/GetStream/getstream-go-webrtc/audio/opus"
	"github.com/GetStream/getstream-go-webrtc/track"
)

// OpusClockRate is the RTP clock rate for Opus. RFC 7587 fixes it at 48 kHz
// regardless of the rate the codec is actually running at internally.
const OpusClockRate = 48000

// silenceLevel is the WebRTC audio level for silence. The scale runs from 0,
// meaning full scale, to 127.
const silenceLevel uint8 = 127

// DefaultBufferDuration is how much encoded audio a [TrackWriter] holds before
// it starts dropping the oldest. It is generous because a text-to-speech engine
// commonly produces a whole sentence faster than real time.
const DefaultBufferDuration = 30 * time.Second

// WriterConfig configures a [TrackWriter]. The zero value is a working 48 kHz
// mono voice configuration.
type WriterConfig struct {
	// Opus configures the encoder. Its zero value is 48 kHz mono at 20 ms,
	// which is what WebRTC negotiates.
	Opus opus.Config
	// BufferDuration caps how much encoded audio is queued. Once it is
	// exceeded the oldest audio is dropped, which bounds both memory and the
	// delay between writing a sample and hearing it. Defaults to
	// [DefaultBufferDuration].
	BufferDuration time.Duration
}

// TrackWriter publishes PCM to a WebRTC audio track.
//
// Call [TrackWriter.Write] with audio at any sample rate, channel count and
// format; it is resampled, encoded and queued. The track pulls packets off the
// queue in real time, so Write never blocks on the network.
//
// When the queue runs dry the writer emits encoded silence rather than
// stalling, which keeps the RTP timeline continuous. That matters: a receiver
// that sees a gap in timestamps treats it as loss and starts concealing.
//
// A TrackWriter is safe for concurrent use.
type TrackWriter struct {
	track.BaseSampleProvider

	enc           *opus.Encoder
	frameDuration time.Duration
	maxFrames     int
	silence       []byte

	mu     sync.Mutex
	queue  deque.Deque[queuedFrame]
	closed bool

	level atomic.Uint32
}

// queuedFrame is one encoded packet and the loudness of the audio around it,
// kept together so the audio-level header extension describes the packet
// actually being sent rather than whatever was written most recently.
type queuedFrame struct {
	data  []byte
	level uint8
}

// NewTrackWriter returns a writer for cfg.
func NewTrackWriter(cfg WriterConfig) (*TrackWriter, error) {
	enc, err := opus.NewEncoder(cfg.Opus)
	if err != nil {
		return nil, err
	}

	bufferDuration := cfg.BufferDuration
	if bufferDuration <= 0 {
		bufferDuration = DefaultBufferDuration
	}
	frameDuration := enc.FrameDuration()

	// Encoded once up front so that NextSample, which runs on the track's own
	// goroutine, never has to touch the encoder.
	silence, err := enc.EncodeSilence()
	if err != nil {
		return nil, err
	}

	w := &TrackWriter{
		enc:           enc,
		frameDuration: frameDuration,
		maxFrames:     max(1, int(bufferDuration/frameDuration)),
		silence:       silence,
	}
	w.level.Store(uint32(silenceLevel))
	return w, nil
}

// Codec is the RTP codec capability this writer produces, ready to hand to
// [track.NewAudioTrack].
func (w *TrackWriter) Codec() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: OpusClockRate,
		Channels:  uint16(w.enc.Config().Channels),
	}
}

// Write encodes pcm and queues it for sending. It returns as soon as the audio
// is queued, not when it has been sent.
//
// Audio that does not fill a whole frame stays in the encoder until the next
// call. Use [TrackWriter.Flush] at the end of an utterance to push it out.
func (w *TrackWriter) Write(pcm audio.PCM) error {
	if pcm.IsEmpty() {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("audio/rtc: write to closed track writer")
	}

	packets, err := w.enc.Encode(pcm)
	if err != nil {
		return err
	}
	w.enqueueWithLevels(packets, pcm)
	return nil
}

// enqueueWithLevels queues packets, giving each the loudness of the stretch of
// input it came from.
//
// Resampling preserves time, so the nth packet covers the nth frame-length
// slice of the input. The encoder may carry a partial frame over from the
// previous call, which shifts that correspondence by less than one frame:
// close enough for a loudness indicator, and much better than tagging a long
// write with a single average.
//
// The caller must hold w.mu.
func (w *TrackWriter) enqueueWithLevels(packets [][]byte, src audio.PCM) {
	if len(packets) == 0 {
		return
	}

	framesPerPacket := int(w.frameDuration * time.Duration(src.SampleRate) / time.Second)
	levels := make([]uint8, 0, len(packets))
	if framesPerPacket > 0 {
		for chunk := range src.Chunks(framesPerPacket, 0, false) {
			levels = append(levels, audioLevel(chunk))
		}
	}

	for i, p := range packets {
		level := silenceLevel
		if len(levels) > 0 {
			level = levels[min(i, len(levels)-1)]
		}
		w.queue.PushBack(queuedFrame{data: p, level: level})
	}
	w.trim()
}

// Flush pushes out the partial frame the encoder is holding, padded with
// silence. Call it when a source has finished, so its last few milliseconds are
// not left waiting for audio that will never come.
func (w *TrackWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}

	packets, err := w.enc.Flush()
	if err != nil {
		return err
	}
	w.enqueue(packets, uint8(w.level.Load()))
	return nil
}

// Clear drops everything queued and resets the encoder.
//
// This is barge-in: when a participant interrupts the agent, whatever it was
// about to say has to stop immediately, and a buffer holding several seconds of
// speech would otherwise keep playing.
func (w *TrackWriter) Clear() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.queue.Clear()
	w.enc.Reset()
	w.level.Store(uint32(silenceLevel))
}

// Buffered is how much encoded audio is waiting to be sent.
func (w *TrackWriter) Buffered() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return time.Duration(w.queue.Len()) * w.frameDuration
}

// enqueue appends packets all carrying the same level. The caller must hold
// w.mu.
func (w *TrackWriter) enqueue(packets [][]byte, level uint8) {
	for _, p := range packets {
		w.queue.PushBack(queuedFrame{data: p, level: level})
	}
	w.trim()
}

// trim drops the oldest audio once the queue is over its cap, which bounds both
// memory and how far behind real time a fast producer can push playback. The
// caller must hold w.mu.
func (w *TrackWriter) trim() {
	for w.queue.Len() > w.maxFrames {
		w.queue.PopFront()
	}
}

// NextSample implements [track.SampleProvider]. The track calls it on its own
// goroutine and paces the calls off the returned duration, so it returns
// immediately with silence rather than waiting for audio to arrive.
func (w *TrackWriter) NextSample(ctx context.Context) (media.Sample, error) {
	if err := ctx.Err(); err != nil {
		return media.Sample{}, err
	}

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return media.Sample{}, io.EOF
	}
	frame := queuedFrame{data: w.silence, level: silenceLevel}
	if w.queue.Len() > 0 {
		frame = w.queue.PopFront()
	}
	w.mu.Unlock()

	w.level.Store(uint32(frame.level))
	return media.Sample{Data: frame.data, Duration: w.frameDuration}, nil
}

// CurrentAudioLevel implements [track.AudioSampleProvider]. It reports the
// loudness of the packet most recently handed to the track, on the WebRTC scale
// where 0 is full scale and 127 is silence.
func (w *TrackWriter) CurrentAudioLevel() uint8 {
	return uint8(w.level.Load())
}

// Close stops the track's write loop. Queued audio is discarded.
func (w *TrackWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.closed = true
	w.queue.Clear()
	return nil
}

// audioLevel converts a buffer's loudness to the WebRTC audio-level scale of
// RFC 6464, which is negated dBov clamped to 0..127.
func audioLevel(pcm audio.PCM) uint8 {
	dbfs := pcm.DBFS()
	if math.IsInf(dbfs, -1) {
		return silenceLevel
	}
	level := math.Round(-dbfs)
	switch {
	case level <= 0:
		return 0
	case level >= float64(silenceLevel):
		return silenceLevel
	}
	return uint8(level)
}

// NewAudioTrack builds a local audio track fed by w, wiring up the Opus codec
// parameters so the caller does not have to restate them.
func NewAudioTrack(trackInfo *sfu_models.TrackInfo, w *TrackWriter, opts ...track.Option) (*track.Local, error) {
	return track.NewAudioTrack(trackInfo, w, w.Codec(), opts...)
}
