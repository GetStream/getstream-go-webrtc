package rtc

import (
	"context"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/audio"
	"github.com/GetStream/getstream-go-webrtc/audio/opus"
	"github.com/GetStream/getstream-go-webrtc/track"
)

// tone builds a mono float32 sine, the stand-in for whatever a text-to-speech
// engine would hand the writer.
func tone(sampleRate int, freq float64, d time.Duration, amplitude float64) audio.PCM {
	n := int(d * time.Duration(sampleRate) / time.Second)
	samples := make([]float32, n)
	for i := range samples {
		samples[i] = float32(amplitude * math.Sin(2*math.Pi*freq*float64(i)/float64(sampleRate)))
	}
	return audio.FromFloat32(samples, sampleRate, 1)
}

func TestTrackWriterImplementsProvider(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)

	var _ track.AudioSampleProvider = w
	require.NoError(t, w.OnBind())
	require.NoError(t, w.OnUnbind())
	require.NoError(t, w.Close())
}

func TestTrackWriterCodec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		channels     int
		wantChannels uint16
	}{
		{"mono", 1, 1},
		{"stereo", 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w, err := NewTrackWriter(WriterConfig{Opus: opus.Config{Channels: tt.channels}})
			require.NoError(t, err)

			codec := w.Codec()
			require.Equal(t, webrtc.MimeTypeOpus, codec.MimeType)
			require.Equal(t, uint32(OpusClockRate), codec.ClockRate)
			require.Equal(t, tt.wantChannels, codec.Channels)
		})
	}
}

// TestTrackWriterCodecClockRateIsAlways48k pins RFC 7587: Opus RTP is stamped
// at 48 kHz even when the codec itself runs slower.
func TestTrackWriterCodecClockRateIsAlways48k(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{Opus: opus.Config{SampleRate: 16000}})
	require.NoError(t, err)
	require.Equal(t, uint32(48000), w.Codec().ClockRate)
}

func TestTrackWriterQueuesEncodedAudio(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)

	require.Zero(t, w.Buffered())
	require.NoError(t, w.Write(tone(48000, 440, time.Second, 0.5)))
	require.Equal(t, time.Second, w.Buffered())

	sample, err := w.NextSample(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, sample.Data)
	require.Equal(t, 20*time.Millisecond, sample.Duration)
	require.Equal(t, 980*time.Millisecond, w.Buffered())
}

// TestTrackWriterEmitsSilenceWhenStarved is the property that keeps the RTP
// timeline continuous: a receiver that sees a timestamp gap treats it as loss.
func TestTrackWriterEmitsSilenceWhenStarved(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)

	for range 3 {
		sample, err := w.NextSample(context.Background())
		require.NoError(t, err)
		require.NotEmpty(t, sample.Data, "starvation should produce silence, not nothing")
		require.Equal(t, 20*time.Millisecond, sample.Duration)
		require.Equal(t, silenceLevel, w.CurrentAudioLevel())
	}

	// The silence must actually decode to silence.
	dec, err := opus.NewDecoder(opus.Config{})
	require.NoError(t, err)
	sample, err := w.NextSample(context.Background())
	require.NoError(t, err)

	pcm, err := dec.Decode(sample.Data)
	require.NoError(t, err)
	require.Equal(t, 960, pcm.Len())
	require.Less(t, pcm.RMS(), 0.01)
}

func TestTrackWriterAudioLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		amplitude float64
		wantLevel uint8
		delta     float64
	}{
		// A full-scale sine is about -3 dBFS RMS, so level 3.
		{"loud", 1.0, 3, 1},
		// Half amplitude is 6 dB quieter.
		{"half", 0.5, 9, 1},
		{"silent", 0, silenceLevel, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w, err := NewTrackWriter(WriterConfig{})
			require.NoError(t, err)
			require.NoError(t, w.Write(tone(48000, 440, 100*time.Millisecond, tt.amplitude)))

			_, err = w.NextSample(context.Background())
			require.NoError(t, err)
			require.InDelta(t, tt.wantLevel, w.CurrentAudioLevel(), tt.delta)
		})
	}
}

func TestTrackWriterLevelFollowsTheQueue(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)

	// Loud first, then quiet. The level reported with each packet should
	// describe that packet, not whatever was written most recently.
	require.NoError(t, w.Write(tone(48000, 440, 100*time.Millisecond, 1.0)))
	require.NoError(t, w.Write(tone(48000, 440, 100*time.Millisecond, 0.01)))

	_, err = w.NextSample(context.Background())
	require.NoError(t, err)
	loud := w.CurrentAudioLevel()

	// Drain the five loud packets, then read a quiet one.
	for range 5 {
		_, err = w.NextSample(context.Background())
		require.NoError(t, err)
	}
	quiet := w.CurrentAudioLevel()

	require.Less(t, loud, quiet, "a lower level number means louder audio")
}

// TestTrackWriterLevelTracksWithinAWrite covers a single long write, such as a
// whole synthesised sentence: the level must follow the audio through it rather
// than reporting one average for the lot.
func TestTrackWriterLevelTracksWithinAWrite(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)

	// Half a second loud followed by half a second nearly silent, in one call.
	var src audio.PCM
	require.NoError(t, src.Append(tone(48000, 440, 500*time.Millisecond, 1.0)))
	require.NoError(t, src.Append(tone(48000, 440, 500*time.Millisecond, 0.001)))
	require.NoError(t, w.Write(src))

	levels := make([]uint8, 0, 50)
	for range 50 {
		_, err := w.NextSample(context.Background())
		require.NoError(t, err)
		levels = append(levels, w.CurrentAudioLevel())
	}

	// Packets from the first half are loud (a low number), the second half
	// quiet (a high number).
	require.Less(t, levels[5], uint8(10))
	require.Greater(t, levels[45], uint8(50))
}

func TestTrackWriterFlushEmitsThePartialFrame(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)

	// 30 ms is one whole frame plus half of another.
	require.NoError(t, w.Write(tone(48000, 440, 30*time.Millisecond, 0.5)))
	require.Equal(t, 20*time.Millisecond, w.Buffered())

	require.NoError(t, w.Flush())
	require.Equal(t, 40*time.Millisecond, w.Buffered())
}

// TestTrackWriterClearIsBargeIn covers the interruption case: the agent has
// queued a long answer and the participant starts talking over it.
func TestTrackWriterClearIsBargeIn(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)
	require.NoError(t, w.Write(tone(48000, 440, 5*time.Second, 0.5)))
	require.Equal(t, 5*time.Second, w.Buffered())

	w.Clear()
	require.Zero(t, w.Buffered())
	require.Equal(t, silenceLevel, w.CurrentAudioLevel())

	// The next sample is silence, immediately, not the rest of the sentence.
	sample, err := w.NextSample(context.Background())
	require.NoError(t, err)
	require.Equal(t, w.silence, sample.Data)
}

func TestTrackWriterBoundsItsQueue(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{BufferDuration: 200 * time.Millisecond})
	require.NoError(t, err)

	require.NoError(t, w.Write(tone(48000, 440, 5*time.Second, 0.5)))
	require.Equal(t, 200*time.Millisecond, w.Buffered(),
		"a source faster than real time must not grow the queue without bound")
}

func TestTrackWriterAcceptsAnyInputFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pcm  audio.PCM
	}{
		{"24k s16 mono, typical TTS", tone(24000, 440, time.Second, 0.5).ToInt16()},
		{"16k f32 mono", tone(16000, 440, time.Second, 0.5)},
		{"8k s16 mono, telephony", tone(8000, 440, time.Second, 0.5).ToInt16()},
		{"48k f32 mono", tone(48000, 440, time.Second, 0.5)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w, err := NewTrackWriter(WriterConfig{})
			require.NoError(t, err)
			require.NoError(t, w.Write(tt.pcm))
			require.NoError(t, w.Flush())

			// One second in, one second of packets out.
			require.InDelta(t, float64(time.Second), float64(w.Buffered()),
				float64(20*time.Millisecond))
		})
	}
}

// TestTrackWriterRoundTrip runs audio all the way through the writer and back
// out through a decoder, which is the only way to know the packets are real.
func TestTrackWriterRoundTrip(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)
	dec, err := opus.NewDecoder(opus.Config{})
	require.NoError(t, err)

	src := tone(16000, 440, 500*time.Millisecond, 0.5)
	require.NoError(t, w.Write(src))
	require.NoError(t, w.Flush())

	var out audio.PCM
	for w.Buffered() > 0 {
		sample, err := w.NextSample(context.Background())
		require.NoError(t, err)

		pcm, err := dec.Decode(sample.Data)
		require.NoError(t, err)
		require.NoError(t, out.Append(pcm))
	}

	require.Equal(t, 48000, out.SampleRate)
	require.InDelta(t, 500*time.Millisecond, out.Duration(), float64(20*time.Millisecond))
	require.InDelta(t, src.RMS(), out.RMS(), 0.05)
}

func TestTrackWriterClosed(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)
	require.NoError(t, w.Write(tone(48000, 440, time.Second, 0.5)))
	require.NoError(t, w.Close())

	// A closed writer stops the track's write loop rather than feeding it
	// silence forever.
	_, err = w.NextSample(context.Background())
	require.ErrorIs(t, err, io.EOF)

	require.Error(t, w.Write(tone(48000, 440, time.Second, 0.5)))
	require.NoError(t, w.Flush(), "flushing a closed writer is a no-op")
}

func TestTrackWriterHonoursContext(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = w.NextSample(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTrackWriterEmptyWrite(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)
	require.NoError(t, w.Write(audio.New(audio.FormatS16, 48000, 1)))
	require.Zero(t, w.Buffered())
}

func TestTrackWriterRejectsBadConfig(t *testing.T) {
	t.Parallel()

	_, err := NewTrackWriter(WriterConfig{Opus: opus.Config{SampleRate: 44100}})
	require.Error(t, err)
}

// TestTrackWriterIsConcurrencySafe runs the access pattern the SDK actually
// produces: application goroutines writing while the track's own goroutine
// pulls samples.
func TestTrackWriterIsConcurrencySafe(t *testing.T) {
	t.Parallel()

	w, err := NewTrackWriter(WriterConfig{})
	require.NoError(t, err)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, err := w.NextSample(context.Background())
				require.NoError(t, err)
				w.CurrentAudioLevel()
			}
		}
	}()

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				require.NoError(t, w.Write(tone(16000, 440, 20*time.Millisecond, 0.5)))
				w.Buffered()
			}
			w.Clear()
		}()
	}

	// Let the writers finish, then stop the reader.
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(stop)
	}()
	wg.Wait()
}
