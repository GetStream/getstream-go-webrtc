package rtc

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/GetStream/getstream-go-webrtc/audio"
	"github.com/GetStream/getstream-go-webrtc/audio/opus"
	"github.com/GetStream/getstream-go-webrtc/internal/red"
)

// DefaultMaxConceal caps how long a run of lost packets is concealed before the
// reader gives up and resynchronises. Beyond a few hundred milliseconds
// concealment stops sounding like the speaker and starts sounding like a fault,
// and the sender has probably stopped rather than been dropped.
const DefaultMaxConceal = 500 * time.Millisecond

// ReaderConfig configures a [TrackReader]. The zero value decodes to 48 kHz
// mono with concealment enabled.
type ReaderConfig struct {
	// Opus configures the decoder. Its zero value is 48 kHz mono at 20 ms.
	// Set SampleRate to what your consumer wants, typically 16000 for speech
	// recognition, and the decoder produces it directly instead of making you
	// resample afterwards.
	Opus opus.Config
	// DisableConcealment stops the reader from synthesising audio for lost
	// packets. By default a gap in sequence numbers is filled in, which keeps
	// the output continuous and lets the decoder fade rather than click.
	DisableConcealment bool
	// MaxConceal caps a run of concealed audio. Defaults to
	// [DefaultMaxConceal].
	MaxConceal time.Duration
}

// RTPSource is the part of [webrtc.TrackRemote] a [TrackReader] uses. Taking
// an interface lets a reader run over a recorded stream or a test fixture as
// well as a live track.
type RTPSource interface {
	ReadRTP() (*rtp.Packet, interceptor.Attributes, error)
}

// TrackReader turns a remote WebRTC audio track into PCM.
//
// It reads RTP, unwraps redundancy when the track carries it, decodes Opus, and
// fills gaps left by lost packets. [TrackReader.Read] returns one buffer per
// call; [TrackReader.Frames] wraps it as an iterator for a range loop.
//
// A TrackReader is not safe for concurrent use. Run one per track, which is the
// natural shape anyway since each track arrives on its own callback.
type TrackReader struct {
	src RTPSource
	dec *opus.Decoder
	red *red.Decoder

	conceal    bool
	maxConceal int
	clockRate  uint32

	pending []audio.PCM
	// expectedSeq is the sequence number the next packet should carry;
	// anything higher means packets went missing in between.
	expectedSeq uint16
	haveSeq     bool
	// baseTimestamp anchors PTS to the first packet seen.
	baseTimestamp uint32
	haveBase      bool
}

// NewTrackReader returns a reader for a remote audio track.
//
// The track comes from the OnTrack callback the SDK invokes when a subscription
// is established; pass its Track field.
func NewTrackReader(remote *webrtc.TrackRemote, cfg ReaderConfig) (*TrackReader, error) {
	if remote == nil {
		return nil, errors.New("audio/rtc: nil remote track")
	}
	return NewRTPReader(remote, remote.Codec().RTPCodecCapability, cfg)
}

// NewRTPReader returns a reader over any source of Opus RTP packets. codec says
// what the packets carry, which decides whether they need unwrapping from RED
// and what clock rate their timestamps use.
func NewRTPReader(src RTPSource, codec webrtc.RTPCodecCapability, cfg ReaderConfig) (*TrackReader, error) {
	if src == nil {
		return nil, errors.New("audio/rtc: nil RTP source")
	}
	dec, err := opus.NewDecoder(cfg.Opus)
	if err != nil {
		return nil, err
	}

	maxConceal := cfg.MaxConceal
	if maxConceal <= 0 {
		maxConceal = DefaultMaxConceal
	}
	clockRate := codec.ClockRate
	if clockRate == 0 {
		clockRate = OpusClockRate
	}

	r := &TrackReader{
		src:        src,
		dec:        dec,
		conceal:    !cfg.DisableConcealment,
		maxConceal: max(1, int(maxConceal/dec.FrameDuration())),
		clockRate:  clockRate,
	}
	// A track negotiated as audio/red carries each packet plus copies of its
	// predecessors, so it has to be unwrapped before the payload is Opus.
	if isRED(codec.MimeType) {
		r.red = red.NewDecoder()
	}
	return r, nil
}

func isRED(mimeType string) bool {
	return strings.EqualFold(mimeType, red.MimeTypeAudio)
}

// Read returns the next buffer of decoded audio. It blocks until a packet
// arrives, and returns [io.EOF] once the track has ended.
func (r *TrackReader) Read() (audio.PCM, error) {
	for len(r.pending) == 0 {
		if err := r.readPacket(); err != nil {
			return audio.PCM{}, err
		}
	}
	pcm := r.pending[0]
	r.pending = r.pending[1:]
	return pcm, nil
}

// Frames iterates decoded audio until the track ends. The loop stops on the
// first error; [io.EOF] is reported as the end of the stream rather than as an
// error, so a clean shutdown yields no error at all.
func (r *TrackReader) Frames() iter.Seq2[audio.PCM, error] {
	return func(yield func(audio.PCM, error) bool) {
		for {
			pcm, err := r.Read()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				yield(audio.PCM{}, err)
				return
			}
			if !yield(pcm, nil) {
				return
			}
		}
	}
}

// Close releases the reader. The remote track itself is owned by the SDK and is
// not closed here.
func (r *TrackReader) Close() error {
	r.pending = nil
	return nil
}

// readPacket pulls one RTP packet and appends whatever audio it yields, which
// may be several buffers when redundancy recovers an earlier packet, or none
// when the packet is a duplicate.
func (r *TrackReader) readPacket() error {
	pkt, _, err := r.src.ReadRTP()
	if err != nil {
		return err
	}

	packets := []*rtp.Packet{pkt}
	if r.red != nil {
		// Decode returns the primary payload plus any earlier packets it can
		// reconstruct from the redundant copies this one carries.
		recovered, rerr := r.red.Decode(pkt)
		if rerr != nil {
			return fmt.Errorf("audio/rtc: decode RED: %w", rerr)
		}
		packets = recovered
	}

	for _, p := range packets {
		if err := r.handle(p); err != nil {
			return err
		}
	}
	return nil
}

// handle decodes one Opus packet, first filling any gap in front of it.
func (r *TrackReader) handle(pkt *rtp.Packet) error {
	if !r.haveBase {
		r.baseTimestamp, r.haveBase = pkt.Timestamp, true
	}

	if r.haveSeq {
		// Sequence numbers wrap, so compare with unsigned arithmetic: a small
		// positive difference is a gap, and anything near the top of the range
		// is a packet that arrived late and has already been accounted for.
		gap := pkt.SequenceNumber - r.expectedSeq
		if gap > 0x8000 {
			return nil // duplicate or too late to matter
		}
		if gap > 0 {
			r.concealGap(int(gap), pkt.Timestamp)
		}
	}
	r.expectedSeq = pkt.SequenceNumber + 1
	r.haveSeq = true

	pcm, err := r.dec.Decode(pkt.Payload)
	if err != nil {
		// A malformed packet is a transport problem, not a reason to tear the
		// stream down; conceal it and carry on.
		if r.conceal {
			r.concealGap(1, pkt.Timestamp)
			return nil
		}
		return err
	}
	pcm.PTS = r.ptsOf(pkt.Timestamp)
	r.append(pcm)
	return nil
}

// concealGap synthesises audio for count missing packets ending just before
// timestamp.
func (r *TrackReader) concealGap(count int, timestamp uint32) {
	if !r.conceal {
		return
	}
	if count > r.maxConceal {
		// The stream has been gone long enough that concealing it would
		// invent audio rather than bridge a glitch. Resynchronise instead.
		count = r.maxConceal
		r.dec.Reset()
	}

	frameTicks := uint32(r.dec.FrameDuration() * time.Duration(r.clockRate) / time.Second)
	for i := count; i > 0; i-- {
		pcm, err := r.dec.Conceal()
		if err != nil {
			return
		}
		pcm.PTS = r.ptsOf(timestamp - uint32(i)*frameTicks)
		r.append(pcm)
	}
}

func (r *TrackReader) append(pcm audio.PCM) {
	if !pcm.IsEmpty() {
		r.pending = append(r.pending, pcm)
	}
}

// ptsOf converts an RTP timestamp to a duration since the first packet.
func (r *TrackReader) ptsOf(timestamp uint32) time.Duration {
	elapsed := timestamp - r.baseTimestamp
	return time.Duration(elapsed) * time.Second / time.Duration(r.clockRate)
}
