package interceptor

import (
	"strings"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
)

// Default RTX probing configuration
//
// Throughput estimate with defaults:
// - Batches: 1000ms / 100ms = 10 batches
// - Packets: 10 * 5 = 50 packets
// - Packet size: ~12 (RTP) + ~8 (extensions) + 1 (payload) = ~21 bytes RTP
// - With UDP/IP: ~21 + 28 = ~49 bytes on wire
// - Total: 50 * 49 ≈ 2.5 KB in first second ≈ 20 kbps
const (
	DefaultRTXProbeBatchSize    = 5                      // Number of packets per batch
	DefaultRTXProbeInterval     = 100 * time.Millisecond // Interval between batches
	DefaultRTXProbeDuration     = 1 * time.Second        // Total probing duration
	DefaultRTXProbeInitialDelay = 50 * time.Millisecond  // Initial delay before probing starts
)

// RTXProberConfig holds configuration for the RTX prober.
type RTXProberConfig struct {
	BatchSize    int           // Number of packets per batch
	Interval     time.Duration // Interval between batches
	Duration     time.Duration // Total probing duration
	InitialDelay time.Duration // Initial delay before probing starts
}

// RTXProberFactory creates RTX prober interceptors that send probe packets
// to help the SFU discover RTX SSRC mappings.
type RTXProberFactory struct {
	config RTXProberConfig
}

// NewRTXProberFactory creates a new RTX prober factory with default configuration.
func NewRTXProberFactory() *RTXProberFactory {
	return &RTXProberFactory{
		config: RTXProberConfig{
			BatchSize:    DefaultRTXProbeBatchSize,
			Interval:     DefaultRTXProbeInterval,
			Duration:     DefaultRTXProbeDuration,
			InitialDelay: DefaultRTXProbeInitialDelay,
		},
	}
}

// NewRTXProberFactoryWithConfig creates a new RTX prober factory with custom configuration.
func NewRTXProberFactoryWithConfig(config RTXProberConfig) *RTXProberFactory {
	// Apply defaults for zero values
	if config.BatchSize == 0 {
		config.BatchSize = DefaultRTXProbeBatchSize
	}
	if config.Interval == 0 {
		config.Interval = DefaultRTXProbeInterval
	}
	if config.Duration == 0 {
		config.Duration = DefaultRTXProbeDuration
	}
	if config.InitialDelay == 0 {
		config.InitialDelay = DefaultRTXProbeInitialDelay
	}
	return &RTXProberFactory{config: config}
}

// NewInterceptor creates a new RTX prober interceptor.
func (f *RTXProberFactory) NewInterceptor(_ string) (interceptor.Interceptor, error) {
	return &RTXProber{
		config:  f.config,
		streams: make(map[uint32]*rtxProberStream),
	}, nil
}

// RTXProber is an interceptor that sends RTX probe packets to help
// the remote peer discover RTX SSRC mappings via header extensions.
type RTXProber struct {
	interceptor.NoOp

	config  RTXProberConfig
	mu      sync.Mutex
	streams map[uint32]*rtxProberStream
}

type rtxProberStream struct {
	info      *interceptor.StreamInfo
	writer    interceptor.RTPWriter
	sequencer rtp.Sequencer
	probed    bool
	midExtID  uint8
	ridExtID  uint8
	rsidExtID uint8
}

// BindLocalStream implements interceptor.Interceptor.
func (p *RTXProber) BindLocalStream(info *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	// Only handle streams that have RTX configured
	if info.SSRCRetransmission == 0 || info.PayloadTypeRetransmission == 0 {
		return writer
	}

	// Only probe video streams (RTX is typically only for video)
	if info.MimeType == "" || !strings.HasPrefix(info.MimeType, "video") {
		return writer
	}

	// Get header extension IDs
	midExtID := getHeaderExtensionID(info.RTPHeaderExtensions, sdp.SDESMidURI)
	ridExtID := getHeaderExtensionID(info.RTPHeaderExtensions, sdp.SDESRTPStreamIDURI)
	rsidExtID := getHeaderExtensionID(info.RTPHeaderExtensions, sdp.SDESRepairRTPStreamIDURI)

	if midExtID == 0 || rsidExtID == 0 {
		// Required extensions not available
		return writer
	}

	stream := &rtxProberStream{
		info:      info,
		writer:    writer,
		sequencer: rtp.NewRandomSequencer(),
		midExtID:  midExtID,
		ridExtID:  ridExtID,
		rsidExtID: rsidExtID,
	}

	p.mu.Lock()
	p.streams[info.SSRC] = stream
	p.mu.Unlock()

	return &rtxProberWriter{
		stream: stream,
		prober: p,
	}
}

// UnbindLocalStream implements interceptor.Interceptor.
func (p *RTXProber) UnbindLocalStream(info *interceptor.StreamInfo) {
	p.mu.Lock()
	delete(p.streams, info.SSRC)
	p.mu.Unlock()
}

type rtxProberWriter struct {
	stream *rtxProberStream
	prober *RTXProber
	once   sync.Once
}

func (w *rtxProberWriter) Write(header *rtp.Header, payload []byte, attrs interceptor.Attributes) (int, error) {
	// Capture mid and rid from the first packet, then start probing
	w.once.Do(func() {
		var mid, rid string

		// Extract mid from header extension
		if w.stream.midExtID != 0 {
			if ext := header.GetExtension(w.stream.midExtID); ext != nil {
				mid = string(ext)
			}
		}

		// Extract rid from header extension
		if w.stream.ridExtID != 0 {
			if ext := header.GetExtension(w.stream.ridExtID); ext != nil {
				rid = string(ext)
			}
		}

		// Start probing in background
		go w.prober.probeStream(w.stream, mid, rid)
	})

	return w.stream.writer.Write(header, payload, attrs)
}

func (p *RTXProber) probeStream(stream *rtxProberStream, mid, rid string) {
	p.mu.Lock()
	if stream.probed {
		p.mu.Unlock()
		return
	}
	stream.probed = true
	p.mu.Unlock()

	// If no mid was found, we can't probe properly
	if mid == "" {
		return
	}

	// RTX probing only works for simulcast streams that have rid set.
	// For non-simulcast streams (rid == ""), the SFU cannot correlate the RTX
	// stream because there's no matching rid/rsid pair. Non-simulcast streams
	// would need SDP-based RTX discovery (a=ssrc-group:FID) instead.
	if rid == "" {
		return
	}

	// Wait before sending probe packets to allow the connection to stabilize
	// and ensure the SFU has set up interceptors for the RTX SSRC
	time.Sleep(p.config.InitialDelay)

	// Send probe packets in batches at regular intervals for the probe duration
	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()

	deadline := time.Now().Add(p.config.Duration)
	for time.Now().Before(deadline) {
		// Send a batch of probe packets
		for range p.config.BatchSize {
			if err := p.sendProbePacket(stream, mid, rid); err != nil {
				return
			}
		}

		// Wait for next interval
		<-ticker.C
	}
}

func (p *RTXProber) sendProbePacket(stream *rtxProberStream, mid, rid string) error {
	info := stream.info

	// Create RTX probe packet
	// RTX packets have a different SSRC and payload type
	header := &rtp.Header{
		Version:        2,
		Padding:        true,
		Extension:      true,
		Marker:         false,
		PayloadType:    info.PayloadTypeRetransmission,
		SequenceNumber: stream.sequencer.NextSequenceNumber(),
		Timestamp:      0, // Probe packet, timestamp doesn't matter
		SSRC:           info.SSRCRetransmission,
	}

	// Set up one-byte header extension profile
	header.ExtensionProfile = 0xBEDE

	// Set mid extension - same as base stream
	if err := header.SetExtension(stream.midExtID, []byte(mid)); err != nil {
		return err
	}

	// Set rsid extension - this is the key: rsid should match the rid of the base stream
	// This allows the SFU to correlate the RTX stream with its base stream.
	// Note: rid is NOT set because this is a repair stream, not a base stream.
	if err := header.SetExtension(stream.rsidExtID, []byte(rid)); err != nil {
		return err
	}

	// Minimal pure padding payload: 1 byte with value 1.
	// Per RTP spec, when P bit is set, the last byte indicates padding count.
	// For the SFU to recognize this as pure padding: payload_size - padding_count == 0
	// So 1 byte with value 1 means the entire payload is padding (1 - 1 = 0).
	payload := []byte{1}

	_, err := stream.writer.Write(header, payload, nil)
	return err
}

func getHeaderExtensionID(extensions []interceptor.RTPHeaderExtension, uri string) uint8 {
	for _, ext := range extensions {
		if ext.URI == uri {
			return uint8(ext.ID)
		}
	}
	return 0
}
