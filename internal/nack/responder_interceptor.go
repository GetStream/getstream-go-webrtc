// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package nack

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/logging"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

const (
	// uint16SizeHalf is half of a math.MaxUint16.
	uint16SizeHalf = 1 << 15

	maxPayloadLen = 1460

	rtxSSRCByteLength = 2
)

// ErrInvalidSize is returned when an incorrect buffer size is supplied.
var ErrInvalidSize = errors.New("invalid buffer size")

var (
	errPacketReleased          = errors.New("could not retain packet, already released")
	errFailedToCastHeaderPool  = errors.New("could not access header pool, failed cast")
	errFailedToCastPayloadPool = errors.New("could not access payload pool, failed cast")
	errPaddingOverflow         = errors.New("padding size exceeds payload size")
)

// ResponderOption can be used to configure ResponderInterceptor.
type ResponderOption func(s *ResponderInterceptor) error

// ResponderSize sets the size of the interceptor.
// Size must be one of: 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768.
func ResponderSize(size uint16) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.size = size
		return nil
	}
}

// ResponderLog sets a logger for the interceptor.
func ResponderLog(log logging.LeveledLogger) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.log = log
		return nil
	}
}

// ResponderMaxRetries sets the maximum number of times a packet can be retransmitted.
// Default is 3.
func ResponderMaxRetries(maxRetries uint8) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.maxRetries = maxRetries
		return nil
	}
}

// ResponderMinInterval sets the minimum interval between retransmissions of the same packet.
// Default is 20ms.
func ResponderMinInterval(minInterval time.Duration) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.minInterval = minInterval
		return nil
	}
}

// ResponderMaxAge sets the maximum age of a packet that can be retransmitted.
// Packets older than this will not be retransmitted.
// Default is 2s.
func ResponderMaxAge(maxAge time.Duration) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.maxAge = maxAge
		return nil
	}
}

// DisableCopy bypasses copy of underlying packets. It should be used when
// you are not re-using underlying buffers of packets that have been written.
func DisableCopy() ResponderOption {
	return func(s *ResponderInterceptor) error {
		s.packetFactory = &packetFactoryNoOp{}
		return nil
	}
}

// ResponderStreamsFilter sets filter for local streams.
func ResponderStreamsFilter(filter func(info *interceptor.StreamInfo) bool) ResponderOption {
	return func(r *ResponderInterceptor) error {
		r.streamsFilter = filter
		return nil
	}
}

// ResponderInterceptorFactory is a interceptor.Factory for a ResponderInterceptor.
type ResponderInterceptorFactory struct {
	opts []ResponderOption
}

// NewResponderInterceptor returns a new ResponderInterceptorFactory.
func NewResponderInterceptor(opts ...ResponderOption) (*ResponderInterceptorFactory, error) {
	return &ResponderInterceptorFactory{opts}, nil
}

// NewInterceptor constructs a new ResponderInterceptor.
func (r *ResponderInterceptorFactory) NewInterceptor(_ string) (interceptor.Interceptor, error) {
	responderInterceptor := &ResponderInterceptor{
		streamsFilter: streamSupportNack,
		size:          2048,
		maxRetries:    3,
		minInterval:   20 * time.Millisecond,
		maxAge:        2 * time.Second,
		streams:       map[uint32]*localStream{},
	}

	for _, opt := range r.opts {
		if err := opt(responderInterceptor); err != nil {
			return nil, err
		}
	}

	if responderInterceptor.log == nil {
		responderInterceptor.log = logging.NewDefaultLoggerFactory().NewLogger("nack_responder")
	}
	if responderInterceptor.packetFactory == nil {
		responderInterceptor.packetFactory = newPacketFactoryCopy()
	}

	if _, err := newRTPBuffer(responderInterceptor.size); err != nil {
		return nil, err
	}

	return responderInterceptor, nil
}

// ResponderInterceptor responds to nack feedback messages.
type ResponderInterceptor struct {
	interceptor.NoOp
	streamsFilter func(info *interceptor.StreamInfo) bool
	size          uint16
	log           logging.LeveledLogger
	packetFactory packetFactory

	// Rate limiting options
	maxRetries  uint8
	minInterval time.Duration
	maxAge      time.Duration

	streams   map[uint32]*localStream
	streamsMu sync.Mutex
}

type localStream struct {
	rtpBuffer      *rtpBuffer
	rtpBufferMutex sync.RWMutex
	rtpWriter      interceptor.RTPWriter

	// Rate limiting options
	maxRetries  uint8
	minInterval time.Duration
	maxAge      time.Duration
}

// BindRTCPReader lets you modify any incoming RTCP packets. It is called once per sender/receiver, however this might
// change in the future. The returned method will be called once per packet batch.
func (n *ResponderInterceptor) BindRTCPReader(reader interceptor.RTCPReader) interceptor.RTCPReader {
	return interceptor.RTCPReaderFunc(func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
		i, attr, err := reader.Read(b, a)
		if err != nil {
			return 0, nil, err
		}

		if attr == nil {
			attr = make(interceptor.Attributes)
		}
		pkts, err := attr.GetRTCPPackets(b[:i])
		if err != nil {
			return 0, nil, err
		}
		for _, rtcpPacket := range pkts {
			nack, ok := rtcpPacket.(*rtcp.TransportLayerNack)
			if !ok {
				continue
			}

			go n.resendPackets(nack)
		}

		return i, attr, err
	})
}

// BindLocalStream lets you modify any outgoing RTP packets. It is called once for per LocalStream.
// The returned method will be called once per rtp packet.
func (n *ResponderInterceptor) BindLocalStream(
	info *interceptor.StreamInfo, writer interceptor.RTPWriter,
) interceptor.RTPWriter {
	if !n.streamsFilter(info) {
		return writer
	}

	// error is already checked in NewInterceptor
	buffer, _ := newRTPBuffer(n.size)
	stream := &localStream{
		rtpBuffer:   buffer,
		rtpWriter:   writer,
		maxRetries:  n.maxRetries,
		minInterval: n.minInterval,
		maxAge:      n.maxAge,
	}
	n.streamsMu.Lock()
	n.streams[info.SSRC] = stream
	n.streamsMu.Unlock()

	return interceptor.RTPWriterFunc(
		func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
			// If this packet doesn't belong to the main SSRC, do not add it to rtpBuffer
			if header.SSRC != info.SSRC {
				return writer.Write(header, payload, attributes)
			}

			pkt, err := n.packetFactory.NewPacket(header, payload, info.SSRCRetransmission, info.PayloadTypeRetransmission)
			if err != nil {
				return 0, err
			}

			stream.rtpBufferMutex.Lock()
			stream.rtpBuffer.Add(pkt)
			stream.rtpBufferMutex.Unlock()

			return writer.Write(header, payload, attributes)
		},
	)
}

// UnbindLocalStream is called when the Stream is removed. It can be used to clean up any data related to that track.
func (n *ResponderInterceptor) UnbindLocalStream(info *interceptor.StreamInfo) {
	n.streamsMu.Lock()
	delete(n.streams, info.SSRC)
	n.streamsMu.Unlock()
}

func (n *ResponderInterceptor) resendPackets(nack *rtcp.TransportLayerNack) {
	n.streamsMu.Lock()
	stream, ok := n.streams[nack.MediaSSRC]
	n.streamsMu.Unlock()
	if !ok {
		return
	}

	for i := range nack.Nacks {
		nack.Nacks[i].Range(func(seq uint16) bool {
			stream.rtpBufferMutex.Lock()
			p := stream.rtpBuffer.Get(seq)
			if p == nil {
				stream.rtpBufferMutex.Unlock()
				return true
			}

			now := time.Now()

			// Check max age
			if stream.maxAge > 0 && now.Sub(p.sentAt) > stream.maxAge {
				stream.rtpBufferMutex.Unlock()
				p.Release()
				return true
			}

			// Check max retries
			if stream.maxRetries > 0 && p.resendCount >= stream.maxRetries {
				stream.rtpBufferMutex.Unlock()
				p.Release()
				return true
			}

			// Check min interval
			if stream.minInterval > 0 && !p.lastResendAt.IsZero() && now.Sub(p.lastResendAt) < stream.minInterval {
				stream.rtpBufferMutex.Unlock()
				p.Release()
				return true
			}

			// Update retransmission tracking
			p.lastResendAt = now
			p.resendCount++

			stream.rtpBufferMutex.Unlock()

			// send without holding rtpBufferMutex
			if _, err := stream.rtpWriter.Write(p.Header(), p.Payload(), interceptor.Attributes{}); err != nil {
				n.log.Warnf("failed resending nacked packet: %+v", err)
			}
			p.Release()

			return true
		})
	}
}

func streamSupportNack(info *interceptor.StreamInfo) bool {
	for _, fb := range info.RTCPFeedback {
		if fb.Type == "nack" && fb.Parameter == "" {
			return true
		}
	}
	return false
}

// ============================================================================
// RTP Buffer implementation (based on pion/interceptor/internal/rtpbuffer)
// ============================================================================

// rtpBuffer stores RTP packets and allows custom logic
// around the lifetime of them via the packetFactory.
type rtpBuffer struct {
	packets      []*retainablePacket
	size         uint16
	highestAdded uint16
	started      bool
}

// newRTPBuffer constructs a new rtpBuffer.
func newRTPBuffer(size uint16) (*rtpBuffer, error) {
	allowedSizes := make([]uint16, 0)
	correctSize := false
	for i := 0; i < 16; i++ {
		if size == 1<<i {
			correctSize = true
			break
		}
		allowedSizes = append(allowedSizes, 1<<i)
	}

	if !correctSize {
		return nil, fmt.Errorf("%w: %d is not a valid size, allowed sizes: %v", ErrInvalidSize, size, allowedSizes)
	}

	return &rtpBuffer{
		packets: make([]*retainablePacket, size),
		size:    size,
	}, nil
}

// Add places the retainablePacket in the rtpBuffer.
func (r *rtpBuffer) Add(packet *retainablePacket) {
	seq := packet.sequenceNumber
	if !r.started {
		r.packets[seq%r.size] = packet
		r.highestAdded = seq
		r.started = true
		return
	}

	diff := seq - r.highestAdded
	if diff == 0 {
		return
	} else if diff < uint16SizeHalf {
		for i := r.highestAdded + 1; i != seq; i++ {
			idx := i % r.size
			prevPacket := r.packets[idx]
			if prevPacket != nil {
				prevPacket.Release()
			}
			r.packets[idx] = nil
		}
		r.highestAdded = seq
	}

	idx := seq % r.size
	prevPacket := r.packets[idx]
	if prevPacket != nil {
		prevPacket.Release()
	}
	r.packets[idx] = packet
}

// Get returns the retainablePacket for the requested sequence number.
func (r *rtpBuffer) Get(seq uint16) *retainablePacket {
	diff := r.highestAdded - seq
	if diff >= uint16SizeHalf {
		return nil
	}

	if diff >= r.size {
		return nil
	}

	pkt := r.packets[seq%r.size]
	if pkt != nil {
		if pkt.sequenceNumber != seq {
			return nil
		}
		// already released
		if err := pkt.Retain(); err != nil {
			return nil
		}
	}

	return pkt
}

// ============================================================================
// Retainable Packet implementation
// ============================================================================

// retainablePacket is a reference counted RTP packet with retransmission tracking.
type retainablePacket struct {
	onRelease func(*rtp.Header, *[]byte)

	countMu sync.Mutex
	count   int

	header  *rtp.Header
	buffer  *[]byte
	payload []byte

	sequenceNumber uint16

	// Retransmission tracking
	sentAt       time.Time
	lastResendAt time.Time
	resendCount  uint8
}

// Header returns the RTP Header of the retainablePacket.
func (p *retainablePacket) Header() *rtp.Header {
	return p.header
}

// Payload returns the RTP Payload of the retainablePacket.
func (p *retainablePacket) Payload() []byte {
	return p.payload
}

// Retain increases the reference count of the retainablePacket.
func (p *retainablePacket) Retain() error {
	p.countMu.Lock()
	defer p.countMu.Unlock()
	if p.count == 0 {
		// already released
		return errPacketReleased
	}
	p.count++
	return nil
}

// Release decreases the reference count of the retainablePacket and frees if needed.
func (p *retainablePacket) Release() {
	p.countMu.Lock()
	defer p.countMu.Unlock()
	p.count--

	if p.count == 0 {
		// release back to pool
		p.onRelease(p.header, p.buffer)
		p.header = nil
		p.buffer = nil
		p.payload = nil
	}
}

// ============================================================================
// Packet Factory implementation
// ============================================================================

// packetFactory allows custom logic around the handling of RTP Packets before they are added to the rtpBuffer.
type packetFactory interface {
	NewPacket(header *rtp.Header, payload []byte, rtxSSRC uint32, rtxPayloadType uint8) (*retainablePacket, error)
}

// packetFactoryCopy is a packetFactory that takes a copy of packets when added to the rtpBuffer.
type packetFactoryCopy struct {
	headerPool   *sync.Pool
	payloadPool  *sync.Pool
	rtxSequencer rtp.Sequencer
}

// newPacketFactoryCopy constructs a packetFactory that takes a copy of packets when added to the rtpBuffer.
func newPacketFactoryCopy() *packetFactoryCopy {
	return &packetFactoryCopy{
		headerPool: &sync.Pool{
			New: func() any {
				return &rtp.Header{}
			},
		},
		payloadPool: &sync.Pool{
			New: func() any {
				buf := make([]byte, maxPayloadLen)
				return &buf
			},
		},
		rtxSequencer: rtp.NewRandomSequencer(),
	}
}

// NewPacket constructs a new retainablePacket that can be added to the rtpBuffer.
//
//nolint:cyclop
func (m *packetFactoryCopy) NewPacket(
	header *rtp.Header, payload []byte, rtxSSRC uint32, rtxPayloadType uint8,
) (*retainablePacket, error) {
	if len(payload) > maxPayloadLen {
		return nil, io.ErrShortBuffer
	}

	packet := &retainablePacket{
		onRelease:      m.releasePacket,
		sequenceNumber: header.SequenceNumber,
		// new packets have retain count of 1
		count:  1,
		sentAt: time.Now(),
	}

	var ok bool
	packet.header, ok = m.headerPool.Get().(*rtp.Header)
	if !ok {
		return nil, errFailedToCastHeaderPool
	}

	*packet.header = header.Clone()

	if payload != nil {
		packet.buffer, ok = m.payloadPool.Get().(*[]byte)
		if !ok {
			return nil, errFailedToCastPayloadPool
		}
		if rtxSSRC != 0 && rtxPayloadType != 0 {
			size := copy((*packet.buffer)[rtxSSRCByteLength:], payload)
			packet.payload = (*packet.buffer)[:size+rtxSSRCByteLength]
		} else {
			size := copy(*packet.buffer, payload)
			packet.payload = (*packet.buffer)[:size]
		}
	}

	if rtxSSRC != 0 && rtxPayloadType != 0 { //nolint:nestif
		if payload == nil {
			packet.buffer, ok = m.payloadPool.Get().(*[]byte)
			if !ok {
				return nil, errFailedToCastPayloadPool
			}
			packet.payload = (*packet.buffer)[:rtxSSRCByteLength]
		}
		// Write the original sequence number at the beginning of the payload.
		binary.BigEndian.PutUint16(packet.payload, packet.header.SequenceNumber)

		// Rewrite the SSRC.
		packet.header.SSRC = rtxSSRC
		// Rewrite the payload type.
		packet.header.PayloadType = rtxPayloadType
		// Rewrite the sequence number.
		packet.header.SequenceNumber = m.rtxSequencer.NextSequenceNumber()
		// Remove padding if present.
		if packet.header.Padding {
			// Older versions of pion/rtp didn't have the Header.PaddingSize field and as a workaround
			// users had to add padding to the payload. We need to handle this case here.
			if packet.header.PaddingSize == 0 && len(packet.payload) > 0 {
				paddingLength := int(packet.payload[len(packet.payload)-1])
				if paddingLength > len(packet.payload) {
					return nil, errPaddingOverflow
				}
				packet.payload = (*packet.buffer)[:len(packet.payload)-paddingLength]
			}

			packet.header.Padding = false
			packet.header.PaddingSize = 0
		}
	}

	return packet, nil
}

func (m *packetFactoryCopy) releasePacket(header *rtp.Header, payload *[]byte) {
	m.headerPool.Put(header)
	if payload != nil {
		m.payloadPool.Put(payload)
	}
}

// packetFactoryNoOp is a packetFactory implementation that doesn't copy packets.
type packetFactoryNoOp struct{}

// NewPacket constructs a new retainablePacket that can be added to the rtpBuffer.
func (f *packetFactoryNoOp) NewPacket(
	header *rtp.Header, payload []byte, _ uint32, _ uint8,
) (*retainablePacket, error) {
	return &retainablePacket{
		onRelease:      f.releasePacket,
		count:          1,
		header:         header,
		payload:        payload,
		sequenceNumber: header.SequenceNumber,
		sentAt:         time.Now(),
	}, nil
}

func (f *packetFactoryNoOp) releasePacket(_ *rtp.Header, _ *[]byte) {
	// no-op
}
