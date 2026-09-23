package nack

import (
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

// A retransmission on a separate rtx stream is a new packet as far as the wire
// is concerned: its own ssrc, payload type and sequence number, with the
// original sequence number prepended to the payload so the receiver can put it
// back where it belongs (RFC 4588).
func TestNewPacketRewritesTheHeaderForRetransmission(t *testing.T) {
	t.Parallel()

	factory := newPacketFactoryCopy()
	header := &rtp.Header{SequenceNumber: 1234, SSRC: 5, PayloadType: 96, Timestamp: 90_000}

	pkt, err := factory.NewPacket(header, []byte("frame"), 42, 97)
	require.NoError(t, err)

	require.Equal(t, uint32(42), pkt.Header().SSRC)
	require.Equal(t, uint8(97), pkt.Header().PayloadType)
	require.Equal(t, uint32(90_000), pkt.Header().Timestamp, "the media timestamp is preserved")
	require.Equal(t, uint16(1234), binary.BigEndian.Uint16(pkt.Payload()))
	require.Equal(t, []byte("frame"), pkt.Payload()[rtxSSRCByteLength:])

	require.Equal(t, uint16(1234), header.SequenceNumber, "the caller's header is left alone")
	require.Equal(t, uint32(5), header.SSRC)
}

// Without an rtx stream negotiated the packet is retransmitted as is, on the
// original ssrc and sequence number.
func TestNewPacketWithoutRetransmissionStreamKeepsTheHeader(t *testing.T) {
	t.Parallel()

	factory := newPacketFactoryCopy()
	header := &rtp.Header{SequenceNumber: 1234, SSRC: 5, PayloadType: 96}

	pkt, err := factory.NewPacket(header, []byte("frame"), 0, 0)
	require.NoError(t, err)

	require.Equal(t, uint32(5), pkt.Header().SSRC)
	require.Equal(t, uint8(96), pkt.Header().PayloadType)
	require.Equal(t, uint16(1234), pkt.Header().SequenceNumber)
	require.Equal(t, []byte("frame"), pkt.Payload())
}

func TestNewPacketWithAnEmptyPayloadStillCarriesTheOriginalSequenceNumber(t *testing.T) {
	t.Parallel()

	factory := newPacketFactoryCopy()

	pkt, err := factory.NewPacket(&rtp.Header{SequenceNumber: 7}, nil, 42, 97)
	require.NoError(t, err)
	require.Len(t, pkt.Payload(), rtxSSRCByteLength)
	require.Equal(t, uint16(7), binary.BigEndian.Uint16(pkt.Payload()))
}

// Padding described in the payload rather than the header has to be stripped,
// because the rtx payload gains two bytes and the trailing count would no longer
// describe the packet.
func TestNewPacketStripsPaddingCarriedInThePayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		payload     []byte
		paddingSize uint8
		wantPayload []byte
		wantErr     error
	}{
		{
			// The count in the last byte includes the padding bytes themselves.
			name:        "padding counted by the last byte is removed",
			payload:     []byte{'a', 'b', 0x00, 0x00, 0x03},
			wantPayload: []byte{'a', 'b'},
		},
		{
			name:        "padding the header already accounts for is left in place",
			payload:     []byte{'a', 'b', 0x00, 0x03},
			paddingSize: 3,
			wantPayload: []byte{'a', 'b', 0x00, 0x03},
		},
		{
			name:    "a padding count larger than the payload is rejected",
			payload: []byte{'a', 0xFF},
			wantErr: errPaddingOverflow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			factory := newPacketFactoryCopy()
			header := &rtp.Header{
				SequenceNumber: 1234,
				Padding:        true,
				PaddingSize:    tt.paddingSize,
			}

			pkt, err := factory.NewPacket(header, tt.payload, 42, 97)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.False(t, pkt.Header().Padding, "the rtx packet declares no padding")
			require.Zero(t, pkt.Header().PaddingSize)
			require.Equal(t, tt.wantPayload, pkt.Payload()[rtxSSRCByteLength:])
		})
	}
}

func TestNewPacketRejectsAPayloadLargerThanTheBuffer(t *testing.T) {
	t.Parallel()

	factory := newPacketFactoryCopy()

	_, err := factory.NewPacket(&rtp.Header{}, make([]byte, maxPayloadLen+1), 0, 0)
	require.ErrorIs(t, err, io.ErrShortBuffer)
}

// DisableCopy hands the buffer straight through, for callers that do not reuse
// the memory they wrote from.
func TestDisableCopyKeepsTheCallersBuffer(t *testing.T) {
	t.Parallel()

	factory := &packetFactoryNoOp{}
	header := &rtp.Header{SequenceNumber: 1234}
	payload := []byte("frame")

	pkt, err := factory.NewPacket(header, payload, 42, 97)
	require.NoError(t, err)
	require.Same(t, header, pkt.Header(), "the header is not copied")
	require.Equal(t, payload, pkt.Payload())

	// Releasing a no-op packet must not blow up on the nil pool.
	pkt.Release()
}

// The interceptor answers nacks it reads off the RTCP stream, which is the only
// path that runs in a real peer connection.
func TestBindRTCPReaderResendsNackedPackets(t *testing.T) {
	t.Parallel()

	responder := newResponder(t)
	writer := &mockRTPWriter{}
	info := &interceptor.StreamInfo{
		SSRC:         12345,
		RTCPFeedback: []interceptor.RTCPFeedback{{Type: "nack"}},
	}

	wrapped := responder.BindLocalStream(info, writer)
	_, err := wrapped.Write(&rtp.Header{SequenceNumber: 100, SSRC: 12345}, []byte("frame"), nil)
	require.NoError(t, err)
	writer.clearWritten()

	raw, err := rtcp.Marshal([]rtcp.Packet{&rtcp.TransportLayerNack{
		MediaSSRC: 12345,
		Nacks:     []rtcp.NackPair{{PacketID: 100}},
	}})
	require.NoError(t, err)

	reader := responder.BindRTCPReader(interceptor.RTCPReaderFunc(
		func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
			return copy(b, raw), a, nil
		}))

	buf := make([]byte, 1500)
	n, attrs, err := reader.Read(buf, nil)
	require.NoError(t, err)
	require.Equal(t, len(raw), n)
	require.NotNil(t, attrs)

	require.Eventually(t, func() bool {
		return len(writer.getWritten()) == 1
	}, time.Second, 5*time.Millisecond)
	require.Equal(t, uint16(100), writer.getWritten()[0].SequenceNumber)
}

func TestBindRTCPReaderPassesReadErrorsThrough(t *testing.T) {
	t.Parallel()

	responder := newResponder(t)
	reader := responder.BindRTCPReader(interceptor.RTCPReaderFunc(
		func(_ []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
			return 0, a, io.EOF
		}))

	_, _, err := reader.Read(make([]byte, 1500), nil)
	require.ErrorIs(t, err, io.EOF)
}

// Once a stream is unbound its buffered packets are unreachable, so a late nack
// for it is dropped rather than answered from a stale buffer.
func TestUnbindLocalStreamStopsAnsweringNacks(t *testing.T) {
	t.Parallel()

	responder := newResponder(t)
	writer := &mockRTPWriter{}
	info := &interceptor.StreamInfo{
		SSRC:         12345,
		RTCPFeedback: []interceptor.RTCPFeedback{{Type: "nack"}},
	}

	wrapped := responder.BindLocalStream(info, writer)
	_, err := wrapped.Write(&rtp.Header{SequenceNumber: 100, SSRC: 12345}, []byte("frame"), nil)
	require.NoError(t, err)
	writer.clearWritten()

	responder.UnbindLocalStream(info)
	responder.resendPackets(&rtcp.TransportLayerNack{
		MediaSSRC: 12345,
		Nacks:     []rtcp.NackPair{{PacketID: 100}},
	})
	require.Empty(t, writer.getWritten())
}

// A stream filter decides which tracks get a retransmission buffer at all;
// tracks it rejects are written straight through.
func TestStreamsFilterDecidesWhichStreamsAreBuffered(t *testing.T) {
	t.Parallel()

	factory, err := NewResponderInterceptor(ResponderStreamsFilter(func(info *interceptor.StreamInfo) bool {
		return info.SSRC == 1
	}))
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)
	responder := inter.(*ResponderInterceptor)

	buffered := &interceptor.StreamInfo{SSRC: 1}
	responder.BindLocalStream(buffered, &mockRTPWriter{})
	unbuffered := &interceptor.StreamInfo{SSRC: 2}
	responder.BindLocalStream(unbuffered, &mockRTPWriter{})

	responder.streamsMu.Lock()
	defer responder.streamsMu.Unlock()
	require.Contains(t, responder.streams, uint32(1))
	require.NotContains(t, responder.streams, uint32(2))
}

// The retransmission buffer is a fixed ring, so what it can answer for is
// bounded by its size and by what has already been overwritten.
func TestRTPBufferAnswersOnlyForWhatItStillHolds(t *testing.T) {
	t.Parallel()

	factory := &packetFactoryNoOp{}
	buffer, err := newRTPBuffer(8)
	require.NoError(t, err)

	add := func(seq uint16) {
		pkt, err := factory.NewPacket(&rtp.Header{SequenceNumber: seq}, []byte("frame"), 0, 0)
		require.NoError(t, err)
		buffer.Add(pkt)
	}

	add(100)
	require.NotNil(t, buffer.Get(100))
	require.Nil(t, buffer.Get(101), "nothing has been sent for that sequence number yet")

	// A duplicate does not disturb what is already there.
	add(100)
	require.NotNil(t, buffer.Get(100))

	// A gap clears the slots that were skipped, so a stale packet that would
	// land on one of them by wrapping is not served.
	add(104)
	require.Nil(t, buffer.Get(102))
	require.NotNil(t, buffer.Get(104))

	// Filling the ring past the original packet drops it.
	for seq := uint16(105); seq <= 112; seq++ {
		add(seq)
	}
	require.Nil(t, buffer.Get(100), "the ring has moved on")
	require.NotNil(t, buffer.Get(112))
}

func TestGetSkipsAReleasedPacket(t *testing.T) {
	t.Parallel()

	buffer, err := newRTPBuffer(8)
	require.NoError(t, err)

	pkt, err := (&packetFactoryNoOp{}).NewPacket(&rtp.Header{SequenceNumber: 100}, []byte("frame"), 0, 0)
	require.NoError(t, err)
	buffer.Add(pkt)

	pkt.Release()
	require.Nil(t, buffer.Get(100), "a packet whose last reference is gone cannot be resent")
}

func newResponder(t *testing.T) *ResponderInterceptor {
	t.Helper()

	factory, err := NewResponderInterceptor(
		ResponderSize(512),
		ResponderMinInterval(0),
	)
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	responder, ok := inter.(*ResponderInterceptor)
	require.True(t, ok)
	return responder
}
