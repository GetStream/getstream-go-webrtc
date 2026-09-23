package nack

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

type mockRTPWriter struct {
	mu      sync.Mutex
	written []rtp.Header
}

func (m *mockRTPWriter) Write(header *rtp.Header, payload []byte, attrs interceptor.Attributes) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.written = append(m.written, *header)
	return len(payload), nil
}

func (m *mockRTPWriter) getWritten() []rtp.Header {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]rtp.Header, len(m.written))
	copy(result, m.written)
	return result
}

func (m *mockRTPWriter) clearWritten() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.written = nil
}

func TestResponderInterceptor_MaxRetries(t *testing.T) {
	factory, err := NewResponderInterceptor(
		ResponderSize(512),
		ResponderMaxRetries(3),
		ResponderMinInterval(0),
		ResponderMaxAge(10*time.Second),
	)
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	resp := inter.(*ResponderInterceptor)

	writer := &mockRTPWriter{}
	info := &interceptor.StreamInfo{
		SSRC: 12345,
		RTCPFeedback: []interceptor.RTCPFeedback{
			{Type: "nack"},
		},
	}

	wrappedWriter := resp.BindLocalStream(info, writer)

	// Write a packet
	header := &rtp.Header{SequenceNumber: 100, SSRC: 12345}
	_, err = wrappedWriter.Write(header, []byte{1, 2, 3}, nil)
	require.NoError(t, err)

	// Clear writer to track only retransmissions
	writer.clearWritten()

	// Create NACKs - first 3 should succeed, 4th should be blocked
	for i := 0; i < 4; i++ {
		nackPkt := &rtcp.TransportLayerNack{
			MediaSSRC: 12345,
			Nacks:     []rtcp.NackPair{{PacketID: 100, LostPackets: 0}},
		}
		resp.resendPackets(nackPkt)
	}

	// Should have exactly 3 written packets (max retries)
	written := writer.getWritten()
	require.Len(t, written, 3)
}

func TestResponderInterceptor_MinInterval(t *testing.T) {
	factory, err := NewResponderInterceptor(
		ResponderSize(512),
		ResponderMaxRetries(10),
		ResponderMinInterval(50*time.Millisecond),
		ResponderMaxAge(10*time.Second),
	)
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	resp := inter.(*ResponderInterceptor)

	writer := &mockRTPWriter{}
	info := &interceptor.StreamInfo{
		SSRC: 12345,
		RTCPFeedback: []interceptor.RTCPFeedback{
			{Type: "nack"},
		},
	}

	wrappedWriter := resp.BindLocalStream(info, writer)

	// Write a packet
	header := &rtp.Header{SequenceNumber: 100, SSRC: 12345}
	_, err = wrappedWriter.Write(header, []byte{1, 2, 3}, nil)
	require.NoError(t, err)

	// Clear writer to track only retransmissions
	writer.clearWritten()

	// First resend should succeed
	nackPkt := &rtcp.TransportLayerNack{
		MediaSSRC: 12345,
		Nacks:     []rtcp.NackPair{{PacketID: 100, LostPackets: 0}},
	}
	resp.resendPackets(nackPkt)

	// Immediate second resend should be blocked (min interval)
	resp.resendPackets(nackPkt)

	// Should have only 1 written packet
	written := writer.getWritten()
	require.Len(t, written, 1)

	// Wait for min interval
	time.Sleep(60 * time.Millisecond)

	// Now resend should succeed
	resp.resendPackets(nackPkt)

	written = writer.getWritten()
	require.Len(t, written, 2)
}

func TestResponderInterceptor_MaxAge(t *testing.T) {
	factory, err := NewResponderInterceptor(
		ResponderSize(512),
		ResponderMaxRetries(10),
		ResponderMinInterval(0),
		ResponderMaxAge(50*time.Millisecond),
	)
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	resp := inter.(*ResponderInterceptor)

	writer := &mockRTPWriter{}
	info := &interceptor.StreamInfo{
		SSRC: 12345,
		RTCPFeedback: []interceptor.RTCPFeedback{
			{Type: "nack"},
		},
	}

	wrappedWriter := resp.BindLocalStream(info, writer)

	// Write a packet
	header := &rtp.Header{SequenceNumber: 100, SSRC: 12345}
	_, err = wrappedWriter.Write(header, []byte{1, 2, 3}, nil)
	require.NoError(t, err)

	// Clear writer to track only retransmissions
	writer.clearWritten()

	// Wait for packet to expire
	time.Sleep(60 * time.Millisecond)

	// Resend should be blocked (packet too old)
	nackPkt := &rtcp.TransportLayerNack{
		MediaSSRC: 12345,
		Nacks:     []rtcp.NackPair{{PacketID: 100, LostPackets: 0}},
	}
	resp.resendPackets(nackPkt)

	// Should have no written packets
	written := writer.getWritten()
	require.Len(t, written, 0)
}

func TestResponderInterceptor_UnknownSequence(t *testing.T) {
	factory, err := NewResponderInterceptor(
		ResponderSize(512),
		ResponderMaxRetries(3),
		ResponderMinInterval(0),
		ResponderMaxAge(10*time.Second),
	)
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	resp := inter.(*ResponderInterceptor)

	writer := &mockRTPWriter{}
	info := &interceptor.StreamInfo{
		SSRC: 12345,
		RTCPFeedback: []interceptor.RTCPFeedback{
			{Type: "nack"},
		},
	}

	resp.BindLocalStream(info, writer)

	// Try to resend a packet that was never added
	nackPkt := &rtcp.TransportLayerNack{
		MediaSSRC: 12345,
		Nacks:     []rtcp.NackPair{{PacketID: 999, LostPackets: 0}},
	}
	resp.resendPackets(nackPkt)

	// Should have no written packets
	written := writer.getWritten()
	require.Len(t, written, 0)
}

func TestResponderInterceptor_BufferOverwrite(t *testing.T) {
	// Use size 4 (power of 2)
	factory, err := NewResponderInterceptor(
		ResponderSize(4),
		ResponderMaxRetries(10),
		ResponderMinInterval(0),
		ResponderMaxAge(10*time.Second),
	)
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	resp := inter.(*ResponderInterceptor)

	writer := &mockRTPWriter{}
	info := &interceptor.StreamInfo{
		SSRC: 12345,
		RTCPFeedback: []interceptor.RTCPFeedback{
			{Type: "nack"},
		},
	}

	wrappedWriter := resp.BindLocalStream(info, writer)

	// Add 4 packets (the buffer size)
	for i := uint16(100); i < 104; i++ {
		header := &rtp.Header{SequenceNumber: i, SSRC: 12345}
		_, err = wrappedWriter.Write(header, []byte{byte(i)}, nil)
		require.NoError(t, err)
	}

	// Clear writer
	writer.clearWritten()

	// All 4 should be resendable
	for i := uint16(100); i < 104; i++ {
		nackPkt := &rtcp.TransportLayerNack{
			MediaSSRC: 12345,
			Nacks:     []rtcp.NackPair{{PacketID: i, LostPackets: 0}},
		}
		resp.resendPackets(nackPkt)
	}
	require.Len(t, writer.getWritten(), 4)

	// Add 5th packet, which overwrites first (100) since buffer size is 4
	header := &rtp.Header{SequenceNumber: 104, SSRC: 12345}
	_, err = wrappedWriter.Write(header, []byte{104}, nil)
	require.NoError(t, err)

	// Clear writer
	writer.clearWritten()

	// Try to resend packet 100 (should fail - overwritten)
	nackPkt := &rtcp.TransportLayerNack{
		MediaSSRC: 12345,
		Nacks:     []rtcp.NackPair{{PacketID: 100, LostPackets: 0}},
	}
	resp.resendPackets(nackPkt)
	require.Len(t, writer.getWritten(), 0)

	// Packet 104 should be resendable
	nackPkt = &rtcp.TransportLayerNack{
		MediaSSRC: 12345,
		Nacks:     []rtcp.NackPair{{PacketID: 104, LostPackets: 0}},
	}
	resp.resendPackets(nackPkt)
	require.Len(t, writer.getWritten(), 1)
	require.Equal(t, uint16(104), writer.getWritten()[0].SequenceNumber)
}

func TestResponderInterceptorFactory(t *testing.T) {
	factory, err := NewResponderInterceptor(
		ResponderSize(256),
		ResponderMaxRetries(5),
		ResponderMinInterval(100*time.Millisecond),
		ResponderMaxAge(5*time.Second),
	)
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)
	require.NotNil(t, inter)

	resp, ok := inter.(*ResponderInterceptor)
	require.True(t, ok)
	require.Equal(t, uint16(256), resp.size)
	require.Equal(t, uint8(5), resp.maxRetries)
	require.Equal(t, 100*time.Millisecond, resp.minInterval)
	require.Equal(t, 5*time.Second, resp.maxAge)
}

func TestResponderInterceptor_BindLocalStream(t *testing.T) {
	factory, err := NewResponderInterceptor()
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	resp := inter.(*ResponderInterceptor)

	writer := &mockRTPWriter{}
	info := &interceptor.StreamInfo{
		SSRC: 12345,
		RTCPFeedback: []interceptor.RTCPFeedback{
			{Type: "nack"},
		},
	}

	wrappedWriter := resp.BindLocalStream(info, writer)
	require.NotNil(t, wrappedWriter)

	// Write a packet
	header := &rtp.Header{SequenceNumber: 100, SSRC: 12345}
	_, err = wrappedWriter.Write(header, []byte{1, 2, 3}, nil)
	require.NoError(t, err)

	// Verify stream was created
	resp.streamsMu.Lock()
	stream, ok := resp.streams[12345]
	resp.streamsMu.Unlock()
	require.True(t, ok)
	require.NotNil(t, stream)
}

func TestResponderInterceptor_ProcessNACK(t *testing.T) {
	factory, err := NewResponderInterceptor(
		ResponderMinInterval(0),
	)
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	resp := inter.(*ResponderInterceptor)

	writer := &mockRTPWriter{}
	info := &interceptor.StreamInfo{
		SSRC: 12345,
		RTCPFeedback: []interceptor.RTCPFeedback{
			{Type: "nack"},
		},
	}

	wrappedWriter := resp.BindLocalStream(info, writer)

	// Write some packets
	for i := uint16(100); i < 110; i++ {
		header := &rtp.Header{SequenceNumber: i, SSRC: 12345}
		_, err = wrappedWriter.Write(header, []byte{byte(i)}, nil)
		require.NoError(t, err)
	}

	// Clear writer to track only retransmissions
	writer.clearWritten()

	// Create a NACK for packets 102, 105, 108
	nackPkt := &rtcp.TransportLayerNack{
		MediaSSRC: 12345,
		Nacks: []rtcp.NackPair{
			{PacketID: 102, LostPackets: 0}, // Just packet 102
			{PacketID: 105, LostPackets: 0}, // Just packet 105
			{PacketID: 108, LostPackets: 0}, // Just packet 108
		},
	}

	resp.resendPackets(nackPkt)

	written := writer.getWritten()
	require.Len(t, written, 3)

	// Check that the correct packets were retransmitted
	seqNums := make(map[uint16]bool)
	for _, h := range written {
		seqNums[h.SequenceNumber] = true
	}
	require.True(t, seqNums[102])
	require.True(t, seqNums[105])
	require.True(t, seqNums[108])
}

func TestResponderInterceptor_InvalidSize(t *testing.T) {
	factory, err := NewResponderInterceptor(
		ResponderSize(500), // Not a power of 2
	)
	require.NoError(t, err)

	_, err = factory.NewInterceptor("test")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidSize)
}

func TestResponderInterceptor_NoNackFeedback(t *testing.T) {
	factory, err := NewResponderInterceptor()
	require.NoError(t, err)

	inter, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	resp := inter.(*ResponderInterceptor)

	writer := &mockRTPWriter{}
	info := &interceptor.StreamInfo{
		SSRC: 12345,
		RTCPFeedback: []interceptor.RTCPFeedback{
			{Type: "pli"}, // Not nack
		},
	}

	// Should return the original writer since nack feedback is not supported
	wrappedWriter := resp.BindLocalStream(info, writer)

	// The writer should be the same (not wrapped)
	// We can verify by checking no stream was created
	resp.streamsMu.Lock()
	_, ok := resp.streams[12345]
	resp.streamsMu.Unlock()
	require.False(t, ok)

	// Write should still work (pass through)
	header := &rtp.Header{SequenceNumber: 100, SSRC: 12345}
	_, err = wrappedWriter.Write(header, []byte{1, 2, 3}, nil)
	require.NoError(t, err)
	require.Len(t, writer.getWritten(), 1)
}
