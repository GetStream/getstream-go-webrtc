package interceptor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test configuration with short durations for fast tests
var testConfig = RTXProberConfig{
	BatchSize:    3,
	Interval:     10 * time.Millisecond,
	Duration:     50 * time.Millisecond,
	InitialDelay: 5 * time.Millisecond,
}

type mockRTPWriter struct {
	packets []*rtp.Header
	mu      sync.Mutex
}

func (m *mockRTPWriter) Write(header *rtp.Header, payload []byte, attrs interceptor.Attributes) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Clone the header to avoid data races
	clone := header.Clone()
	m.packets = append(m.packets, &clone)
	return len(payload), nil
}

func (m *mockRTPWriter) getPackets() []*rtp.Header {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.packets
}

func TestRTXProber_SkipsNonRTXStreams(t *testing.T) {
	factory := NewRTXProberFactoryWithConfig(testConfig)
	i, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	mockWriter := &mockRTPWriter{}

	// Stream without RTX
	info := &interceptor.StreamInfo{
		SSRC:                      12345,
		SSRCRetransmission:        0, // No RTX
		PayloadTypeRetransmission: 0,
		MimeType:                  "video/vp8",
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.SDESMidURI, ID: 1},
			{URI: sdp.SDESRTPStreamIDURI, ID: 2},
			{URI: sdp.SDESRepairRTPStreamIDURI, ID: 3},
		},
	}

	writer := i.BindLocalStream(info, mockWriter)

	// Should return the original writer (no wrapping)
	assert.Equal(t, mockWriter, writer)
}

func TestRTXProber_WrapsVideoStreamWithRTX(t *testing.T) {
	factory := NewRTXProberFactoryWithConfig(testConfig)
	i, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	mockWriter := &mockRTPWriter{}

	// Video stream with RTX
	info := &interceptor.StreamInfo{
		SSRC:                      12345,
		SSRCRetransmission:        12346,
		PayloadTypeRetransmission: 97,
		MimeType:                  "video/vp8",
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.SDESMidURI, ID: 1},
			{URI: sdp.SDESRTPStreamIDURI, ID: 2},
			{URI: sdp.SDESRepairRTPStreamIDURI, ID: 3},
		},
	}

	writer := i.BindLocalStream(info, mockWriter)

	// Should return a wrapped writer
	assert.NotEqual(t, mockWriter, writer)
	_, ok := writer.(*rtxProberWriter)
	assert.True(t, ok)
}

func TestRTXProber_SendsProbePackets(t *testing.T) {
	factory := NewRTXProberFactoryWithConfig(testConfig)
	i, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	mockWriter := &mockRTPWriter{}

	baseSSRC := uint32(12345)
	rtxSSRC := uint32(12346)
	rtxPT := uint8(97)

	info := &interceptor.StreamInfo{
		SSRC:                      baseSSRC,
		SSRCRetransmission:        rtxSSRC,
		PayloadTypeRetransmission: rtxPT,
		MimeType:                  "video/vp8",
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.SDESMidURI, ID: 1},
			{URI: sdp.SDESRTPStreamIDURI, ID: 2},
			{URI: sdp.SDESRepairRTPStreamIDURI, ID: 3},
		},
	}

	writer := i.BindLocalStream(info, mockWriter)

	// Send a packet to trigger probing
	header := &rtp.Header{
		Version:          2,
		PayloadType:      96,
		SequenceNumber:   1,
		Timestamp:        0,
		SSRC:             baseSSRC,
		ExtensionProfile: 0xBEDE,
	}
	// Set mid and rid extensions
	require.NoError(t, header.SetExtension(1, []byte("0")))
	require.NoError(t, header.SetExtension(2, []byte("f")))

	_, err = writer.Write(header, []byte{0x00}, nil)
	require.NoError(t, err)

	// Wait for probing to complete (initial delay + duration + buffer)
	time.Sleep(testConfig.InitialDelay + testConfig.Duration + 20*time.Millisecond)

	// Check that probe packets were sent
	packets := mockWriter.getPackets()

	// Find probe packets (those with RTX SSRC)
	var probePackets []*rtp.Header
	for _, pkt := range packets {
		if pkt.SSRC == rtxSSRC {
			probePackets = append(probePackets, pkt)
		}
	}

	// Calculate expected probe count: batches * batchSize
	// batches = duration / interval = 50ms / 10ms = 5 batches
	expectedBatches := int(testConfig.Duration / testConfig.Interval)
	expectedProbes := expectedBatches * testConfig.BatchSize

	// Should have approximately the expected number of probes
	assert.InDelta(t, expectedProbes, len(probePackets), float64(testConfig.BatchSize))

	// Verify probe packet properties
	for _, pkt := range probePackets {
		assert.Equal(t, rtxSSRC, pkt.SSRC)
		assert.Equal(t, rtxPT, pkt.PayloadType)

		// Check mid extension
		midExt := pkt.GetExtension(1)
		assert.NotNil(t, midExt)
		assert.Equal(t, "0", string(midExt))

		// Check rid extension is NOT set (RTX is a repair stream, not a base stream)
		ridExt := pkt.GetExtension(2)
		assert.Nil(t, ridExt)

		// Check rsid extension (should match rid from base packet)
		rsidExt := pkt.GetExtension(3)
		assert.NotNil(t, rsidExt)
		assert.Equal(t, "f", string(rsidExt))
	}
}

func TestRTXProber_ProbesOnlyOnce(t *testing.T) {
	factory := NewRTXProberFactoryWithConfig(testConfig)
	i, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	mockWriter := &mockRTPWriter{}

	info := &interceptor.StreamInfo{
		SSRC:                      12345,
		SSRCRetransmission:        12346,
		PayloadTypeRetransmission: 97,
		MimeType:                  "video/vp8",
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.SDESMidURI, ID: 1},
			{URI: sdp.SDESRTPStreamIDURI, ID: 2},
			{URI: sdp.SDESRepairRTPStreamIDURI, ID: 3},
		},
	}

	writer := i.BindLocalStream(info, mockWriter)

	header := &rtp.Header{
		Version:          2,
		PayloadType:      96,
		SequenceNumber:   1,
		SSRC:             12345,
		ExtensionProfile: 0xBEDE,
	}
	require.NoError(t, header.SetExtension(1, []byte("0")))
	require.NoError(t, header.SetExtension(2, []byte("f")))

	// Send multiple packets rapidly
	for j := range 10 {
		header.SequenceNumber = uint16(j)
		_, err = writer.Write(header, []byte{0x00}, nil)
		require.NoError(t, err)
	}

	// Wait for probing to complete
	time.Sleep(testConfig.InitialDelay + testConfig.Duration + 20*time.Millisecond)

	packets := mockWriter.getPackets()

	// Count RTX probe packets
	var probeCount int
	for _, pkt := range packets {
		if pkt.SSRC == 12346 {
			probeCount++
		}
	}

	// Calculate expected probe count from a single probing session
	expectedBatches := int(testConfig.Duration / testConfig.Interval)
	expectedProbes := expectedBatches * testConfig.BatchSize

	// Should have approximately the expected number (allow some tolerance for timing)
	assert.InDelta(t, expectedProbes, probeCount, float64(testConfig.BatchSize))
}

func TestRTXProber_SkipsNonSimulcastWithoutRID(t *testing.T) {
	factory := NewRTXProberFactoryWithConfig(testConfig)
	i, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	mockWriter := &mockRTPWriter{}

	info := &interceptor.StreamInfo{
		SSRC:                      12345,
		SSRCRetransmission:        12346,
		PayloadTypeRetransmission: 97,
		MimeType:                  "video/vp8",
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.SDESMidURI, ID: 1},
			{URI: sdp.SDESRTPStreamIDURI, ID: 2},
			{URI: sdp.SDESRepairRTPStreamIDURI, ID: 3},
		},
	}

	writer := i.BindLocalStream(info, mockWriter)

	// Send a packet WITHOUT rid extension (non-simulcast case)
	header := &rtp.Header{
		Version:          2,
		PayloadType:      96,
		SequenceNumber:   1,
		SSRC:             12345,
		ExtensionProfile: 0xBEDE,
	}
	// Only set mid, no rid (simulating non-simulcast)
	require.NoError(t, header.SetExtension(1, []byte("0")))
	// Note: NOT setting rid extension

	_, err = writer.Write(header, []byte{0x00}, nil)
	require.NoError(t, err)

	// Wait for probing to complete (if it were to run)
	time.Sleep(testConfig.InitialDelay + testConfig.Duration + 20*time.Millisecond)

	packets := mockWriter.getPackets()

	// Count RTX probe packets
	var probeCount int
	for _, pkt := range packets {
		if pkt.SSRC == 12346 {
			probeCount++
		}
	}

	// Should have NO probe packets for non-simulcast streams without rid
	assert.Equal(t, 0, probeCount)
}

func TestRTXProber_ConcurrentAccess(t *testing.T) {
	factory := NewRTXProberFactoryWithConfig(testConfig)
	i, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	mockWriter := &mockRTPWriter{}

	info := &interceptor.StreamInfo{
		SSRC:                      12345,
		SSRCRetransmission:        12346,
		PayloadTypeRetransmission: 97,
		MimeType:                  "video/vp8",
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.SDESMidURI, ID: 1},
			{URI: sdp.SDESRTPStreamIDURI, ID: 2},
			{URI: sdp.SDESRepairRTPStreamIDURI, ID: 3},
		},
	}

	writer := i.BindLocalStream(info, mockWriter)

	header := &rtp.Header{
		Version:          2,
		PayloadType:      96,
		SequenceNumber:   1,
		SSRC:             12345,
		ExtensionProfile: 0xBEDE,
	}
	require.NoError(t, header.SetExtension(1, []byte("0")))
	require.NoError(t, header.SetExtension(2, []byte("f")))

	// Concurrent writes
	var wg sync.WaitGroup
	var writeCount atomic.Int32

	for j := range 100 {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			h := header.Clone()
			h.SequenceNumber = uint16(seq)
			_, err := writer.Write(&h, []byte{0x00}, nil)
			if err == nil {
				writeCount.Add(1)
			}
		}(j)
	}

	wg.Wait()

	// All writes should succeed
	assert.Equal(t, int32(100), writeCount.Load())

	// Wait for probing to complete
	time.Sleep(testConfig.InitialDelay + testConfig.Duration + 20*time.Millisecond)

	packets := mockWriter.getPackets()

	// Count probe packets
	var probeCount int
	for _, pkt := range packets {
		if pkt.SSRC == 12346 {
			probeCount++
		}
	}

	// Should have probe packets from only one probing session
	expectedBatches := int(testConfig.Duration / testConfig.Interval)
	expectedProbes := expectedBatches * testConfig.BatchSize
	assert.InDelta(t, expectedProbes, probeCount, float64(testConfig.BatchSize))
}

func TestRTXProber_DefaultConfig(t *testing.T) {
	factory := NewRTXProberFactory()

	// Verify default config values
	assert.Equal(t, DefaultRTXProbeBatchSize, factory.config.BatchSize)
	assert.Equal(t, DefaultRTXProbeInterval, factory.config.Interval)
	assert.Equal(t, DefaultRTXProbeDuration, factory.config.Duration)
	assert.Equal(t, DefaultRTXProbeInitialDelay, factory.config.InitialDelay)
}

func TestRTXProber_CustomConfig(t *testing.T) {
	customConfig := RTXProberConfig{
		BatchSize:    10,
		Interval:     200 * time.Millisecond,
		Duration:     2 * time.Second,
		InitialDelay: 100 * time.Millisecond,
	}

	factory := NewRTXProberFactoryWithConfig(customConfig)

	assert.Equal(t, 10, factory.config.BatchSize)
	assert.Equal(t, 200*time.Millisecond, factory.config.Interval)
	assert.Equal(t, 2*time.Second, factory.config.Duration)
	assert.Equal(t, 100*time.Millisecond, factory.config.InitialDelay)
}

func TestRTXProber_ConfigDefaultsForZeroValues(t *testing.T) {
	// Config with all zero values should get defaults
	factory := NewRTXProberFactoryWithConfig(RTXProberConfig{})

	assert.Equal(t, DefaultRTXProbeBatchSize, factory.config.BatchSize)
	assert.Equal(t, DefaultRTXProbeInterval, factory.config.Interval)
	assert.Equal(t, DefaultRTXProbeDuration, factory.config.Duration)
	assert.Equal(t, DefaultRTXProbeInitialDelay, factory.config.InitialDelay)
}
