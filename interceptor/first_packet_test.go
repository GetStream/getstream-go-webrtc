package interceptor

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

func TestFirstPacketReportsTheFirstWriteAndReadOnce(t *testing.T) {
	var writes, reads atomic.Int32
	var wroteAt atomic.Int64
	factory := NewFirstPacketFactory(func(at time.Time) {
		writes.Add(1)
		wroteAt.Store(at.UnixNano())
	}, func(time.Time) { reads.Add(1) })
	built, err := factory.NewInterceptor("")
	require.NoError(t, err)

	writer := built.BindLocalStream(&interceptor.StreamInfo{}, interceptor.RTPWriterFunc(
		func(_ *rtp.Header, payload []byte, _ interceptor.Attributes) (int, error) { return len(payload), nil }))
	reader := built.BindRemoteStream(&interceptor.StreamInfo{}, interceptor.RTPReaderFunc(
		func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) { return len(b), a, nil }))

	before := time.Now()
	for range 3 {
		_, err := writer.Write(&rtp.Header{}, []byte{1}, nil)
		require.NoError(t, err)
		_, _, err = reader.Read(make([]byte, 4), nil)
		require.NoError(t, err)
	}

	require.Equal(t, int32(1), writes.Load(), "only the first write is reported")
	require.Equal(t, int32(1), reads.Load(), "only the first read is reported")
	require.False(t, time.Unix(0, wroteAt.Load()).Before(before))
}

func TestFirstPacketLeavesStreamsAloneWithoutACallback(t *testing.T) {
	built, err := NewFirstPacketFactory(nil, nil).NewInterceptor("")
	require.NoError(t, err)

	writer := interceptor.RTPWriterFunc(func(*rtp.Header, []byte, interceptor.Attributes) (int, error) { return 7, nil })
	n, err := built.BindLocalStream(&interceptor.StreamInfo{}, writer).Write(&rtp.Header{}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 7, n)
}

func TestFirstPacketDoesNotReportAFailedWrite(t *testing.T) {
	var writes atomic.Int32
	built, err := NewFirstPacketFactory(func(time.Time) { writes.Add(1) }, nil).NewInterceptor("")
	require.NoError(t, err)

	failing := interceptor.RTPWriterFunc(func(*rtp.Header, []byte, interceptor.Attributes) (int, error) {
		return 0, errWriteFailed
	})
	_, err = built.BindLocalStream(&interceptor.StreamInfo{}, failing).Write(&rtp.Header{}, nil, nil)
	require.ErrorIs(t, err, errWriteFailed)
	require.Zero(t, writes.Load())
}

func TestFirstPacketDoesNotReportAPacketDroppedBeforeTheTransportIsReady(t *testing.T) {
	var writes atomic.Int32
	built, err := NewFirstPacketFactory(func(time.Time) { writes.Add(1) }, nil).NewInterceptor("")
	require.NoError(t, err)
	ready := false
	writer := built.BindLocalStream(&interceptor.StreamInfo{}, interceptor.RTPWriterFunc(
		func(_ *rtp.Header, payload []byte, _ interceptor.Attributes) (int, error) {
			if !ready {
				return 0, nil
			}
			return len(payload), nil
		}))

	_, err = writer.Write(&rtp.Header{}, []byte{1}, nil)
	require.NoError(t, err)
	require.Zero(t, writes.Load(), "pion drops packets before DTLS finishes and reports zero bytes")

	ready = true
	_, err = writer.Write(&rtp.Header{}, []byte{1}, nil)
	require.NoError(t, err)
	require.Equal(t, int32(1), writes.Load())
}

var errWriteFailed = &writeError{}

type writeError struct{}

func (*writeError) Error() string { return "write failed" }
