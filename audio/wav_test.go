package audio

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWAVRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pcm  PCM
	}{
		{"s16 mono", FromInt16([]int16{0, 1, -1, 32767, -32768}, 16000, 1)},
		{"s16 stereo", FromInt16([]int16{1, 2, 3, 4, 5, 6}, 48000, 2)},
		{"f32 mono", FromFloat32([]float32{0, 0.5, -0.5, 1, -1}, 44100, 1)},
		{"f32 stereo", FromFloat32([]float32{0.1, 0.2, 0.3, 0.4}, 8000, 2)},
		{"empty", New(FormatS16, 48000, 1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			require.NoError(t, WriteWAV(&buf, tt.pcm))

			got, err := ReadWAV(&buf)
			require.NoError(t, err)

			require.Equal(t, tt.pcm.Format, got.Format)
			require.Equal(t, tt.pcm.SampleRate, got.SampleRate)
			require.Equal(t, tt.pcm.Channels, got.Channels)
			require.Equal(t, tt.pcm.Len(), got.Len())
			require.Equal(t, tt.pcm.Bytes(), got.Bytes())
		})
	}
}

func TestWAVHeaderIsWellFormed(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	// 100 stereo frames, so 200 interleaved samples and 400 bytes of payload.
	require.NoError(t, WriteWAV(&buf, FromInt16(make([]int16, 200), 16000, 2)))
	raw := buf.Bytes()

	require.Equal(t, "RIFF", string(raw[0:4]))
	require.Equal(t, uint32(len(raw)-8), binary.LittleEndian.Uint32(raw[4:8]))
	require.Equal(t, "WAVE", string(raw[8:12]))
	require.Equal(t, "fmt ", string(raw[12:16]))
	require.Equal(t, uint16(wavFormatPCM), binary.LittleEndian.Uint16(raw[20:22]))
	require.Equal(t, uint16(2), binary.LittleEndian.Uint16(raw[22:24]))
	require.Equal(t, uint32(16000), binary.LittleEndian.Uint32(raw[24:28]))
	// Byte rate is rate * channels * bytes per sample.
	require.Equal(t, uint32(16000*2*2), binary.LittleEndian.Uint32(raw[28:32]))
	require.Equal(t, uint16(4), binary.LittleEndian.Uint16(raw[32:34]))
	require.Equal(t, uint16(16), binary.LittleEndian.Uint16(raw[34:36]))
	require.Equal(t, "data", string(raw[36:40]))
	require.Equal(t, uint32(400), binary.LittleEndian.Uint32(raw[40:44]))
}

func TestReadWAVRejectsNonWAV(t *testing.T) {
	t.Parallel()

	_, err := ReadWAV(bytes.NewReader([]byte("not a wav file at all")))
	require.ErrorIs(t, err, ErrNotWAV)

	_, err = ReadWAV(bytes.NewReader(nil))
	require.ErrorIs(t, err, ErrNotWAV)
}

func TestReadWAVSkipsUnknownChunks(t *testing.T) {
	t.Parallel()

	// Build a file with a LIST chunk between fmt and data, which is what a
	// file tagged by an editor looks like.
	var body bytes.Buffer
	body.WriteString("WAVE")

	fmtChunk := make([]byte, 16)
	binary.LittleEndian.PutUint16(fmtChunk[0:], wavFormatPCM)
	binary.LittleEndian.PutUint16(fmtChunk[2:], 1)
	binary.LittleEndian.PutUint32(fmtChunk[4:], 8000)
	binary.LittleEndian.PutUint32(fmtChunk[8:], 16000)
	binary.LittleEndian.PutUint16(fmtChunk[12:], 2)
	binary.LittleEndian.PutUint16(fmtChunk[14:], 16)
	writeChunk(&body, "fmt ", fmtChunk)
	writeChunk(&body, "LIST", []byte("INFOsome metadata"))
	writeChunk(&body, "data", []byte{0x01, 0x00, 0x02, 0x00})

	var file bytes.Buffer
	file.WriteString("RIFF")
	require.NoError(t, binary.Write(&file, binary.LittleEndian, uint32(body.Len())))
	file.Write(body.Bytes())

	got, err := ReadWAV(&file)
	require.NoError(t, err)
	require.Equal(t, []int16{1, 2}, got.Int16())
}

func TestReadWAVWidensBitDepths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		bits int
		data []byte
		want []int16
	}{
		{
			name: "8-bit unsigned",
			bits: 8,
			data: []byte{128, 255, 0},
			want: []int16{0, 32512, -32768},
		},
		{
			name: "24-bit",
			bits: 24,
			// 0x7FFFFF is full scale positive, 0x800000 full scale negative.
			data: []byte{0xFF, 0xFF, 0x7F, 0x00, 0x00, 0x80},
			want: []int16{32767, -32768},
		},
		{
			name: "32-bit",
			bits: 32,
			data: []byte{0xFF, 0xFF, 0xFF, 0x7F, 0x00, 0x00, 0x00, 0x80},
			want: []int16{32767, -32768},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file := buildWAV(t, wavFormatPCM, tt.bits, 8000, 1, tt.data)
			got, err := ReadWAV(bytes.NewReader(file))
			require.NoError(t, err)
			require.Equal(t, FormatS16, got.Format)
			require.Equal(t, tt.want, got.Int16())
		})
	}
}

func TestReadWAVRejectsUnsupportedEncoding(t *testing.T) {
	t.Parallel()

	// Format tag 6 is A-law, which belongs to the G.711 path, not here.
	file := buildWAV(t, 6, 8, 8000, 1, []byte{0x01})
	_, err := ReadWAV(bytes.NewReader(file))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")
}

// buildWAV assembles a minimal WAV file with an arbitrary format tag and bit
// depth, for exercising readers of files this package cannot write.
func buildWAV(t *testing.T, formatTag uint16, bits, sampleRate, channels int, data []byte) []byte {
	t.Helper()

	blockAlign := channels * bits / 8
	fmtChunk := make([]byte, 16)
	binary.LittleEndian.PutUint16(fmtChunk[0:], formatTag)
	binary.LittleEndian.PutUint16(fmtChunk[2:], uint16(channels))
	binary.LittleEndian.PutUint32(fmtChunk[4:], uint32(sampleRate))
	binary.LittleEndian.PutUint32(fmtChunk[8:], uint32(sampleRate*blockAlign))
	binary.LittleEndian.PutUint16(fmtChunk[12:], uint16(blockAlign))
	binary.LittleEndian.PutUint16(fmtChunk[14:], uint16(bits))

	var body bytes.Buffer
	body.WriteString("WAVE")
	writeChunk(&body, "fmt ", fmtChunk)
	writeChunk(&body, "data", data)

	var file bytes.Buffer
	file.WriteString("RIFF")
	require.NoError(t, binary.Write(&file, binary.LittleEndian, uint32(body.Len())))
	file.Write(body.Bytes())
	return file.Bytes()
}

func TestWAVFileHelpers(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "tone.wav")
	src := sine(16000, 440, 16000, 0.5).ToInt16()

	require.NoError(t, WriteWAVFile(path, src))

	got, err := ReadWAVFile(path)
	require.NoError(t, err)
	require.Equal(t, src.Int16(), got.Int16())
	require.Equal(t, 16000, got.SampleRate)

	_, err = ReadWAVFile(filepath.Join(t.TempDir(), "missing.wav"))
	require.Error(t, err)
}

func TestWAVWriterStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format Format
	}{
		{"s16", FormatS16},
		{"f32", FormatF32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "stream.wav")
			w, err := CreateWAVFile(path, tt.format, 16000, 1)
			require.NoError(t, err)

			// Write in 20 ms pieces, the way a recorder receives them.
			src := sine(16000, 440, 16000, 0.5)
			for chunk := range src.Chunks(320, 0, false) {
				require.NoError(t, w.Write(chunk))
			}
			require.Equal(t, int64(16000), w.Frames())
			require.NoError(t, w.Close())
			require.NoError(t, w.Close(), "Close should be idempotent")

			got, err := ReadWAVFile(path)
			require.NoError(t, err)
			require.Equal(t, tt.format, got.Format)
			require.Equal(t, 16000, got.SampleRate)
			require.Equal(t, 16000, got.Len())
			require.InDelta(t, src.RMS(), got.RMS(), 0.001)

			// The file on disk must be exactly the header plus the payload.
			info, err := os.Stat(path)
			require.NoError(t, err)
			header := int64(44)
			if tt.format == FormatF32 {
				header = 56
			}
			require.Equal(t, header+int64(16000*tt.format.BytesPerSample()), info.Size())
		})
	}
}

func TestWAVWriterConvertsInput(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "converted.wav")
	w, err := CreateWAVFile(path, FormatS16, 16000, 1)
	require.NoError(t, err)

	// Hand it 48 kHz stereo float, the shape a decoder produces.
	src := sine(48000, 440, 48000, 0.5)
	stereo, err := NewResampler(FormatF32, 48000, 2).Resample(src)
	require.NoError(t, err)

	require.NoError(t, w.Write(stereo))
	require.NoError(t, w.Close())

	got, err := ReadWAVFile(path)
	require.NoError(t, err)
	require.Equal(t, 16000, got.SampleRate)
	require.Equal(t, 1, got.Channels)
	require.InDelta(t, 16000, got.Len(), 2)
}

func TestWAVWriterRejectsWriteAfterClose(t *testing.T) {
	t.Parallel()

	w, err := CreateWAVFile(filepath.Join(t.TempDir(), "closed.wav"), FormatS16, 8000, 1)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.Error(t, w.Write(FromInt16([]int16{1}, 8000, 1)))
}

func TestWAVWriterRejectsBadParameters(t *testing.T) {
	t.Parallel()

	_, err := CreateWAVFile(filepath.Join(t.TempDir(), "bad.wav"), FormatS16, 0, 1)
	require.Error(t, err)
}
