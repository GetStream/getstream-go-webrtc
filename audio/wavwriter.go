package audio

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

// Offsets into the header [WriteWAV] emits, which Close seeks back to once the
// payload length is known.
//
//	0  RIFF   4  size   8  WAVE
//	12 "fmt " 16 size   20 16-byte payload
//	36 "fact" 40 size   44 4-byte payload   (float only)
//	.. "data" .. size
const (
	wavRIFFSizeOffset = 4
	wavFactValueAt    = 44
	wavDataSizeS16At  = 40
	wavDataSizeF32At  = 52
)

// WAVWriter appends audio to a WAV file incrementally.
//
// [WriteWAV] has to hold the whole file in memory because the RIFF header
// carries byte counts. A WAVWriter instead writes a placeholder header up front
// and seeks back to patch the counts on [WAVWriter.Close], so recording a call
// that runs for an hour costs nothing but the file.
//
// Buffers passed to [WAVWriter.Write] are converted to the writer's sample rate,
// channel count and format, so a caller can hand it whatever the decoder
// produced. A WAVWriter is not safe for concurrent use.
type WAVWriter struct {
	w         io.WriteSeeker
	closer    io.Closer
	resampler *StreamResampler
	format    Format
	rate      int
	channels  int
	written   int64
	closed    bool
}

// NewWAVWriter starts a WAV file on w and writes its header.
func NewWAVWriter(w io.WriteSeeker, format Format, sampleRate, channels int) (*WAVWriter, error) {
	if err := validate(format, sampleRate, channels); err != nil {
		return nil, err
	}
	// A header describing an empty file; Close rewrites its size fields.
	if err := WriteWAV(w, New(format, sampleRate, channels)); err != nil {
		return nil, err
	}
	return &WAVWriter{
		w:         w,
		resampler: NewStreamResampler(format, sampleRate, channels, 0),
		format:    format,
		rate:      sampleRate,
		channels:  channels,
	}, nil
}

// CreateWAVFile creates a WAV file at path and returns a writer for it. The
// returned writer closes the file on [WAVWriter.Close].
func CreateWAVFile(path string, format Format, sampleRate, channels int) (*WAVWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	ww, err := NewWAVWriter(f, format, sampleRate, channels)
	if err != nil {
		f.Close()
		return nil, err
	}
	ww.closer = f
	return ww, nil
}

// Write appends pcm to the file, converting it as needed.
func (w *WAVWriter) Write(pcm PCM) error {
	if w.closed {
		return fmt.Errorf("audio: write to closed WAV writer")
	}
	frames, err := w.resampler.Write(pcm)
	if err != nil {
		return err
	}
	return w.emit(frames)
}

func (w *WAVWriter) emit(frames []PCM) error {
	for _, f := range frames {
		if f.IsEmpty() {
			continue
		}
		if _, err := w.w.Write(f.Bytes()); err != nil {
			return err
		}
		w.written += int64(f.NumSamples() * w.format.BytesPerSample())
	}
	return nil
}

// Frames is the number of frames written so far.
func (w *WAVWriter) Frames() int64 {
	return w.written / int64(w.format.BytesPerSample()*w.channels)
}

// Duration is how much audio has been written so far.
func (w *WAVWriter) Duration() time.Duration {
	return time.Duration(w.Frames()) * time.Second / time.Duration(w.rate)
}

// Close flushes the resampler, patches the RIFF sizes and closes the underlying
// file if this writer opened it. It is safe to call twice.
func (w *WAVWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	tail, err := w.resampler.Flush()
	if err == nil {
		err = w.emit(tail)
	}
	if err == nil {
		err = w.patchSizes()
	}
	if w.closer != nil {
		if cerr := w.closer.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

func (w *WAVWriter) patchSizes() error {
	if w.written%2 == 1 {
		if _, err := w.w.Write([]byte{0}); err != nil { // RIFF word alignment
			return err
		}
	}

	dataSizeAt, headerBytes := int64(wavDataSizeS16At), int64(44)
	if w.format == FormatF32 {
		dataSizeAt, headerBytes = wavDataSizeF32At, 56
	}
	padded := w.written + w.written%2

	if err := w.patch(wavRIFFSizeOffset, uint32(headerBytes-8+padded)); err != nil {
		return err
	}
	if err := w.patch(dataSizeAt, uint32(w.written)); err != nil {
		return err
	}
	if w.format == FormatF32 {
		if err := w.patch(wavFactValueAt, uint32(w.Frames())); err != nil {
			return err
		}
	}

	_, err := w.w.Seek(0, io.SeekEnd)
	return err
}

func (w *WAVWriter) patch(offset int64, value uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	if _, err := w.w.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	_, err := w.w.Write(buf[:])
	return err
}
