package resample

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readAll drains the planarReader through buffers of the given size and
// returns every byte it produced.
func readAll(t *testing.T, pr *planarReader, bufSize int) []byte {
	t.Helper()
	var out bytes.Buffer
	buf := make([]byte, bufSize)
	for {
		n, err := pr.Read(buf)
		// Data and error must never be reported together.
		if n > 0 {
			require.NoError(t, err)
		}
		out.Write(buf[:n])
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			return out.Bytes()
		}
	}
}

func TestPlanarReaderInterleaving(t *testing.T) {
	t.Parallel()

	// int16, 2 channels: L0 R0 L1 R1 ...
	cfg := planarConfig{chunkSize: 4}
	pr := newPlanarReader(
		[]io.Reader{
			bytes.NewReader([]byte{0x01, 0x00, 0x03, 0x00, 0x05, 0x00}),
			bytes.NewReader([]byte{0x02, 0x00, 0x04, 0x00, 0x06, 0x00}),
		},
		2, 2, cfg,
	)

	got := readAll(t, pr, 4)
	assert.Equal(t, []byte{
		0x01, 0x00, 0x02, 0x00,
		0x03, 0x00, 0x04, 0x00,
		0x05, 0x00, 0x06, 0x00,
	}, got)
	assert.Equal(t, int64(12), pr.totalRead)
}

// A sample straddling two reader chunks (chunk size not a multiple of
// elemSize) must be reassembled transparently.
func TestPlanarReaderSampleAcrossChunks(t *testing.T) {
	t.Parallel()

	left := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	right := []byte{11, 12, 13, 14, 15, 16, 17, 18}
	cfg := planarConfig{chunkSize: 5} // not a multiple of elemSize 4
	pr := newPlanarReader(
		[]io.Reader{bytes.NewReader(left), bytes.NewReader(right)},
		4, 2, cfg,
	)

	got := readAll(t, pr, 7) // odd buffer size: frames get split via leftover
	expected := make([]byte, 0, 16)
	for f := range 2 {
		expected = append(expected, left[f*4:(f+1)*4]...)
		expected = append(expected, right[f*4:(f+1)*4]...)
	}
	assert.Equal(t, expected, got)
}

func TestPlanarReaderEmpty(t *testing.T) {
	t.Parallel()

	cfg := planarConfig{chunkSize: 8}
	pr := newPlanarReader(
		[]io.Reader{bytes.NewReader(nil), bytes.NewReader(nil)},
		2, 2, cfg,
	)

	n, err := pr.Read(make([]byte, 16))
	assert.Zero(t, n)
	require.ErrorIs(t, err, io.EOF)
	assert.Zero(t, pr.totalRead)
}

func TestPlanarReaderMemoryBound(t *testing.T) {
	t.Parallel()

	const chunkSize = 64
	const ch = 2
	const elemSize = 4
	const frames = 10000

	readers := make([]io.Reader, ch)
	for i := range ch {
		data := make([]byte, frames*elemSize)
		for j := range data {
			data[j] = byte(i + j)
		}
		readers[i] = bytes.NewReader(data)
	}
	pr := newPlanarReader(readers, elemSize, ch, planarConfig{chunkSize: chunkSize})

	// Memory used by the adapter must be independent of the stream length.
	adapterCap := cap(pr.frameBuf) + cap(pr.leftover)
	for _, cs := range pr.channels {
		adapterCap += cap(cs.buf)
	}
	expectedBound := ch*chunkSize + 2*ch*elemSize
	assert.LessOrEqual(t, adapterCap, expectedBound)

	got := readAll(t, pr, 33)
	assert.Len(t, got, frames*ch*elemSize)

	// Buffers never grew beyond their initial capacity.
	for _, cs := range pr.channels {
		assert.LessOrEqual(t, cap(cs.buf), chunkSize)
	}
	assert.LessOrEqual(t, cap(pr.leftover), ch*elemSize)
}

func TestPlanarReaderTornSample(t *testing.T) {
	t.Parallel()

	pr := newPlanarReader(
		[]io.Reader{
			bytes.NewReader([]byte{1, 2, 3, 4}),
			bytes.NewReader([]byte{5, 6, 7}), // 3 bytes, elemSize 4
		},
		4, 2, planarConfig{chunkSize: 4},
	)

	_, err := io.ReadAll(pr)
	require.ErrorIs(t, err, ErrPartialSample)
	assert.Equal(t, int64(7), pr.totalRead)
}

func TestPlanarReaderUnequalAndPadding(t *testing.T) {
	t.Parallel()

	// 3 frames on channel 0, 1 frame on channel 1.
	ch0 := []byte{1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0}
	ch1 := []byte{9, 0, 0, 0}

	t.Run("reject", func(t *testing.T) {
		t.Parallel()
		pr := newPlanarReader(
			[]io.Reader{bytes.NewReader(ch0), bytes.NewReader(ch1)},
			4, 2, planarConfig{chunkSize: 4},
		)
		_, err := io.ReadAll(pr)
		assert.ErrorIs(t, err, ErrUnequalChannelLength)
	})

	t.Run("pad", func(t *testing.T) {
		t.Parallel()
		pr := newPlanarReader(
			[]io.Reader{bytes.NewReader(ch0), bytes.NewReader(ch1)},
			4, 2, planarConfig{chunkSize: 4, pad: true},
		)
		got, err := io.ReadAll(pr)
		require.NoError(t, err)
		require.Len(t, got, 24)
		// First frame unchanged.
		assert.Equal(t, []byte{1, 0, 0, 0, 9, 0, 0, 0}, got[:8])
		// Following frames: channel 1 zero padded.
		assert.Equal(t, []byte{2, 0, 0, 0, 0, 0, 0, 0}, got[8:16])
		assert.Equal(t, []byte{3, 0, 0, 0, 0, 0, 0, 0}, got[16:24])
	})
}

type stubErrorReader struct{ err error }

func (s stubErrorReader) Read([]byte) (int, error) { return 0, s.err }

func TestPlanarReaderPropagatesReaderError(t *testing.T) {
	t.Parallel()

	myErr := errors.New("boom")
	pr := newPlanarReader(
		[]io.Reader{
			bytes.NewReader([]byte{1, 2}),
			stubErrorReader{err: myErr},
		},
		2, 2, planarConfig{chunkSize: 4},
	)
	_, err := io.ReadAll(pr)
	assert.ErrorIs(t, err, myErr)
}

func TestPlanarReaderZeroLengthRead(t *testing.T) {
	t.Parallel()

	pr := newPlanarReader(
		[]io.Reader{
			bytes.NewReader([]byte{1, 2}),
			bytes.NewReader([]byte{3, 4}),
		},
		2, 2, planarConfig{chunkSize: 4},
	)

	n, err := pr.Read(nil)
	assert.Zero(t, n)
	require.NoError(t, err)

	got := readAll(t, pr, 3)
	assert.Equal(t, []byte{1, 2, 3, 4}, got)
}

// A full buffer that cannot hold a whole element is reported as a torn
// sample instead of issuing a zero-length Read forever.
func TestPlanarReaderFullBufferPartialElement(t *testing.T) {
	t.Parallel()

	// elemSize 4 with a chunk capacity of 3: the 3-byte tail fills the
	// buffer before a complete sample can be assembled.
	pr := newPlanarReader(
		[]io.Reader{
			bytes.NewReader([]byte{1, 2, 3, 4}),
			bytes.NewReader([]byte{5, 6, 7, 8}),
		},
		4, 2, planarConfig{chunkSize: 3},
	)
	_, err := io.ReadAll(pr)
	require.ErrorIs(t, err, ErrPartialSample)
}

// Every Read that finds data must return a full buffer: a short read with a
// nil error would be treated by readFrom as the end of stream.
func TestPlanarReaderAlwaysFillsBufferWhileDataAvailable(t *testing.T) {
	t.Parallel()

	const frames = 300
	data := make([]byte, frames*4)
	for i := range data {
		data[i] = byte(i)
	}
	pr := newPlanarReader(
		[]io.Reader{bytes.NewReader(data), bytes.NewReader(data)},
		4, 2, planarConfig{chunkSize: 5},
	)

	// Deliberately use sizes that are not multiples of the 8-byte frame.
	for _, bufSize := range []int{1, 3, 7, 13, 64} {
		buf := make([]byte, bufSize)
		first := true
		for {
			n, err := pr.Read(buf)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
				break
			}
			if first {
				assert.Equal(t, bufSize, n, "buffer size %d not filled while data available", bufSize)
				first = false
			} else {
				assert.Equal(t, bufSize, n)
			}
		}
	}
}
