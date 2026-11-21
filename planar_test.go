package resample_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/gunter-q12/resample"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// limitedReader returns at most max bytes per Read call and may inject
// an error (possibly together with data, as real readers are allowed to).
type limitedReader struct {
	data      []byte
	pos       int
	max       int
	failAfter int // -1: never inject; otherwise inject err once pos reaches it
	err       error
	injected  bool
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.pos >= len(l.data) {
		if l.err != nil && !l.injected {
			l.injected = true
			return 0, l.err
		}
		return 0, io.EOF
	}
	n := min(len(p), l.max, len(l.data)-l.pos)
	copy(p, l.data[l.pos:l.pos+n])
	l.pos += n
	if l.failAfter >= 0 && l.pos >= l.failAfter && l.err != nil && !l.injected {
		l.injected = true
		return n, l.err
	}
	return n, nil
}

// stallReader returns (0, nil) stallFor times once its data is exhausted
// before finally returning io.EOF. stallFor = -1 stalls forever.
type stallReader struct {
	data     []byte
	pos      int
	stallFor int
	stalls   int
}

func (s *stallReader) Read(p []byte) (int, error) {
	if s.pos < len(s.data) {
		n := min(len(p), len(s.data)-s.pos)
		copy(p, s.data[s.pos:s.pos+n])
		s.pos += n
		return n, nil
	}
	if s.stallFor < 0 || s.stalls < s.stallFor {
		s.stalls++
		return 0, nil
	}
	return 0, io.EOF
}

type failWriter struct {
	failAfter int
	written   int
	err       error
}

func (f *failWriter) Write(p []byte) (int, error) {
	if f.written+len(p) > f.failAfter {
		return 0, f.err
	}
	f.written += len(p)
	return len(p), nil
}

func bytesOf[T number](t *testing.T, values []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, values))
	return buf.Bytes()
}

func planarReaders[T number](t *testing.T, channels [][]T, maxRead int) []io.Reader {
	t.Helper()
	readers := make([]io.Reader, len(channels))
	for i, ch := range channels {
		readers[i] = &limitedReader{data: bytesOf(t, ch), max: maxRead, failAfter: -1}
	}
	return readers
}

func interleave[T number](channels [][]T) []T {
	frames := 0
	for _, ch := range channels {
		frames = max(frames, len(ch))
	}
	out := make([]T, frames*len(channels))
	for f := range frames {
		for c, ch := range channels {
			if f < len(ch) {
				out[f*len(channels)+c] = ch[f]
			}
		}
	}
	return out
}

func deinterleave[T number](t *testing.T, interleaved []T, ch int) [][]T {
	t.Helper()
	require.Zero(t, len(interleaved)%ch, "interleaved stream ends with a partial frame")
	frames := len(interleaved) / ch
	out := make([][]T, ch)
	for c := range ch {
		out[c] = make([]T, frames)
		for f := range frames {
			out[c][f] = interleaved[f*ch+c]
		}
	}
	return out
}

func decode[T any](t *testing.T, data []byte) []T {
	t.Helper()
	var zero T
	elemSize := int(reflect.TypeOf(zero).Size())
	out := make([]T, len(data)/elemSize)
	require.NoError(t, binary.Read(bytes.NewReader(data), binary.LittleEndian, out))
	return out
}

func writeReference[T number](t *testing.T, format resample.Format, ir, or, ch int, input []T) []T {
	t.Helper()
	outBuf := new(bytes.Buffer)
	r, err := resample.New(outBuf, format, ir, or, ch, resample.WithLinearFilter())
	require.NoError(t, err)
	_, err = r.Write(bytesOf(t, input))
	require.NoError(t, err)
	return decode[T](t, outBuf.Bytes())
}

func runPlanar[T number](t *testing.T, format resample.Format, ir, or int, channels [][]T, maxRead int,
	options ...resample.PlanarOption) ([]T, int64, error) {
	t.Helper()
	outBuf := new(bytes.Buffer)
	r, err := resample.New(outBuf, format, ir, or, len(channels), resample.WithLinearFilter())
	require.NoError(t, err)
	n, err := r.ReadFromChannels(planarReaders(t, channels, maxRead), options...)
	return decode[T](t, outBuf.Bytes()), n, err
}

// checkFormats runs the planar path against the interleaved Write reference
// for every supported integer and floating point format.
func checkFormats[T number](t *testing.T, format resample.Format, ir, or int, channels [][]T) {
	t.Helper()

	ref := writeReference(t, format, ir, or, len(channels), interleave(channels))

	for _, maxRead := range []int{1, 2, 3, 7, 64, 4096} {
		got, n, err := runPlanar(t, format, ir, or, channels, maxRead)
		require.NoError(t, err)
		assert.Equal(t, ref, got, "maxRead=%d changed the output", maxRead)

		var planarBytes int64
		for _, ch := range channels {
			planarBytes += int64(len(ch)) * int64(reflect.TypeOf(ch[0]).Size())
		}
		assert.Equal(t, planarBytes, n)
	}
}

func TestReadFromChannelsFormats(t *testing.T) {
	t.Parallel()

	makeCh := func(base int16) []int16 {
		v := make([]int16, 48)
		for i := range v {
			v[i] = int16(i*3) + base
		}
		return v
	}

	t.Run("int16", func(t *testing.T) {
		t.Parallel()
		ch := [][]int16{makeCh(0), makeCh(1000)}
		checkFormats(t, resample.FormatInt16, 2, 3, ch)
	})
	t.Run("int32", func(t *testing.T) {
		t.Parallel()
		ch := [][]int32{{1, 3, 5, 7, 9, 11}, {2, 4, 6, 8, 10, 12}}
		checkFormats(t, resample.FormatInt32, 3, 2, ch)
	})
	t.Run("int64", func(t *testing.T) {
		t.Parallel()
		ch := [][]int64{{1, 3, 5, 7, 9, 11}, {-2, -4, -6, -8, -10, -12}}
		checkFormats(t, resample.FormatInt64, 1, 2, ch)
	})
	t.Run("float32", func(t *testing.T) {
		t.Parallel()
		ch := [][]float32{{0.5, 1.5, 2.5, 3.5}, {10, 20, 30, 40}}
		checkFormats(t, resample.FormatFloat32, 2, 4, ch)
	})
	t.Run("float64", func(t *testing.T) {
		t.Parallel()
		ch := [][]float64{{0, 0.25, 0.5, 0.75, 1}, {1, 0.75, 0.5, 0.25, 0}}
		checkFormats(t, resample.FormatFloat64, 4, 3, ch)
	})
}

// Different per-channel read sizes must not change a single output sample:
// the resampler's own batching is independent of how the readers chunk data.
func TestPlanarChunkSizeIndependence(t *testing.T) {
	t.Parallel()

	const frames = 20000
	ch0 := make([]int16, frames)
	ch1 := make([]int16, frames)
	for i := range frames {
		ch0[i] = int16((i*7 + 1) % 1000)
		ch1[i] = int16((i*13 + 5) % 1000)
	}
	channels := [][]int16{ch0, ch1}

	for _, rates := range [][2]int{{8000, 16000}, {16000, 12000}, {48000, 48000}} {
		for _, memo := range []bool{true, false} {
			t.Run(fmt.Sprintf("%d->%d memo=%v", rates[0], rates[1], memo), func(t *testing.T) {
				t.Parallel()
				checkChunkSizeIndependence(t, rates[0], rates[1], memo, channels)
			})
		}
	}
}

func filterOptions(memo bool) []resample.Option {
	opts := []resample.Option{resample.WithKaiserFastestFilter()}
	if !memo {
		opts = append(opts, resample.WithNoMemoization())
	}
	return opts
}

func checkChunkSizeIndependence(t *testing.T, inRate, outRate int, memo bool,
	channels [][]int16) {
	t.Helper()
	const frames = 20000

	chunkSizes := []int{2, 3, 5, 16, 64, 1024, 32 * 1024}
	outputs := make([][]int16, len(chunkSizes))
	for i, cs := range chunkSizes {
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, inRate, outRate, 2, filterOptions(memo)...)
		require.NoError(t, err)
		n, err := r.ReadFromChannels(planarReaders(t, channels, cs))
		require.NoError(t, err)
		assert.Equal(t, int64(frames*2*2), n)
		outputs[i] = decode[int16](t, outBuf.Bytes())
	}

	for i := 1; i < len(outputs); i++ {
		assert.Len(t, outputs[i], len(outputs[0]),
			"chunk size %d changed output length", chunkSizes[i])
		assert.Equal(t, outputs[0], outputs[i],
			"chunk size %d changed output samples", chunkSizes[i])
	}

	// The planar path must be byte-identical to the interleaved
	// ReadFrom path on the same PCM bytes.
	interleaved := bytesOf(t, interleave(channels))
	refBuf := new(bytes.Buffer)
	ref, err := resample.New(refBuf, resample.FormatInt16, inRate, outRate, 2, filterOptions(memo)...)
	require.NoError(t, err)
	nn, err := ref.ReadFrom(bytes.NewReader(interleaved))
	require.NoError(t, err)
	assert.Equal(t, int64(len(interleaved)), nn)
	assert.Equal(t, decode[int16](t, refBuf.Bytes()), outputs[0])

	// Output duration: number of frames follows the rate ratio.
	expectedFrames := int(float64(frames*outRate) / float64(inRate))
	assert.Equal(t, expectedFrames, len(outputs[0])/2)
}

// Non-overlapping pulses placed at different positions in each channel must
// stay in their channel and keep their phase; short (1-byte) readers and
// several chunk sizes are checked.
func TestPlanarOffsetPulses(t *testing.T) {
	t.Parallel()

	const frames = 64
	ch0 := make([]int16, frames)
	ch1 := make([]int16, frames)
	ch0[10] = 1000
	ch1[27] = -1000
	channels := [][]int16{ch0, ch1}

	for _, maxRead := range []int{1, 2, 3, 17} {
		t.Run(fmt.Sprintf("maxRead=%d identity", maxRead), func(t *testing.T) {
			t.Parallel()

			// ir == or with the linear filter is an identity mapping,
			// so every sample must survive at its exact position.
			got, n, err := runPlanar(t, resample.FormatInt16, 1000, 1000, channels, maxRead)
			require.NoError(t, err)
			assert.Equal(t, int64(frames*2*2), n)

			planar := deinterleave(t, got, 2)
			assert.Equal(t, ch0, planar[0], "left channel phase corrupted")
			assert.Equal(t, ch1, planar[1], "right channel phase corrupted")
		})
	}

	t.Run("upsampled pulses stay in channel", func(t *testing.T) {
		t.Parallel()

		got, _, err := runPlanar(t, resample.FormatInt16, 1000, 2000, channels, 3)
		require.NoError(t, err)
		planar := deinterleave(t, got, 2)

		energy := func(s []int16) int {
			sum := 0
			for _, v := range s {
				sum += int(v) * int(v)
			}
			return sum
		}
		assert.Positive(t, energy(planar[0]))
		assert.Positive(t, energy(planar[1]))

		// The two channels must be zero where the other channel pulses.
		// Linear 1->2 interpolation spreads a pulse over three output
		// frames (500, 1000, 500); input frame k maps to output frame 2k.
		for i, v := range planar[1] {
			if i >= 19 && i <= 21 {
				assert.NotZero(t, planar[0][i], "left pulse missing around frame %d", i)
				assert.Zero(t, v, "right channel leaked into left pulse region")
			}
		}
		for i, v := range planar[0] {
			if i >= 53 && i <= 55 {
				assert.NotZero(t, planar[1][i], "right pulse missing around frame %d", i)
				assert.Zero(t, v, "left channel leaked into right pulse region")
			}
		}

		// And the exact interpolated shape must be preserved, phase-aligned.
		assert.Equal(t, int16(1000), planar[0][20])
		assert.Equal(t, int16(500), planar[0][19])
		assert.Equal(t, int16(500), planar[0][21])
		assert.Equal(t, int16(-1000), planar[1][54])
		assert.Equal(t, int16(-500), planar[1][53])
		assert.Equal(t, int16(-500), planar[1][55])
	})
}

func TestPlanarUnequalChannelLength(t *testing.T) {
	t.Parallel()

	ch0 := []int16{1, 2, 3, 4, 5, 6}
	ch1 := []int16{10, 20}

	t.Run("reject by default", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		n, err := r.ReadFromChannels(planarReaders(t, [][]int16{ch0, ch1}, 3))
		require.ErrorIs(t, err, resample.ErrUnequalChannelLength)
		// Consumed bytes from both readers are still reported.
		assert.Equal(t, int64(len(ch0)*2+len(ch1)*2), n)
	})

	t.Run("pad with silence", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		n, err := r.ReadFromChannels(
			planarReaders(t, [][]int16{ch0, ch1}, 3),
			resample.WithSilencePadding(),
		)
		require.NoError(t, err)
		assert.Equal(t, int64(len(ch0)*2+len(ch1)*2), n)

		got := deinterleave(t, decode[int16](t, outBuf.Bytes()), 2)
		require.Len(t, got[0], len(ch0))
		assert.Equal(t, ch0, got[0])
		assert.Equal(t, []int16{10, 20, 0, 0, 0, 0}, got[1])
	})

	t.Run("other channel shorter", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		_, err = r.ReadFromChannels(planarReaders(t, [][]int16{ch1, ch0}, 3))
		require.ErrorIs(t, err, resample.ErrUnequalChannelLength)
	})
}

func TestPlanarEmptyStreams(t *testing.T) {
	t.Parallel()

	t.Run("both empty", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		n, err := r.ReadFromChannels(planarReaders(t, [][]int16{{}, {}}, 4))
		require.NoError(t, err)
		assert.Equal(t, int64(0), n)
		assert.Empty(t, outBuf.Bytes())
	})

	t.Run("one empty rejected", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		_, err = r.ReadFromChannels(planarReaders(t, [][]int16{{1, 2, 3}, {}}, 2))
		require.ErrorIs(t, err, resample.ErrUnequalChannelLength)
	})

	t.Run("one empty padded with silence", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		_, err = r.ReadFromChannels(
			planarReaders(t, [][]int16{{1, 2, 3}, {}}, 2),
			resample.WithSilencePadding(),
		)
		require.NoError(t, err)
		got := deinterleave(t, decode[int16](t, outBuf.Bytes()), 2)
		assert.Equal(t, []int16{1, 2, 3}, got[0])
		assert.Equal(t, []int16{0, 0, 0}, got[1])
	})
}

func TestPlanarHalfSample(t *testing.T) {
	t.Parallel()

	good := bytesOf(t, []int16{1, 2, 3, 4})
	torn := append(bytesOf(t, []int16{10, 20}), 0xAB) // 5 bytes: two samples + 1 stray byte

	outBuf := new(bytes.Buffer)
	r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
	require.NoError(t, err)

	n, err := r.ReadFromChannels([]io.Reader{
		bytes.NewReader(good),
		bytes.NewReader(torn),
	})
	require.ErrorIs(t, err, resample.ErrPartialSample)
	assert.Equal(t, int64(len(good)+len(torn)), n)

	t.Run("torn sample also rejected in padding mode", func(t *testing.T) {
		t.Parallel()
		padBuf := new(bytes.Buffer)
		padResampler, padErr := resample.New(padBuf, resample.FormatInt16, 1000, 1000, 2,
			resample.WithLinearFilter())
		require.NoError(t, padErr)

		_, padErr = padResampler.ReadFromChannels([]io.Reader{
			bytes.NewReader(good),
			bytes.NewReader(torn),
		}, resample.WithSilencePadding())
		require.ErrorIs(t, padErr, resample.ErrPartialSample)
	})
}

var errInjectedReader = errors.New("injected reader failure")

func TestPlanarReaderError(t *testing.T) {
	t.Parallel()

	ch0 := make([]int16, 500)
	ch1 := make([]int16, 500)
	for i := range ch0 {
		ch0[i] = int16(i)
		ch1[i] = int16(-i)
	}

	t.Run("error delivered with final bytes", func(t *testing.T) {
		t.Parallel()
		readers := []io.Reader{
			&limitedReader{data: bytesOf(t, ch0), max: 7, failAfter: 200, err: errInjectedReader},
			&limitedReader{data: bytesOf(t, ch1), max: 5, failAfter: -1},
		}
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 8000, 16000, 2, resample.WithKaiserFastestFilter())
		require.NoError(t, err)

		_, err = r.ReadFromChannels(readers)
		require.ErrorIs(t, err, errInjectedReader)
	})

	t.Run("error after exhaustion", func(t *testing.T) {
		t.Parallel()
		readers := []io.Reader{
			&limitedReader{
				data: bytesOf(t, ch0), max: 7,
				failAfter: len(bytesOf(t, ch0)), err: errInjectedReader,
			},
			&limitedReader{data: bytesOf(t, ch1), max: 5, failAfter: -1},
		}
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 8000, 16000, 2, resample.WithKaiserFastestFilter())
		require.NoError(t, err)
		_, err = r.ReadFromChannels(readers)
		require.ErrorIs(t, err, errInjectedReader)
	})
}

func TestPlanarNoProgress(t *testing.T) {
	t.Parallel()

	healthy := bytesOf(t, make([]int16, 100))

	t.Run("permanent stall", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		_, err = r.ReadFromChannels([]io.Reader{
			bytes.NewReader(healthy),
			&stallReader{data: nil, stallFor: -1},
		})
		require.ErrorIs(t, err, resample.ErrNoProgress)
	})

	t.Run("stall below threshold still completes", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		_, err = r.ReadFromChannels([]io.Reader{
			bytes.NewReader(healthy),
			&stallReader{data: nil, stallFor: 3},
		}, resample.WithSilencePadding())
		require.NoError(t, err)
	})
}

var errBoomWriter = errors.New("writer boom")

func TestPlanarWriterFailure(t *testing.T) {
	t.Parallel()

	ch0 := make([]int16, 1000)
	ch1 := make([]int16, 1000)

	out := &failWriter{failAfter: 64, err: errBoomWriter}
	r, err := resample.New(out, resample.FormatInt16, 8000, 16000, 2, resample.WithKaiserFastestFilter())
	require.NoError(t, err)

	_, err = r.ReadFromChannels(planarReaders(t, [][]int16{ch0, ch1}, 11))
	require.ErrorIs(t, err, errBoomWriter)
}

func TestPlanarValidation(t *testing.T) {
	t.Parallel()

	t.Run("channel count mismatch", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		_, err = r.ReadFromChannels([]io.Reader{bytes.NewReader(nil)})
		require.ErrorIs(t, err, resample.ErrChannelCountMismatch)

		_, err = r.ReadFromChannels([]io.Reader{
			bytes.NewReader(nil), bytes.NewReader(nil), bytes.NewReader(nil),
		})
		require.ErrorIs(t, err, resample.ErrChannelCountMismatch)
	})

	t.Run("nil reader", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		_, err = r.ReadFromChannels([]io.Reader{bytes.NewReader(nil), nil})
		require.Error(t, err)
	})

	t.Run("chunk size too small", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt64, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		_, err = r.ReadFromChannels(
			[]io.Reader{bytes.NewReader(nil), bytes.NewReader(nil)},
			resample.WithPlanarChunkSize(4),
		)
		require.Error(t, err)

		_, err = r.ReadFromChannels(
			[]io.Reader{bytes.NewReader(nil), bytes.NewReader(nil)},
			resample.WithPlanarChunkSize(0),
		)
		require.Error(t, err)
	})

	t.Run("nil option ignored", func(t *testing.T) {
		t.Parallel()
		outBuf := new(bytes.Buffer)
		r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
		require.NoError(t, err)

		_, err = r.ReadFromChannels(
			planarReaders(t, [][]int16{{1, 2}, {3, 4}}, 4),
			nil,
			resample.WithSilencePadding(),
		)
		require.NoError(t, err)
	})
}

// A torn sample discovered while draining an over-long channel in reject
// mode takes precedence over the unequal-length rejection.
func TestPlanarRejectDrainFindsTornSample(t *testing.T) {
	t.Parallel()

	good := bytesOf(t, []int16{1, 2, 3, 4})
	torn := append(bytesOf(t, []int16{10}), 0xAB) // 3 bytes: one sample + 1 stray byte

	outBuf := new(bytes.Buffer)
	r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
	require.NoError(t, err)

	// Channel 0 ends first (unequal), then draining channel 1 finds the tear.
	_, err = r.ReadFromChannels([]io.Reader{
		bytes.NewReader(good[:2]),
		bytes.NewReader(torn),
	})
	require.ErrorIs(t, err, resample.ErrPartialSample)
}

// A reader failure on the over-long channel in reject mode is not hidden by
// the unequal-length rejection.
func TestPlanarRejectDrainFindsReaderError(t *testing.T) {
	t.Parallel()

	outBuf := new(bytes.Buffer)
	r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
	require.NoError(t, err)

	_, err = r.ReadFromChannels([]io.Reader{
		bytes.NewReader(bytesOf(t, []int16{1})),
		&limitedReader{
			data: bytesOf(t, make([]int16, 100)), max: 4,
			failAfter: 10, err: errInjectedReader,
		},
	})
	require.ErrorIs(t, err, errInjectedReader)
}

// The returned count covers every byte the readers produced, including bytes
// read after a failure surfaced on another channel.
func TestPlanarConsumedByteCount(t *testing.T) {
	t.Parallel()

	ch0 := make([]int16, 50)
	ch1 := make([]int16, 30)

	outBuf := new(bytes.Buffer)
	r, err := resample.New(outBuf, resample.FormatInt16, 1000, 1000, 2, resample.WithLinearFilter())
	require.NoError(t, err)

	n, err := r.ReadFromChannels(
		planarReaders(t, [][]int16{ch0, ch1}, 5),
		resample.WithSilencePadding(),
	)
	require.NoError(t, err)
	assert.Equal(t, int64((len(ch0)+len(ch1))*2), n)

	got := deinterleave(t, decode[int16](t, outBuf.Bytes()), 2)
	assert.Len(t, got[0], len(ch0))
}

// Existing Write/ReadFrom behavior must remain untouched: a smoke test
// through both interleaved paths with multiple channels.
func TestInterleavedPathsUnchanged(t *testing.T) {
	t.Parallel()

	input := interleave([][]int16{
		{1, 3, 5, 7, 9},
		{2, 4, 6, 8, 10},
	})

	for _, path := range []string{"Write", "ReadFrom"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			outBuf := new(bytes.Buffer)
			r, err := resample.New(outBuf, resample.FormatInt16, 1, 2, 2, resample.WithLinearFilter())
			require.NoError(t, err)

			if path == "Write" {
				_, err = r.Write(bytesOf(t, input))
			} else {
				_, err = r.ReadFrom(bytes.NewReader(bytesOf(t, input)))
			}
			require.NoError(t, err)
			got := decode[int16](t, outBuf.Bytes())
			planar := deinterleave(t, got, 2)
			// Linear 1->2 upsampling interpolates midpoints independently per channel.
			assert.Equal(t, []int16{1, 2, 3, 4, 5, 6, 7, 8, 9}, planar[0][:9])
			assert.Equal(t, []int16{2, 3, 4, 5, 6, 7, 8, 9, 10}, planar[1][:9])
		})
	}
}

// Example_resamplingPlanarStreams shows planar per-channel input being
// interleaved and resampled into interleaved PCM.
func Example_resamplingPlanarStreams() {
	encode := func(values []int16) []byte {
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, values)
		return buf.Bytes()
	}

	left := bytes.NewReader(encode([]int16{1, 3, 5}))
	right := bytes.NewReader(encode([]int16{2, 4, 6}))

	outBuf := new(bytes.Buffer)
	r, _ := resample.New(outBuf, resample.FormatInt16, 1, 2, 2, resample.WithLinearFilter())
	_, _ = r.ReadFromChannels([]io.Reader{left, right}, resample.WithSilencePadding())

	out := make([]int16, outBuf.Len()/2)
	_ = binary.Read(outBuf, binary.LittleEndian, &out)

	planar := make([][]int16, 2)
	for f := range len(out) / 2 {
		planar[0] = append(planar[0], out[f*2])
		planar[1] = append(planar[1], out[f*2+1])
	}
	fmt.Println(planar[0][:5])
	fmt.Println(planar[1][:5])
	// Output:
	// [1 2 3 4 5]
	// [2 3 4 5 6]
}
