package resample

import (
	"errors"
	"fmt"
	"io"
)

// Sentinel errors returned by [Resampler.ReadFromChannels].
var (
	// ErrChannelCountMismatch is returned when the number of provided
	// per-channel readers differs from the resampler's channel number.
	ErrChannelCountMismatch = errors.New("resampler: number of channel readers does not match the channel count")

	// ErrUnequalChannelLength is returned in the reject mode when one of the
	// channel readers reaches its end before the others.
	ErrUnequalChannelLength = errors.New("resampler: channel streams have unequal lengths")

	// ErrPartialSample is returned when a channel stream ends in the middle of
	// a sample, i.e. the total number of read bytes is not divisible by the
	// format's element size.
	ErrPartialSample = errors.New("resampler: channel stream ends in the middle of a sample")

	// ErrNoProgress is returned when a channel reader repeatedly reports
	// zero bytes read without an error. Such a reader can never reach EOF,
	// so processing is aborted instead of looping forever.
	ErrNoProgress = errors.New("resampler: multiple Read calls returned no data and no error")
)

// maxStallReads is the number of consecutive zero-progress Read calls
// tolerated before ErrNoProgress is returned.
const maxStallReads = 100

// PlanarOption configures [Resampler.ReadFromChannels].
type PlanarOption func(*planarConfig) error

// planarConfig holds the settings of a single ReadFromChannels call.
type planarConfig struct {
	pad       bool
	chunkSize int
}

// WithSilencePadding makes [Resampler.ReadFromChannels] tolerate channel
// streams of unequal length: shorter channels are padded with silence
// until the longest channel ends.
//
// By default unequal channel lengths are rejected with [ErrUnequalChannelLength].
func WithSilencePadding() PlanarOption {
	return func(cfg *planarConfig) error {
		cfg.pad = true
		return nil
	}
}

// WithPlanarChunkSize sets the maximum number of bytes read from each
// channel reader at a time. Smaller values use slightly less memory but
// perform more Read calls.
//
// The chunk size does not affect the produced samples or their timing:
// resampling runs through the same batching path as [Resampler.ReadFrom],
// independent of how the input is split into reads.
//
// The default is 32 KiB; the value must be at least one element of the
// resampler's format.
func WithPlanarChunkSize(bytes int) PlanarOption {
	return func(cfg *planarConfig) error {
		if bytes <= 0 {
			return fmt.Errorf("resampler: planar chunk size %d must be greater than zero", bytes)
		}
		cfg.chunkSize = bytes
		return nil
	}
}

// ReadFromChannels resamples planar (per-channel) PCM streams.
//
// One io.Reader per channel must be provided, in channel order; each reader
// carries the samples of a single channel. The readers are read in bounded
// chunks, the samples are interleaved frame by frame and the resulting
// interleaved stream is resampled through the same pipeline used by
// [Resampler.Write] and [Resampler.ReadFrom], writing interleaved PCM to the
// io.Writer passed to [New].
//
// The whole input is never held in memory: the working set is bounded by the
// resampler's batch size plus a small per-channel read buffer, regardless of
// the stream length or the chunk size.
//
// By default channel streams of unequal length are rejected with
// [ErrUnequalChannelLength]; pass [WithSilencePadding] to pad shorter
// channels with silence instead. A stream ending in the middle of a sample
// always produces [ErrPartialSample]. A reader that repeatedly returns
// zero bytes without an error produces [ErrNoProgress]; any other reader
// error and any output writer error is returned wrapped and remains
// discoverable through [errors.Is].
//
// It returns the total number of planar input bytes consumed from all
// channel readers (including bytes of partially read samples), and the first
// error encountered, if any. A clean end of stream returns a nil error.
func (r *Resampler) ReadFromChannels(readers []io.Reader, options ...PlanarOption) (int64, error) {
	switch r.format {
	case FormatInt16:
		return readFromChannels[int16](r, readers, options...)
	case FormatInt32:
		return readFromChannels[int32](r, readers, options...)
	case FormatInt64:
		return readFromChannels[int64](r, readers, options...)
	case FormatFloat32:
		return readFromChannels[float32](r, readers, options...)
	case FormatFloat64:
		return readFromChannels[float64](r, readers, options...)
	default:
		panic("unknown format")
	}
}

// readFromChannels is an actual implementation of Resampler.ReadFromChannels.
func readFromChannels[T number](r *Resampler, readers []io.Reader, options ...PlanarOption) (int64, error) {
	cfg := planarConfig{chunkSize: 32 * 1024} //nolint:mnd // default per-channel read chunk
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&cfg); err != nil {
			return 0, err
		}
	}

	if len(readers) != r.ch {
		return 0, ErrChannelCountMismatch
	}
	for _, reader := range readers {
		if reader == nil {
			return 0, errors.New("resampler: channel reader is nil")
		}
	}
	if cfg.chunkSize < r.elemSize {
		return 0, fmt.Errorf("resampler: planar chunk size %d is smaller than the element size %d",
			cfg.chunkSize, r.elemSize)
	}

	pr := newPlanarReader(readers, r.elemSize, r.ch, cfg)
	_, streamErr := readFrom[T](r, pr)

	// readFrom only reacts to io.EOF and short reads; every other reader
	// error would otherwise be swallowed when it is reported together with
	// the final bytes. The planar reader's terminal state is authoritative
	// for input failures; a writer failure leaves it nil, so it cannot be
	// masked here.
	err := streamErr
	if pr.err != nil && !errors.Is(pr.err, io.EOF) {
		err = fmt.Errorf("resampler: reading planar channels: %w", pr.err)
	}
	// Empty (or fully consumed) streams end cleanly, per io.ReaderFrom contract.
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return pr.totalRead, err
}

// channelState tracks the bounded read buffer of a single planar channel.
type channelState struct {
	reader io.Reader

	buf    []byte // bytes received from the reader and not consumed yet
	offset int    // position of the next unconsumed byte in buf

	// err is the terminal state reported by the reader (io.EOF included).
	err error

	// stalls counts consecutive reader calls returning zero bytes without an error.
	stalls int
}

// planarReader assembles an interleaved PCM stream out of per-channel
// readers. It never returns data together with an error: data already
// available is always delivered first, the error is reported on the next
// Read call, matching [bytes.Buffer] semantics.
type planarReader struct {
	channels []channelState
	elemSize int
	ch       int
	cfg      planarConfig

	// frameBuf is reused for every assembled interleaved frame.
	frameBuf []byte

	// leftover holds an assembled frame that did not fit into the previous
	// Read buffer; at most one frame (elemSize*ch bytes) is retained.
	leftover []byte

	// err is the terminal error to report once all buffered data is drained.
	err error

	// totalRead is the number of bytes consumed from all channel readers.
	totalRead int64
}

func newPlanarReader(readers []io.Reader, elemSize, ch int, cfg planarConfig) *planarReader {
	pr := &planarReader{
		channels: make([]channelState, ch),
		elemSize: elemSize,
		ch:       ch,
		cfg:      cfg,
		frameBuf: make([]byte, elemSize*ch),
		leftover: make([]byte, 0, elemSize*ch),
	}
	for i, reader := range readers {
		// len 0 with full capacity: refill grows it as bytes arrive.
		pr.channels[i] = channelState{
			reader: reader,
			buf:    make([]byte, 0, cfg.chunkSize),
		}
	}
	return pr
}

// refill pulls more bytes from the channel reader, preserving a partial
// element (at most elemSize-1 bytes) across the read boundary.
func (cs *channelState) refill(pr *planarReader) {
	if cs.err != nil {
		return
	}

	// Compact the unconsumed tail to the front of the buffer.
	carry := len(cs.buf) - cs.offset
	copy(cs.buf[:carry], cs.buf[cs.offset:])
	cs.offset = 0

	// The buffer is full but does not hold a whole element. This can only
	// happen with a chunk size smaller than the element size (rejected by the
	// public API) or a misbehaving internal construction; treat it as a
	// torn sample rather than issuing a zero-length Read that would loop.
	if carry >= cap(cs.buf) && carry < pr.elemSize {
		cs.err = ErrPartialSample
		return
	}

	n, err := cs.reader.Read(cs.buf[carry:cap(cs.buf)])
	pr.totalRead += int64(n)
	cs.buf = cs.buf[:carry+n]

	if n > 0 {
		// Real progress: the buffered tail grew, reset the stall counter.
		cs.stalls = 0
	}
	if err != nil {
		// A stream ending with a tail that does not form a whole sample is
		// a torn sample, regardless of how the reader signalled the end.
		if len(cs.buf) > 0 && len(cs.buf) < pr.elemSize {
			cs.err = ErrPartialSample
			return
		}
		cs.err = err
		return
	}
	if n == 0 {
		// No bytes and no error. If a whole element is already buffered the
		// caller will consume it without ever calling refill again; this
		// branch is only reached while waiting for the rest of a sample, so
		// the reader is making no progress at all.
		cs.stalls++
		if cs.stalls >= maxStallReads {
			cs.err = ErrNoProgress
		}
	}
}

// readSample reads one complete element of the channel into dst.
// A false result means that no element is available; channelState.err then
// holds the terminal state (io.EOF, ErrPartialSample, ErrNoProgress or the
// reader's own error).
func (cs *channelState) readSample(pr *planarReader, dst []byte) bool {
	for len(cs.buf)-cs.offset < pr.elemSize {
		cs.refill(pr)
		if cs.err != nil {
			return false
		}
	}
	copy(dst, cs.buf[cs.offset:cs.offset+pr.elemSize])
	cs.offset += pr.elemSize
	return true
}

// Read implements io.Reader, returning interleaved frames.
func (pr *planarReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if pr.err != nil {
		return 0, pr.err
	}

	written := 0

	// First deliver a frame retained from the previous call.
	if len(pr.leftover) > 0 {
		n := copy(p, pr.leftover)
		written += n
		if n == len(pr.leftover) {
			pr.leftover = pr.leftover[:0]
		} else {
			pr.leftover = pr.leftover[n:]
			return written, nil
		}
	}

	frameLen := len(pr.frameBuf)
	for len(p[written:]) >= frameLen {
		if err := pr.buildFrame(pr.frameBuf); err != nil {
			pr.err = err
			if written > 0 {
				// Defer the terminal state until buffered data is drained.
				return written, nil
			}
			return 0, err
		}
		written += copy(p[written:], pr.frameBuf)
	}

	// Fill the remaining (sub-frame) tail with the beginning of one more
	// frame, retaining its rest in leftover. This keeps every Read full as
	// long as the stream has data: readFrom treats a short read with a nil
	// error as the end of stream.
	if len(p[written:]) > 0 {
		if err := pr.buildFrame(pr.frameBuf); err != nil {
			pr.err = err
			if written > 0 {
				return written, nil
			}
			return 0, err
		}
		k := copy(p[written:], pr.frameBuf)
		written += k
		if k < frameLen {
			pr.leftover = append(pr.leftover, pr.frameBuf[k:]...)
		}
	}

	return written, nil
}

// buildFrame assembles a single interleaved frame, one element per channel.
// In reject mode any channel reaching its end before the others causes
// ErrUnequalChannelLength; in padding mode ended channels contribute silence.
// io.EOF is returned only when every channel has ended simultaneously.
func (pr *planarReader) buildFrame(frame []byte) error {
	eofs := 0
	for i := range pr.channels {
		cs := &pr.channels[i]
		dst := frame[i*pr.elemSize : (i+1)*pr.elemSize]

		if cs.readSample(pr, dst) {
			continue
		}
		// readSample only fails after setting a terminal state.
		if errors.Is(cs.err, io.EOF) {
			eofs++
			continue
		}
		// ErrPartialSample, ErrNoProgress or an error reported by the reader.
		return cs.err
	}

	if eofs == 0 {
		return nil
	}
	if eofs == pr.ch {
		return io.EOF
	}
	if !pr.cfg.pad {
		// Reject mode: no padded output is produced. Drain every channel
		// to its end first so that the returned byte count and the readers'
		// positions fully reflect the consumed input; drainage is bounded
		// by the same per-channel buffers.
		return pr.drainChannels(ErrUnequalChannelLength)
	}

	// Padding mode: ended channels contribute silence; samples of the active
	// channels were already placed into the frame by the loop above.
	for i := range pr.channels {
		if errors.Is(pr.channels[i].err, io.EOF) {
			clear(frame[i*pr.elemSize : (i+1)*pr.elemSize])
		}
	}
	return nil
}

// drainChannels reads every channel until its terminal state. Buffered data
// is discarded; the per-channel buffers keep their bounded capacity.
// It returns the most severe terminal error encountered: a reader failure or
// a torn sample takes precedence over the unequal-length rejection.
func (pr *planarReader) drainChannels(fallback error) error {
	err := fallback
	for i := range pr.channels {
		cs := &pr.channels[i]
		for cs.err == nil {
			cs.refill(pr)
		}
		cs.offset = len(cs.buf)
		if !errors.Is(cs.err, io.EOF) && errors.Is(err, ErrUnequalChannelLength) {
			err = cs.err
		}
	}
	return err
}
