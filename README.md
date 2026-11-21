# Resample

[![Go Reference](https://pkg.go.dev/badge/github.com/Gunter-Q12/Resample.svg)](https://pkg.go.dev/github.com/Gunter-Q12/Resample)
[![Go Report Card](https://goreportcard.com/badge/github.com/Gunter-Q12/Resample)](https://goreportcard.com/report/github.com/Gunter-Q12/Resample)
[![cov](https://Gunter-Q12.github.io/Resample/badges/coverage.svg)](https://github.com/Gunter-Q12/Resample/actions)

Resample is a package for audio resampling in pure Go.
Implementation is based on [bandlimited interpolation](https://ccrma.stanford.edu/~jos/resample/resample.pdf).

**Resample's main features are:**

- io.Copy support (uses little RAM, even for GB-sized files)
- concurrency
- [speed](#performance)
- [tested precision](#precision)
- no C dependencies

## Example

See the [API documentation on go.dev](https://pkg.go.dev/github.com/gunter-q12/resample).

```go
package main

import (
	"github.com/gunter-q12/resample"
	"io"
	"os"
)

func main() {
	input, _ := os.Open("./original.raw")
	output, _ := os.Create("./resampled.raw")

	res, _ := resample.New(output, resample.FormatInt16, 48000, 16000, 2)
	_, _ = io.Copy(res, input)
}
```

### Planar (per-channel) input

When each channel is a separate PCM stream, pass one `io.Reader` per channel
to `ReadFromChannels` instead of reading the whole recording into memory.
Readers are consumed in bounded chunks, interleaved on the fly and resampled
through the same pipeline; the output is interleaved PCM.

```go
left, _ := os.Open("./left.raw")
right, _ := os.Open("./right.raw")
output, _ := os.Create("./resampled.raw")

res, _ := resample.New(output, resample.FormatInt16, 48000, 16000, 2)
// Shorter channels are padded with silence instead of being rejected.
_, _ = res.ReadFromChannels(
	[]io.Reader{left, right},
	resample.WithSilencePadding(),
)
```

- Unequal channel lengths are rejected by default; `WithSilencePadding` pads
  the shorter channels with silence.
- A stream ending in the middle of a sample returns `ErrPartialSample`;
  a reader stuck returning zero bytes returns `ErrNoProgress`.
- Memory stays bounded regardless of the recording length or the per-channel
  read chunk size (`WithPlanarChunkSize`), which never changes the output.

## Benchmarks

### Precision

Precision test were performed against
a [resampy](https://resampy.readthedocs.io/en/stable/) library
that implements the same exact resampling method.
Results were used to create [precision unit tests](precision_test.go)
and ensure that implementation is correct.

### Performance

Each table row has a different number of entries because libraries
provide a different number of quality settings.
Results are sorted from the lowest quality to the highest.


Downsampling 44100 -> 16000 Hz, 10 Mb file, in seconds

| Library                                                |      |      |      |      |      |
|--------------------------------------------------------|------|------|------|------|------|
| [zaf/resample (SOXR)](https://github.com/zaf/resample) | 0.12 | 0.13 | 0.14 | 0.14 | 0.16 |
| **Resample**                                           | 0.27 | 0.31 | 0.38 |      |      |
| [resampy](https://resampy.readthedocs.io/en/stable/)   | 1.12 | 1.4  |      |      |      |
| [gomplerate](https://github.com/zeozeozeo/gomplerate)  | 0.47 |      |      |      |      |

Upsampling 44100 <- 16000 Hz, 10 Mb file, in seconds

| Library                                                |      |      |      |      |      |
|--------------------------------------------------------|------|------|------|------|------|
| [zaf/resample (SOXR)](https://github.com/zaf/resample) | 0.32 | 0.37 | 0.38 | 0.36 | 0.43 |
| **Resample**                                           | 0.47 | 0.7  | 1.2  |      |      |
| [resampy](https://resampy.readthedocs.io/en/stable/)   | 1.43 | 2.67 |      |      |      |
| [gomplerate](https://github.com/zeozeozeo/gomplerate)  | 1.66 |      |      |      |      |


Settings used:

| Library                                                |               |             |            |       |           |
|--------------------------------------------------------|---------------|-------------|------------|-------|-----------|
| [zaf/resample (SOXR)](https://github.com/zaf/resample) | Quick         | LowQ        | MediumQ    | HighQ | VeryHighQ |
| **Resample**                                           | KaiserFastest | KaiserFast  | KaiserBest |       |           |
| [resampy](https://resampy.readthedocs.io/en/stable/)   | kaiser_fast   | kaiser_best |            |       |           |
| [gomplerate](https://github.com/zeozeozeo/gomplerate)  | Default       |             |            |       |           |
