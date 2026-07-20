//go:build (arm64 || amd64) && !purego && !(gitpack_libdeflate && cgo)

package objstore

import (
	"os"
	"unsafe"
)

const inflateFastYieldBytes = 64 << 10

// inflateFastDisabled is a process-wide kill-switch for the assembly Huffman
// kernels. Setting GOGITPACK_NOASM_INFLATE to any value other than "", "0",
// or "false" routes every decode through the portable Go decoder, letting
// operators disable the assembly path with a config flip instead of a
// rebuild with the purego build tag. Read once at process start.
var inflateFastDisabled = func() bool {
	switch os.Getenv("GOGITPACK_NOASM_INFLATE") {
	case "", "0", "false":
		return false
	default:
		return true
	}
}()

type inflateFastStatus uint32

const (
	inflateFastTail inflateFastStatus = iota
	inflateFastBlockDone
	inflateFastBadData
	inflateFastYield
)

// inflateFastState is the complete state transferred across the assembly
// boundary. Pointer fields keep all referenced Go storage visible to the GC.
type inflateFastState struct {
	src        *byte
	dst        *byte
	litlen     *uint32
	offset     *uint32
	bitbuf     uint64
	nbits      uint64
	pos        uint64
	out        uint64
	inLimit    uint64
	outLimit   uint64
	litlenMask uint64
	offsetMask uint64
	yieldAt    uint64
	status     inflateFastStatus
	_          uint32
}

// decodeHuffman is the shared driver for both assembly Huffman kernels.
//
// The margin guard and the inflateFastState construction below are the
// bounds proofs that make the kernels' branchless 8-byte loads and
// overrunning match copies memory-safe, so they live here — once — rather
// than per architecture: a field missed in one arch-specific copy would
// zero-initialize silently and become a single-arch out-of-bounds bug.
// Each architecture contributes only its kernel entry point
// (inflateHuffmanFastKernel) and its eligibility threshold
// (inflateFastMinLitlenBits); the mutually exclusive build tags on the
// per-arch files guarantee exactly one definition of each.
func (d *goInflater) decodeHuffman(r *deflateBits, dst []byte, out int) (int, error) {
	// Small dynamic tables favor Go's overlap-aware match copier (the
	// threshold is 0 on architectures whose kernel handles all table
	// sizes). The kill-switch also lands here so a misbehaving kernel can
	// be disabled at runtime.
	if inflateFastDisabled || d.litlenBits < inflateFastMinLitlenBits {
		return d.decodeHuffmanGo(r, dst, out)
	}
	if len(r.src)-r.pos <= deflateFastInputMargin ||
		len(dst)-out <= deflateFastOutputMargin {
		return d.decodeHuffmanTail(r, dst, out)
	}

	state := inflateFastState{
		src:        unsafe.SliceData(r.src),
		dst:        unsafe.SliceData(dst),
		litlen:     &d.litlen[0],
		offset:     &d.offset[0],
		bitbuf:     r.buf,
		nbits:      uint64(r.nbits),
		pos:        uint64(r.pos),
		out:        uint64(out),
		inLimit:    uint64(len(r.src) - deflateFastInputMargin),
		outLimit:   uint64(len(dst) - deflateFastOutputMargin),
		litlenMask: bitMask(uint(d.litlenBits)),
		offsetMask: bitMask(uint(d.offsetBits)),
		yieldAt:    uint64(out + inflateFastYieldBytes),
	}

	for {
		inflateHuffmanFastKernel(&state)
		if state.status == inflateFastYield {
			continue
		}

		r.buf = state.bitbuf
		r.nbits = uint(state.nbits)
		r.pos = int(state.pos)
		out = int(state.out)

		switch state.status {
		case inflateFastTail:
			return d.decodeHuffmanTail(r, dst, out)
		case inflateFastBlockDone:
			return out, nil
		case inflateFastBadData:
			return out, errDeflateBadData
		default:
			panic("invalid inflate fast-loop status")
		}
	}
}
