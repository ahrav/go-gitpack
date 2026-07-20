//go:build arm64 && !purego && !(gitpack_libdeflate && cgo)

package objstore

import "unsafe"

const (
	// Refill-skip thresholds for the assembly kernel, exported to
	// inflate_fast_arm64.s through go_asm.h as $const_... literals.
	//
	// After decoding a litlen entry, the kernel must have enough buffered
	// bits to decode an offset entry, its extra bits, and preload the next
	// litlen entry without an intermediate refill. The branchless refill
	// counter understates the buffered bits by at least one (it counts at
	// most 63 of the 64 buffered bits), and BGT passes only above the
	// threshold, so each threshold is the worst-case bit need minus two.
	//
	// Direct path: a first-level offset entry consumes at most
	// offsetTableBits code bits.
	inflateFastOffsetRefillThreshold = offsetTableBits + maxOffsetExtraBits + litlenTableBits - 2
	// Subtable path: a chained offset code consumes up to maxCodeLen bits.
	inflateFastOffsetSubRefillThreshold = maxCodeLen + maxOffsetExtraBits + litlenTableBits - 2

	// inflateFastWideCopyDistance is the minimum read distance for the
	// 32-byte block-copy loop in the assembly kernel. Match copies with a
	// shorter offset first grow the distance by chunked doubling: an
	// 8-byte load that partially overlaps a recent store cannot
	// store-to-load forward on Neoverse cores, and the measured stall
	// (e.g. offset 9: 540µs vs 241µs for offset 8 on a 1 MiB
	// period-repeat payload, Graviton3) dominates long-match decode.
	// Reads at or beyond this distance trail every store still in
	// flight, so the wide loop runs stall-free.
	inflateFastWideCopyDistance = 64
)

//go:noescape
func inflateHuffmanFastArm64(state *inflateFastState)

func (d *goInflater) decodeHuffman(r *deflateBits, dst []byte, out int) (int, error) {
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
		inflateHuffmanFastArm64(&state)
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
			panic("invalid ARM64 inflate fast-loop status")
		}
	}
}
