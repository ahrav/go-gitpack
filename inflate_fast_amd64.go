//go:build amd64 && !purego && !gitpack_libdeflate

package objstore

import "unsafe"

const (
	inflateFastAsmEnabled         = true
	inflateFastAMD64MinLitlenBits = 7
)

//go:noescape
func inflateHuffmanFastAMD64(state *inflateFastState)

func (d *goInflater) decodeHuffman(r *deflateBits, dst []byte, out int) (int, error) {
	// Small dynamic tables favor Go's overlap-aware match copier.
	if d.litlenBits < inflateFastAMD64MinLitlenBits {
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
		inflateHuffmanFastAMD64(&state)
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
			panic("invalid AMD64 inflate fast-loop status")
		}
	}
}
