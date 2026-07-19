package objstore

/*
The table layout and one-shot decoder architecture in this file are adapted
from libdeflate's DEFLATE decompressor.

Copyright 2016 Eric Biggers
Copyright 2024 Google LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"sync"
	"unsafe"
)

const (
	precodeTableBits = 7
	litlenTableBits  = 11
	offsetTableBits  = 8

	precodeTableSize = 1 << precodeTableBits
	litlenTableSize  = 2342
	offsetTableSize  = 402

	maxLitlenSyms = 288
	maxOffsetSyms = 32
	maxCodeLen    = 15

	// maxOffsetExtraBits is the largest extra-bit count of any DEFLATE
	// offset code (codes 28-29, RFC 1951 section 3.2.5). It must match
	// the largest value in offsetExtraBits.
	maxOffsetExtraBits = 13

	// A fast iteration can emit two literals, a 258-byte match, and
	// intentionally overwrite up to 39 bytes while copying whole words.
	deflateFastInputMargin  = 25
	deflateFastOutputMargin = 299
	deflateFastMatchCopy    = 40

	huffLiteral      uint32 = 0x80000000
	huffExceptional         = 0x00008000
	huffSubtable            = 0x00004000
	huffEndOfBlock          = 0x00002000
	huffBitCountMask        = 0x0000003f
	huffInvalid             = huffExceptional
)

var (
	errDeflateBadData = errors.New("deflate: invalid data")

	// Truncation-class errors wrap io.ErrUnexpectedEOF so callers that
	// classified the previous flate-based path's truncation failures with
	// errors.Is(err, io.ErrUnexpectedEOF) observe the same identity.
	errDeflateTruncated   = fmt.Errorf("deflate: truncated input: %w", io.ErrUnexpectedEOF)
	errDeflateShortOutput = fmt.Errorf("deflate: output shorter than destination: %w", io.ErrUnexpectedEOF)

	// errDeflateOutputOverrun wraps errZlibStreamOverrun so the one-shot
	// path and the fallback ensureZlibStreamEnd path report the same error
	// class for streams that continue past the declared object size.
	errDeflateOutputOverrun = fmt.Errorf("deflate: output exceeds destination: %w", errZlibStreamOverrun)
)

var lengthBases = [...]uint16{
	3, 4, 5, 6, 7, 8, 9, 10,
	11, 13, 15, 17,
	19, 23, 27, 31,
	35, 43, 51, 59,
	67, 83, 99, 115,
	131, 163, 195, 227,
	258,
}

var lengthExtraBits = [...]uint8{
	0, 0, 0, 0, 0, 0, 0, 0,
	1, 1, 1, 1,
	2, 2, 2, 2,
	3, 3, 3, 3,
	4, 4, 4, 4,
	5, 5, 5, 5,
	0,
}

var offsetBases = [...]uint16{
	1, 2, 3, 4,
	5, 7, 9, 13,
	17, 25, 33, 49,
	65, 97, 129, 193,
	257, 385, 513, 769,
	1025, 1537, 2049, 3073,
	4097, 6145, 8193, 12289,
	16385, 24577,
}

var offsetExtraBits = [...]uint8{
	0, 0, 0, 0,
	1, 1, 2, 2,
	3, 3, 4, 4,
	5, 5, 6, 6,
	7, 7, 8, 8,
	9, 9, 10, 10,
	11, 11, 12, 12,
	13, 13,
}

var precodeDecodeResults = func() (results [19]uint32) {
	for sym := range results {
		results[sym] = uint32(sym) << 16
	}
	return results
}()

var litlenDecodeResults = func() (results [maxLitlenSyms]uint32) {
	for sym := 0; sym < 256; sym++ {
		results[sym] = huffLiteral | uint32(sym)<<16
	}
	results[256] = huffExceptional | huffEndOfBlock
	for i, base := range lengthBases {
		results[257+i] = uint32(base)<<16 | uint32(lengthExtraBits[i])
	}
	results[286], results[287] = huffInvalid, huffInvalid
	return results
}()

var offsetDecodeResults = func() (results [maxOffsetSyms]uint32) {
	for sym, base := range offsetBases {
		results[sym] = uint32(base)<<16 | uint32(offsetExtraBits[sym])
	}
	results[30], results[31] = huffInvalid, huffInvalid
	return results
}()

type huffmanTableKind uint8

const (
	precodeTable huffmanTableKind = iota
	litlenTable
	offsetTable
)

// goInflater owns the decode-table scratch space. Keeping it pooled avoids a
// large stack frame and amortizes table storage across one-shot decodes.
type goInflater struct {
	precodeLens [19]uint8
	lens        [maxLitlenSyms + maxOffsetSyms]uint8

	precode [precodeTableSize]uint32
	litlen  [litlenTableSize]uint32
	offset  [offsetTableSize]uint32

	sortedEntries [maxLitlenSyms]uint32

	litlenBits uint8
	offsetBits uint8
	static     bool
}

var goInflaterPool = sync.Pool{
	New: func() any { return new(goInflater) },
}

// inflatePackZlibGo decodes the pack-specific zlib envelope used by the mmap
// fast path. It validates the two-byte header, decodes exactly one DEFLATE
// stream into dst, and returns the header plus raw-DEFLATE bytes consumed.
//
// The Adler-32 trailer is deliberately outside this contract. It is neither
// required nor validated, and any trailer or following pack data is left as
// trailing input. src and dst must not overlap.
func inflatePackZlibGo(src, dst []byte) (int, error) {
	if len(src) < 2 {
		return 0, errDeflateBadData
	}
	if err := validateZlibHeader(src[0], src[1]); err != nil {
		return 0, err
	}

	d := goInflaterPool.Get().(*goInflater)
	consumed, produced, err := d.inflateRaw(src[2:], dst)
	goInflaterPool.Put(d)
	if err != nil {
		return 0, err
	}
	if produced != len(dst) {
		return 0, errDeflateShortOutput
	}
	return consumed + 2, nil
}

type deflateBits struct {
	src   []byte
	buf   uint64
	nbits uint
	pos   int
}

func (r *deflateBits) refill() {
	if r.nbits > 56 || r.pos == len(r.src) {
		return
	}

	if len(r.src)-r.pos >= 8 {
		nbytes := int((64 - r.nbits) >> 3)
		word := binary.LittleEndian.Uint64(r.src[r.pos:])
		if nbytes != 8 {
			word &= uint64(1)<<(uint(nbytes)*8) - 1
		}
		r.buf |= word << r.nbits
		r.nbits += uint(nbytes) * 8
		r.pos += nbytes
		return
	}

	for r.nbits <= 56 && r.pos < len(r.src) {
		r.buf |= uint64(r.src[r.pos]) << r.nbits
		r.nbits += 8
		r.pos++
	}
}

func (r *deflateBits) read(n uint) (uint64, bool) {
	if n == 0 {
		return 0, true
	}
	if r.nbits < n {
		r.refill()
		if r.nbits < n {
			return 0, false
		}
	}
	v := r.buf & bitMask(n)
	r.buf >>= n
	r.nbits -= n
	return v, true
}

func (r *deflateBits) alignToByte() {
	padding := r.nbits & 7
	r.buf >>= padding
	r.nbits -= padding
	r.pos -= int(r.nbits >> 3)
	r.buf = 0
	r.nbits = 0
}

func (r *deflateBits) consumed() int {
	return r.pos - int(r.nbits>>3)
}

func bitMask(n uint) uint64 {
	if n == 64 {
		return ^uint64(0)
	}
	return uint64(1)<<n - 1
}

func (d *goInflater) inflateRaw(src, dst []byte) (int, int, error) {
	r := deflateBits{src: src}
	out := 0

	for {
		header, ok := r.read(3)
		if !ok {
			return 0, out, errDeflateTruncated
		}
		final := header&1 != 0

		switch (header >> 1) & 3 {
		case 0:
			r.alignToByte()
			if len(src)-r.pos < 4 {
				return 0, out, errDeflateTruncated
			}
			n := int(binary.LittleEndian.Uint16(src[r.pos:]))
			nn := binary.LittleEndian.Uint16(src[r.pos+2:])
			r.pos += 4
			if uint16(n) != ^nn {
				return 0, out, errDeflateBadData
			}
			if n > len(src)-r.pos {
				return 0, out, errDeflateTruncated
			}
			if n > len(dst)-out {
				return 0, out, errDeflateOutputOverrun
			}
			copy(dst[out:out+n], src[r.pos:r.pos+n])
			r.pos += n
			out += n

		case 1:
			if !d.loadFixedTables() {
				return 0, out, errDeflateBadData
			}
			var err error
			out, err = d.decodeHuffman(&r, dst, out)
			if err != nil {
				return 0, out, err
			}

		case 2:
			if !d.loadDynamicTables(&r) {
				return 0, out, errDeflateBadData
			}
			var err error
			out, err = d.decodeHuffman(&r, dst, out)
			if err != nil {
				return 0, out, err
			}

		default:
			return 0, out, errDeflateBadData
		}

		if final {
			if out != len(dst) {
				return 0, out, errDeflateShortOutput
			}
			return r.consumed(), out, nil
		}
	}
}

func (d *goInflater) loadFixedTables() bool {
	if d.static {
		return true
	}

	for i := 0; i < 144; i++ {
		d.lens[i] = 8
	}
	for i := 144; i < 256; i++ {
		d.lens[i] = 9
	}
	for i := 256; i < 280; i++ {
		d.lens[i] = 7
	}
	for i := 280; i < maxLitlenSyms; i++ {
		d.lens[i] = 8
	}
	for i := maxLitlenSyms; i < len(d.lens); i++ {
		d.lens[i] = 5
	}

	offsetBits, ok := d.buildTable(
		d.offset[:], d.lens[maxLitlenSyms:], offsetTable, offsetTableBits, maxCodeLen, false,
	)
	if !ok {
		return false
	}
	litlenBits, ok := d.buildTable(
		d.litlen[:], d.lens[:maxLitlenSyms], litlenTable, litlenTableBits, maxCodeLen, false,
	)
	if !ok {
		return false
	}
	d.offsetBits = uint8(offsetBits)
	d.litlenBits = uint8(litlenBits)
	d.static = true
	return true
}

var precodeOrder = [...]uint8{
	16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
}

func (d *goInflater) loadDynamicTables(r *deflateBits) bool {
	// A malformed dynamic block can fail after partially replacing either
	// decode table. Invalidate the fixed-table cache before touching any
	// scratch state so a pooled decoder can never reuse those partial tables.
	d.static = false

	counts, ok := r.read(14)
	if !ok {
		return false
	}
	numLitlen := 257 + int(counts&31)
	numOffset := 1 + int((counts>>5)&31)
	numPrecode := 4 + int((counts>>10)&15)

	clear(d.precodeLens[:])
	for i := 0; i < numPrecode; i++ {
		n, ok := r.read(3)
		if !ok {
			return false
		}
		d.precodeLens[precodeOrder[i]] = uint8(n)
	}

	preBits, ok := d.buildTable(
		d.precode[:], d.precodeLens[:], precodeTable, precodeTableBits, 7, false,
	)
	if !ok {
		return false
	}

	total := numLitlen + numOffset
	clear(d.lens[:total])
	for i := 0; i < total; {
		entry, _, ok := decodeTableEntry(r, d.precode[:], uint(preBits))
		if !ok {
			return false
		}
		sym := int(entry >> 16)
		switch {
		case sym < 16:
			d.lens[i] = uint8(sym)
			i++

		case sym == 16:
			if i == 0 {
				return false
			}
			extra, ok := r.read(2)
			if !ok {
				return false
			}
			n := 3 + int(extra)
			if n > total-i {
				return false
			}
			v := d.lens[i-1]
			for j := 0; j < n; j++ {
				d.lens[i+j] = v
			}
			i += n

		case sym == 17:
			extra, ok := r.read(3)
			if !ok {
				return false
			}
			n := 3 + int(extra)
			if n > total-i {
				return false
			}
			clear(d.lens[i : i+n])
			i += n

		case sym == 18:
			extra, ok := r.read(7)
			if !ok {
				return false
			}
			n := 11 + int(extra)
			if n > total-i {
				return false
			}
			clear(d.lens[i : i+n])
			i += n

		default:
			return false
		}
	}

	if d.lens[256] == 0 {
		return false
	}

	offsetBits, ok := d.buildTable(
		d.offset[:], d.lens[numLitlen:total], offsetTable, offsetTableBits, maxCodeLen, true,
	)
	if !ok {
		return false
	}
	litlenBits, ok := d.buildTable(
		d.litlen[:], d.lens[:numLitlen], litlenTable, litlenTableBits, maxCodeLen, false,
	)
	if !ok {
		return false
	}

	d.offsetBits = uint8(offsetBits)
	d.litlenBits = uint8(litlenBits)
	return true
}

func (d *goInflater) decodeHuffmanGo(r *deflateBits, dst []byte, out int) (int, error) {
	litlenBits := uint(d.litlenBits)
	litlenMask := bitMask(litlenBits)
	offsetBits := uint(d.offsetBits)
	offsetMask := bitMask(offsetBits)
	bitbuf := r.buf
	nbits := r.nbits
	pos := r.pos
	src := r.src
	inFastEnd := len(src) - 8
	outFastEnd := len(dst) - deflateFastOutputMargin
	var nextEntry uint32
	haveNext := false

	for out <= outFastEnd {
		if nbits < 48 {
			if pos > inFastEnd {
				break
			}
			// The top byte is loaded speculatively but left uncounted;
			// the next refill overlaps it, preserving exact consumption.
			bitbuf |= binary.LittleEndian.Uint64(src[pos:]) << nbits
			pos += 7 - int((nbits>>3)&7)
			nbits |= 56
		}

		var entry uint32
		if haveNext {
			entry = nextEntry
			haveNext = false
		} else {
			entry = d.litlen[int(bitbuf&litlenMask)]
		}
		if entry == 0 {
			return out, errDeflateBadData
		}
		var saved uint64
		if entry&huffSubtable != 0 {
			r.buf, r.nbits, r.pos = bitbuf, nbits, pos
			var ok bool
			entry, saved, ok = decodeTableEntry(r, d.litlen[:], litlenBits)
			if !ok {
				return out, errDeflateBadData
			}
			bitbuf, nbits, pos = r.buf, r.nbits, r.pos
		} else {
			n := uint(entry & huffBitCountMask)
			saved = bitbuf
			bitbuf >>= n
			nbits -= n
		}

		if entry&huffLiteral != 0 {
			dst[out] = byte(entry >> 16)
			out++
			if nbits < litlenBits {
				continue
			}
			entry = d.litlen[int(bitbuf&litlenMask)]
			if entry&huffLiteral == 0 {
				nextEntry = entry
				haveNext = true
				continue
			}
			n := uint(entry & huffBitCountMask)
			bitbuf >>= n
			nbits -= n
			dst[out] = byte(entry >> 16)
			out++

			if nbits < litlenBits {
				continue
			}
			entry = d.litlen[int(bitbuf&litlenMask)]
			if entry&huffLiteral == 0 {
				nextEntry = entry
				haveNext = true
				continue
			}
			n = uint(entry & huffBitCountMask)
			bitbuf >>= n
			nbits -= n
			dst[out] = byte(entry >> 16)
			out++
			continue
		}
		if entry&huffEndOfBlock != 0 {
			r.buf, r.nbits, r.pos = bitbuf, nbits, pos
			return out, nil
		}
		if entry&huffExceptional != 0 {
			return out, errDeflateBadData
		}

		codeBits := uint((entry >> 8) & huffBitCountMask)
		totalBits := uint(entry & huffBitCountMask)
		length := int(entry >> 16)
		length += int((saved &^ (^uint64(0) << totalBits)) >> codeBits)

		entry = d.offset[int(bitbuf&offsetMask)]
		if entry == 0 {
			return out, errDeflateBadData
		}
		if entry&huffSubtable != 0 {
			r.buf, r.nbits, r.pos = bitbuf, nbits, pos
			var ok bool
			entry, saved, ok = decodeTableEntry(r, d.offset[:], offsetBits)
			if !ok {
				return out, errDeflateBadData
			}
			bitbuf, nbits, pos = r.buf, r.nbits, r.pos
		} else {
			n := uint(entry & huffBitCountMask)
			saved = bitbuf
			bitbuf >>= n
			nbits -= n
		}
		if entry&huffExceptional != 0 {
			return out, errDeflateBadData
		}
		codeBits = uint((entry >> 8) & huffBitCountMask)
		totalBits = uint(entry & huffBitCountMask)
		offset := int(entry >> 16)
		offset += int((saved &^ (^uint64(0) << totalBits)) >> codeBits)
		if offset <= 0 || offset > out {
			return out, errDeflateBadData
		}

		if nbits >= litlenBits {
			nextEntry = d.litlen[int(bitbuf&litlenMask)]
			haveNext = true
		}
		if offset >= 8 && length <= deflateFastMatchCopy {
			// The loop margin proves the destination window, and the
			// validated offset proves the source window. [8]byte has
			// alignment 1; unsafe only removes redundant bounds checks.
			base := unsafe.Pointer(unsafe.SliceData(dst))
			d := unsafe.Add(base, out)
			s := unsafe.Add(base, out-offset)
			*(*[8]byte)(d) = *(*[8]byte)(s)
			*(*[8]byte)(unsafe.Add(d, 8)) = *(*[8]byte)(unsafe.Add(s, 8))
			*(*[8]byte)(unsafe.Add(d, 16)) = *(*[8]byte)(unsafe.Add(s, 16))
			*(*[8]byte)(unsafe.Add(d, 24)) = *(*[8]byte)(unsafe.Add(s, 24))
			*(*[8]byte)(unsafe.Add(d, 32)) = *(*[8]byte)(unsafe.Add(s, 32))
		} else if offset == 1 {
			d := dst[out : out+deflateFastOutputMargin]
			v := uint64(dst[out-1]) * 0x0101010101010101
			binary.LittleEndian.PutUint64(d[0:], v)
			binary.LittleEndian.PutUint64(d[8:], v)
			binary.LittleEndian.PutUint64(d[16:], v)
			binary.LittleEndian.PutUint64(d[24:], v)
			binary.LittleEndian.PutUint64(d[32:], v)
			for copied := deflateFastMatchCopy; copied < length; copied += 8 {
				binary.LittleEndian.PutUint64(d[copied:], v)
			}
		} else {
			copyMatch(dst, out, offset, length)
		}
		out += length
	}

	r.buf, r.nbits, r.pos = bitbuf, nbits, pos
	return d.decodeHuffmanTail(r, dst, out)
}

func (d *goInflater) decodeHuffmanTail(r *deflateBits, dst []byte, out int) (int, error) {
	litlenTable := d.litlen[:]
	litlenBits := uint(d.litlenBits)
	litlenMask := bitMask(litlenBits)
	offsetTable := d.offset[:]
	offsetBits := uint(d.offsetBits)
	offsetMask := bitMask(offsetBits)

	for {
		if r.nbits < litlenBits {
			r.refill()
		}
		entry := litlenTable[int(r.buf&litlenMask)]
		if entry == 0 {
			return out, errDeflateBadData
		}
		var saved uint64
		if entry&huffSubtable != 0 {
			var ok bool
			entry, saved, ok = decodeTableEntry(r, litlenTable, litlenBits)
			if !ok {
				return out, errDeflateBadData
			}
		} else {
			n := uint(entry & huffBitCountMask)
			if r.nbits < n {
				r.refill()
				if r.nbits < n {
					return out, errDeflateTruncated
				}
			}
			saved = r.buf
			r.buf >>= n
			r.nbits -= n
		}

		if entry&huffLiteral != 0 {
			if out == len(dst) {
				return out, errDeflateOutputOverrun
			}
			dst[out] = byte(entry >> 16)
			out++
			continue
		}
		if entry&huffEndOfBlock != 0 {
			return out, nil
		}
		if entry&huffExceptional != 0 {
			return out, errDeflateBadData
		}

		codeBits := uint((entry >> 8) & huffBitCountMask)
		totalBits := uint(entry & huffBitCountMask)
		length := int(entry >> 16)
		length += int((saved &^ (^uint64(0) << totalBits)) >> codeBits)
		if length > len(dst)-out {
			return out, errDeflateOutputOverrun
		}

		if r.nbits < offsetBits {
			r.refill()
		}
		entry = offsetTable[int(r.buf&offsetMask)]
		if entry == 0 {
			return out, errDeflateBadData
		}
		if entry&huffSubtable != 0 {
			var ok bool
			entry, saved, ok = decodeTableEntry(r, offsetTable, offsetBits)
			if !ok {
				return out, errDeflateBadData
			}
		} else {
			n := uint(entry & huffBitCountMask)
			if r.nbits < n {
				r.refill()
				if r.nbits < n {
					return out, errDeflateTruncated
				}
			}
			saved = r.buf
			r.buf >>= n
			r.nbits -= n
		}
		if entry&huffExceptional != 0 {
			return out, errDeflateBadData
		}
		codeBits = uint((entry >> 8) & huffBitCountMask)
		totalBits = uint(entry & huffBitCountMask)
		offset := int(entry >> 16)
		offset += int((saved &^ (^uint64(0) << totalBits)) >> codeBits)
		if offset <= 0 || offset > out {
			return out, errDeflateBadData
		}

		copyMatch(dst, out, offset, length)
		out += length
	}
}

func decodeTableEntry(r *deflateBits, table []uint32, tableBits uint) (uint32, uint64, bool) {
	if r.nbits < tableBits {
		r.refill()
	}
	entry := table[int(r.buf&bitMask(tableBits))]
	if entry == 0 {
		return 0, 0, false
	}

	if entry&huffSubtable != 0 {
		n := uint(entry & huffBitCountMask)
		if r.nbits < n {
			return 0, 0, false
		}
		r.buf >>= n
		r.nbits -= n

		subBits := uint((entry >> 8) & 0x1f)
		start := int(entry >> 16)
		if subBits > maxCodeLen || start >= len(table) {
			return 0, 0, false
		}
		if r.nbits < subBits {
			r.refill()
		}
		index := start + int(r.buf&bitMask(subBits))
		if index >= len(table) {
			return 0, 0, false
		}
		entry = table[index]
		if entry == 0 {
			return 0, 0, false
		}
	}

	n := uint(entry & huffBitCountMask)
	if r.nbits < n {
		r.refill()
		if r.nbits < n {
			return 0, 0, false
		}
	}
	saved := r.buf
	r.buf >>= n
	r.nbits -= n
	return entry, saved, true
}

func copyMatch(dst []byte, out, offset, length int) {
	end := out + length
	n := copy(dst[out:end], dst[out-offset:out])
	for n < length {
		n += copy(dst[out+n:end], dst[out:out+n])
	}
}

func (d *goInflater) buildTable(
	table []uint32,
	lens []uint8,
	kind huffmanTableKind,
	maxTableBits int,
	maxLen int,
	allowEmpty bool,
) (int, bool) {
	var decodeResults []uint32
	switch kind {
	case precodeTable:
		decodeResults = precodeDecodeResults[:]
	case litlenTable:
		decodeResults = litlenDecodeResults[:]
	case offsetTable:
		decodeResults = offsetDecodeResults[:]
	default:
		return 0, false
	}
	if len(table) == 0 || len(lens) > len(decodeResults) ||
		maxLen < 1 || maxLen > maxCodeLen ||
		maxTableBits < 1 || maxTableBits > maxLen {
		return 0, false
	}

	var count [maxCodeLen + 1]int
	used := 0
	actualMax := 0
	singleSym := 0
	for sym, n := range lens {
		if int(n) > maxLen {
			return 0, false
		}
		if n != 0 {
			count[n]++
			used++
			singleSym = sym
			if int(n) > actualMax {
				actualMax = int(n)
			}
		}
	}

	if used == 0 {
		if !allowEmpty {
			return 0, false
		}
		table[0] = huffInvalid
		return 0, true
	}

	left := 1
	for n := 1; n <= maxLen; n++ {
		left = (left << 1) - count[n]
		if left < 0 {
			return 0, false
		}
	}
	if left != 0 && !(used == 1 && actualMax == 1) {
		return 0, false
	}

	mainBits := min(maxTableBits, actualMax)
	mainSize := 1 << mainBits
	if mainSize > len(table) {
		return 0, false
	}
	if left != 0 {
		// A permitted singleton code uses only half its one-bit table.
		// Keep the other half explicitly invalid so fast decoders never
		// need a separate zero-entry check.
		table[0] = decodeResults[singleSym] + 0x101
		table[1] = huffInvalid
		return mainBits, true
	}

	// Sort symbols by codeword length and then by symbol value. Canonical
	// codewords have this same ordering.
	var offsets [maxCodeLen + 1]int
	for n := 1; n < actualMax; n++ {
		offsets[n+1] = offsets[n] + count[n]
	}
	nextOffset := offsets
	for sym, n := range lens {
		if n == 0 {
			continue
		}
		d.sortedEntries[nextOffset[n]] = decodeResults[sym]
		nextOffset[n]++
	}

	sorted := d.sortedEntries[:used]
	sortedPos := 0
	codeword := 0
	codeBits := 1
	for count[codeBits] == 0 {
		codeBits++
	}
	remaining := count[codeBits]
	tableEnd := 1 << codeBits

	// Fill short-code entries while incrementally doubling the main table.
	// This replaces strided stores with contiguous copies.
	for codeBits <= mainBits {
		for {
			table[codeword] = sorted[sortedPos] + uint32(codeBits)*0x101
			sortedPos++
			remaining--
			if codeword == tableEnd-1 {
				for codeBits < mainBits {
					copy(table[tableEnd:tableEnd*2], table[:tableEnd])
					tableEnd *= 2
					codeBits++
				}
				return mainBits, true
			}
			codeword = nextReversedCode(codeword, codeBits)
			if remaining == 0 {
				break
			}
		}

		for {
			codeBits++
			if codeBits <= mainBits {
				copy(table[tableEnd:tableEnd*2], table[:tableEnd])
				tableEnd *= 2
			}
			remaining = count[codeBits]
			if remaining != 0 {
				break
			}
		}
	}

	// Long codewords are grouped by their main-table prefix. Size and fill
	// each subtable as its first codeword is reached.
	tableEnd = mainSize
	mainMask := mainSize - 1
	subtablePrefix := -1
	subtableStart := 0
	subtableEnd := 0

	for {
		prefix := codeword & mainMask
		if prefix != subtablePrefix {
			subtablePrefix = prefix
			subtableStart = tableEnd
			subtableBits := codeBits - mainBits
			codespaceUsed := remaining
			for codespaceUsed < 1<<subtableBits {
				subtableBits++
				codespaceUsed = (codespaceUsed << 1) +
					count[mainBits+subtableBits]
			}
			subtableEnd = subtableStart + 1<<subtableBits
			if subtableEnd > len(table) {
				return 0, false
			}
			tableEnd = subtableEnd
			table[prefix] = uint32(subtableStart)<<16 |
				huffExceptional | huffSubtable |
				uint32(subtableBits)<<8 | uint32(mainBits)
		}

		entry := sorted[sortedPos] + uint32(codeBits-mainBits)*0x101
		sortedPos++
		stride := 1 << (codeBits - mainBits)
		for i := subtableStart + (codeword >> mainBits); i < subtableEnd; i += stride {
			table[i] = entry
		}

		if codeword == 1<<codeBits-1 {
			return mainBits, true
		}
		codeword = nextReversedCode(codeword, codeBits)
		remaining--
		for remaining == 0 {
			codeBits++
			remaining = count[codeBits]
		}
	}
}

func nextReversedCode(codeword, codeBits int) int {
	bit := 1 << (bits.Len(uint(codeword^((1<<codeBits)-1))) - 1)
	return codeword&(bit-1) | bit
}

func reverseCode(code, n int) int {
	return int(bits.Reverse16(uint16(code)) >> (16 - n))
}
