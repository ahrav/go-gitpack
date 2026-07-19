//go:build (arm64 || amd64) && !purego && !(gitpack_libdeflate && cgo)

package objstore

const inflateFastYieldBytes = 64 << 10

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
