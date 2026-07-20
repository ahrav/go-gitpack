//go:build amd64 && !purego && !(gitpack_libdeflate && cgo)

package objstore

// inflateFastAMD64MinLitlenBits gates kernel eligibility: below this
// litlen-table size, small dynamic tables favor Go's overlap-aware match
// copier over the assembly fast loop.
const inflateFastAMD64MinLitlenBits = 7

// inflateFastMinLitlenBits is this architecture's eligibility threshold for
// the shared decodeHuffman driver in inflate_fast_state.go.
const inflateFastMinLitlenBits = inflateFastAMD64MinLitlenBits

//go:noescape
func inflateHuffmanFastAMD64(state *inflateFastState)

// inflateHuffmanFastKernel adapts the shared decodeHuffman driver to the
// amd64 assembly kernel.
func inflateHuffmanFastKernel(state *inflateFastState) {
	inflateHuffmanFastAMD64(state)
}
