//go:build arm64 && !purego && !(gitpack_libdeflate && cgo)

package objstore

const (
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

	// inflateFastMinLitlenBits is this architecture's eligibility
	// threshold for the shared decodeHuffman driver in
	// inflate_fast_state.go. The arm64 kernel handles every table size,
	// so the driver never routes to the Go decoder on table-size grounds.
	inflateFastMinLitlenBits = 0
)

//go:noescape
func inflateHuffmanFastArm64(state *inflateFastState)

// inflateHuffmanFastKernel adapts the shared decodeHuffman driver to the
// arm64 assembly kernel.
func inflateHuffmanFastKernel(state *inflateFastState) {
	inflateHuffmanFastArm64(state)
}
