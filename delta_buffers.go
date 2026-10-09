package objstore

import (
	"math/bits"
	"sync"
)

// Multi-hop delta reconstruction writes each hop's result into a buffer sized
// to that hop's target. Hops the offset cache will retain write into an
// exact-size allocation the cache then owns; every other hop borrows from
// this pool and returns the buffer once the next hop has consumed it.
//
// Buffers are grouped into power-of-two size classes so a request is served
// by a buffer at most 2x its size. sync.Pool bounds idle memory by the
// collector's cycle: a buffer unused across two GC cycles is freed, so idle
// retention is proportional to recent demand rather than to a fixed arena
// count. Requests above the largest class allocate directly and are never
// pooled, which keeps one huge object from pinning memory for the process
// lifetime.

const (
	// deltaBufMinShift is the smallest size class, 4 KiB: smaller targets
	// still cost one 4 KiB buffer, which keeps the class count small.
	deltaBufMinShift = 12
	// deltaBufMaxShift is the largest pooled size class, 16 MiB.
	deltaBufMaxShift = 24
	deltaBufClasses  = deltaBufMaxShift - deltaBufMinShift + 1
)

var deltaBufPools [deltaBufClasses]sync.Pool

// deltaBufClass returns the size class whose buffers hold at least n bytes,
// or -1 when n exceeds the largest pooled class.
func deltaBufClass(n int) int {
	if n <= 1<<deltaBufMinShift {
		return 0
	}
	shift := bits.Len(uint(n - 1))
	if shift > deltaBufMaxShift {
		return -1
	}
	return shift - deltaBufMinShift
}

// getDeltaBuf returns a zero-length buffer with capacity >= n. The contents
// beyond len are unspecified: callers write every byte they later read.
func getDeltaBuf(n int) []byte {
	class := deltaBufClass(n)
	if class < 0 {
		return make([]byte, 0, n)
	}
	if p, ok := deltaBufPools[class].Get().(*[]byte); ok {
		return (*p)[:0]
	}
	return make([]byte, 0, 1<<(class+deltaBufMinShift))
}

// putDeltaBuf returns a buffer obtained from getDeltaBuf. Buffers whose
// capacity is not a pooled class size (direct allocations) are dropped.
func putDeltaBuf(b []byte) {
	c := cap(b)
	if c == 0 || c&(c-1) != 0 {
		return
	}
	shift := bits.Len(uint(c)) - 1
	if shift < deltaBufMinShift || shift > deltaBufMaxShift {
		return
	}
	b = b[:0]
	deltaBufPools[shift-deltaBufMinShift].Put(&b)
}
