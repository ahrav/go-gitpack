package objstore

import (
	"bytes"
	"sync"
	"unsafe"
)

// myersWorkBudget bounds the line comparisons one shortest-edit-script
// search may spend. The middle-snake search costs O((N+M)·D), so the budget
// admits every edit distance on files up to about 5,800 lines and an edit
// distance of about 670 on a fifty-thousand-line file, keeping a wholesale
// rewrite of a large file under about a hundred milliseconds. Pairs beyond
// it fall back to the greedy forward walk, whose output on such pairs is
// dominated by lines that are new in any alignment. On the rails history
// the budget trades 3% more emitted lines for 18% less scan CPU against a
// budget four times larger.
const myersWorkBudget = 1 << 24

// diffScratch holds the per-worker buffers of one shortest-edit-script
// search: the forward and reverse frontiers indexed by diagonal, the
// pending subproblems, and the added-line flags of the new side.
type diffScratch struct {
	vf, vr []int32
	stack  []diffRange
	added  []bool
	fa, fb []uint64
	// seen is the fingerprint bitmap editDistanceLowerBound probes.
	seen []uint64
}

// myersBoundLines is the pair size (old plus new lines) from which
// addedLinesMyers checks the edit-distance lower bound before searching. A
// search that ends at the budget costs up to the whole budget, which only a
// pair this large can reach ((n+m)·(n+m)/2 comparisons), and the bound costs
// a few nanoseconds per line.
const myersBoundLines = 2048

// myersBoundBits sizes the fingerprint bitmap of editDistanceLowerBound:
// 2^16 bits, 8 KiB, so clearing and probing it stay in L1.
const myersBoundBits = 16

// diffRange is one pending subproblem: old[a0:a1] against new[b0:b1].
type diffRange struct{ a0, a1, b0, b1 int }

var diffScratchPool = sync.Pool{New: func() any { return &diffScratch{} }}

func getDiffScratch() *diffScratch { return diffScratchPool.Get().(*diffScratch) }

// putDiffScratch returns sc to the pool; the added flags are capped so one
// huge blob cannot pin them for the process lifetime.
func putDiffScratch(sc *diffScratch) {
	const maxPooledFlags = 1 << 20
	if cap(sc.added) > maxPooledFlags {
		sc.added = nil
	}
	if cap(sc.fa) > maxPooledFlags {
		sc.fa, sc.fb = nil, nil
	}
	diffScratchPool.Put(sc)
}

// lineFingerprints fills dst with one cheap 64-bit fingerprint per line:
// the length mixed with the first and last eight bytes. Equal lines have
// equal fingerprints, and the frontier searches compare fingerprints before
// strings, so the common unequal comparison costs one word compare instead
// of two pointer dereferences and a length check.
func lineFingerprints(dst []uint64, lines []string) []uint64 {
	dst = dst[:0]
	for _, l := range lines {
		var head, tail uint64
		switch {
		case len(l) >= 8:
			head = le64s(l[:8])
			tail = le64s(l[len(l)-8:])
		default:
			for i := 0; i < len(l); i++ {
				head = head<<8 | uint64(l[i])
			}
		}
		dst = append(dst, (uint64(len(l))*0x9E3779B97F4A7C15)^head^(tail*0xC2B2AE3D27D4EB4F))
	}
	return dst
}

// le64s reads the first eight bytes of s as a little-endian word; the
// compiler fuses the shifts into one load.
func le64s(s string) uint64 {
	return uint64(s[0]) | uint64(s[1])<<8 | uint64(s[2])<<16 | uint64(s[3])<<24 |
		uint64(s[4])<<32 | uint64(s[5])<<40 | uint64(s[6])<<48 | uint64(s[7])<<56
}

// addedLinesMyers marks in sc.added[i] whether newLines[i] lies outside a
// longest common subsequence of oldLines and newLines, using Myers' linear
// space shortest-edit-script algorithm: each subproblem is split at a middle
// snake found by forward and reverse frontier searches that meet halfway,
// and the two halves are solved in turn. Memory is O(D) for the frontiers
// plus O(M) for the flags. It reports false, leaving the flags unspecified,
// when the search would exceed myersWorkBudget.
func addedLinesMyers(oldLines, newLines []string, sc *diffScratch) bool {
	n, m := len(oldLines), len(newLines)
	if cap(sc.added) < m {
		sc.added = make([]bool, m)
	}
	added := sc.added[:m]
	for i := range added {
		added[i] = false
	}
	// The top-level search explores forward and reverse distances up to
	// half each, so its cost is about (n+m)·half comparisons; subproblems
	// have smaller edit distances and sizes.
	half := min((n+m+1)/2, myersWorkBudget/(n+m+1)+1)
	// The edit distance is at least the length difference, so a pair the
	// budget cannot cover is known before any comparison; a large addition
	// to a small file is the common shape.
	if n-m > 2*half || m-n > 2*half {
		return false
	}
	width := 2*half + 3
	if cap(sc.vf) < width {
		sc.vf = make([]int32, width)
		sc.vr = make([]int32, width)
	}
	vf, vr := sc.vf[:width], sc.vr[:width]
	sc.fa = lineFingerprints(sc.fa, oldLines)
	sc.fb = lineFingerprints(sc.fb, newLines)
	fa, fb := sc.fa, sc.fb
	// The top-level middle snake is found at forward step d for an odd
	// edit distance 2d-1 and at reverse step d for an even one 2d, both
	// within the search iff D <= 2*limit. A lower bound above that is a
	// search that would run to the budget and fail; the greedy walk takes
	// over without the search, with the same result.
	if n+m >= myersBoundLines {
		limit := min((n+m+1)/2, half)
		if editDistanceLowerBound(fa, fb, sc) > 2*limit {
			return false
		}
	}

	sc.stack = append(sc.stack[:0], diffRange{0, n, 0, m})
	for len(sc.stack) > 0 {
		r := sc.stack[len(sc.stack)-1]
		sc.stack = sc.stack[:len(sc.stack)-1]
		// Strip the common prefix and suffix of the subproblem; after that
		// an empty side is a base case and a non-empty pair has edit
		// distance at least two, so the middle snake splits it into two
		// strictly smaller subproblems.
		for r.a0 < r.a1 && r.b0 < r.b1 && oldLines[r.a0] == newLines[r.b0] {
			r.a0++
			r.b0++
		}
		for r.a0 < r.a1 && r.b0 < r.b1 && oldLines[r.a1-1] == newLines[r.b1-1] {
			r.a1--
			r.b1--
		}
		if r.b0 == r.b1 {
			continue
		}
		if r.a0 == r.a1 {
			for i := r.b0; i < r.b1; i++ {
				added[i] = true
			}
			continue
		}
		xs, ys, xe, ye, ok := middleSnake(oldLines[r.a0:r.a1], newLines[r.b0:r.b1], fa[r.a0:r.a1], fb[r.b0:r.b1], vf, vr, half)
		if !ok {
			return false
		}
		sc.stack = append(sc.stack,
			diffRange{r.a0 + xe, r.a1, r.b0 + ye, r.b1},
			diffRange{r.a0, r.a0 + xs, r.b0, r.b0 + ys},
		)
	}
	return true
}

// editDistanceLowerBound returns a lower bound on the edit distance between
// the line sequences with fingerprints fa and fb. A line whose fingerprint
// occurs nowhere on the other side is outside every common subsequence, so
// LCS <= min(m - uniqueB, n - uniqueA) and D = n + m - 2*LCS is at least
// max(n - m + 2*uniqueB, m - n + 2*uniqueA). Membership is tested through a
// bitmap over the low fingerprint bits: a bitmap hit for an absent line only
// lowers the count of lines known unique, so the bound stays a lower bound.
func editDistanceLowerBound(fa, fb []uint64, sc *diffScratch) int {
	const words = 1 << (myersBoundBits - 6)
	if cap(sc.seen) < 2*words {
		sc.seen = make([]uint64, 2*words)
	}
	seenA, seenB := sc.seen[:words], sc.seen[words:2*words]
	clear(seenA)
	clear(seenB)
	const mask = 1<<myersBoundBits - 1
	for _, f := range fa {
		seenA[(f&mask)>>6] |= 1 << (f & 63)
	}
	for _, f := range fb {
		seenB[(f&mask)>>6] |= 1 << (f & 63)
	}
	uniqueA, uniqueB := 0, 0
	for _, f := range fa {
		if seenB[(f&mask)>>6]&(1<<(f&63)) == 0 {
			uniqueA++
		}
	}
	for _, f := range fb {
		if seenA[(f&mask)>>6]&(1<<(f&63)) == 0 {
			uniqueB++
		}
	}
	n, m := len(fa), len(fb)
	return max(n-m+2*uniqueB, m-n+2*uniqueA)
}

// middleSnake finds a snake on the middle of a shortest edit path between a
// and b: forward and reverse frontier searches advance by one edit each
// until they overlap on a diagonal. It returns the snake's start and end
// (xs, ys)-(xe, ye) in a/b coordinates, and false when the searches exceed
// half edits each. fa and fb are the lines' fingerprints; vf and vr must
// hold at least 2*half+3 entries.
func middleSnake(a, b []string, fa, fb []uint64, vf, vr []int32, half int) (xs, ys, xe, ye int, ok bool) {
	n, m := len(a), len(b)
	delta := n - m
	odd := delta&1 != 0
	limit := min((n+m+1)/2, half)
	offset := limit + 1
	// The frontier loops below are the search's inner loop: every step
	// reads two or three frontier entries by a diagonal index the compiler
	// cannot bound, and every snake step reads a fingerprint and a line
	// from each side by an index it has just compared against n and m. The
	// accesses go through raw pointers so the loop carries no bounds
	// checks; the indexes stay in range by construction: frontier entries
	// offset+k with k in [-d-1, d+1] and d <= limit lie in [0, 2*limit+2]
	// and the frontiers hold 2*half+3 >= 2*limit+3 entries (the caller
	// sizes them for half), and x < n, y < m hold at every snake step.
	fp := frontier{unsafe.Pointer(unsafe.SliceData(vf))}
	rp := frontier{unsafe.Pointer(unsafe.SliceData(vr))}
	side1 := diffSide{lines: unsafe.Pointer(unsafe.SliceData(a)), fps: unsafe.Pointer(unsafe.SliceData(fa))}
	side2 := diffSide{lines: unsafe.Pointer(unsafe.SliceData(b)), fps: unsafe.Pointer(unsafe.SliceData(fb))}
	_ = vf[2*limit+2]
	_ = vr[2*limit+2]
	fp.set(offset+1, 0)
	rp.set(offset+1, 0)
	for d := 0; d <= limit; d++ {
		// Forward step over diagonals k = x - y.
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && fp.at(offset+k-1) < fp.at(offset+k+1)) {
				x = fp.at(offset + k + 1)
			} else {
				x = fp.at(offset+k-1) + 1
			}
			y := x - k
			x0, y0 := x, y
			for x < n && y < m && side1.fp(x) == side2.fp(y) && side1.line(x) == side2.line(y) {
				x++
				y++
			}
			fp.set(offset+k, x)
			// A reverse point on the same diagonal lies at reverse diagonal
			// delta-k; the paths overlap once the forward x reaches it.
			if odd {
				if kr := delta - k; kr >= -(d-1) && kr <= d-1 && x+rp.at(offset+kr) >= n {
					return x0, y0, x, y, true
				}
			}
		}
		// Reverse step: the same search over the reversed sequences, with
		// x', y' counted from the ends of a and b.
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && rp.at(offset+k-1) < rp.at(offset+k+1)) {
				x = rp.at(offset + k + 1)
			} else {
				x = rp.at(offset+k-1) + 1
			}
			y := x - k
			x0, y0 := x, y
			for x < n && y < m && side1.fp(n-1-x) == side2.fp(m-1-y) && side1.line(n-1-x) == side2.line(m-1-y) {
				x++
				y++
			}
			rp.set(offset+k, x)
			if !odd {
				if kf := delta - k; kf >= -d && kf <= d && x+fp.at(offset+kf) >= n {
					return n - x, m - y, n - x0, m - y0, true
				}
			}
		}
	}
	return 0, 0, 0, 0, false
}

// frontier is an unchecked view of a middle-snake frontier array; see the
// bounds argument in middleSnake.
type frontier struct{ p unsafe.Pointer }

func (f frontier) at(i int) int     { return int(*(*int32)(unsafe.Add(f.p, uintptr(i)*4))) }
func (f frontier) set(i int, v int) { *(*int32)(unsafe.Add(f.p, uintptr(i)*4)) = int32(v) }

// diffSide is an unchecked view of one side's lines and fingerprints; see
// the bounds argument in middleSnake.
type diffSide struct{ lines, fps unsafe.Pointer }

func (s diffSide) fp(i int) uint64 { return *(*uint64)(unsafe.Add(s.fps, uintptr(i)*8)) }
func (s diffSide) line(i int) string {
	return *(*string)(unsafe.Add(s.lines, uintptr(i)*unsafe.Sizeof("")))
}

// commonSuffixLineBoundary returns the length of the longest common byte
// suffix of a and b that starts immediately after a '\n' or at the start of
// a side. Trimming it leaves remainders that end on a line boundary on both
// sides, so line-based diffing of the remainders is equivalent to diffing
// the full inputs.
func commonSuffixLineBoundary(a, b []byte) int {
	n := min(len(a), len(b))
	la, lb := len(a), len(b)
	i := 0
	// Consecutive versions share most of their tail once the common prefix
	// is gone, so the bulk of it is compared a chunk at a time through the
	// runtime's vectorized equality, then the mismatching chunk is narrowed
	// word- and byte-wise; the byte loop alone was 15% of a scan's CPU.
	const chunk = 256
	for i+chunk <= n && bytes.Equal(a[la-i-chunk:la-i], b[lb-i-chunk:lb-i]) {
		i += chunk
	}
	for i+8 <= n && le64(a[la-i-8:]) == le64(b[lb-i-8:]) {
		i += 8
	}
	for i < n && a[la-1-i] == b[lb-1-i] {
		i++
	}
	if i == 0 {
		return 0
	}
	startA, startB := len(a)-i, len(b)-i
	if (startA == 0 || a[startA-1] == '\n') && (startB == 0 || b[startB-1] == '\n') {
		return i
	}
	// Snap forward to the first newline within the common suffix.
	j := bytes.IndexByte(a[startA:], '\n')
	if j < 0 {
		return 0
	}
	return i - j - 1
}
