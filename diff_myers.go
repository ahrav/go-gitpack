package objstore

import (
	"bytes"
	"sync"
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
}

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
	diffScratchPool.Put(sc)
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
	width := 2*half + 3
	if cap(sc.vf) < width {
		sc.vf = make([]int32, width)
		sc.vr = make([]int32, width)
	}
	vf, vr := sc.vf[:width], sc.vr[:width]

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
		xs, ys, xe, ye, ok := middleSnake(oldLines[r.a0:r.a1], newLines[r.b0:r.b1], vf, vr, half)
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

// middleSnake finds a snake on the middle of a shortest edit path between a
// and b: forward and reverse frontier searches advance by one edit each
// until they overlap on a diagonal. It returns the snake's start and end
// (xs, ys)-(xe, ye) in a/b coordinates, and false when the searches exceed
// half edits each. vf and vr must hold at least 2*half+3 entries.
func middleSnake(a, b []string, vf, vr []int32, half int) (xs, ys, xe, ye int, ok bool) {
	n, m := len(a), len(b)
	delta := n - m
	odd := delta&1 != 0
	limit := min((n+m+1)/2, half)
	offset := limit + 1
	vf[offset+1] = 0
	vr[offset+1] = 0
	for d := 0; d <= limit; d++ {
		// Forward step over diagonals k = x - y.
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && vf[offset+k-1] < vf[offset+k+1]) {
				x = int(vf[offset+k+1])
			} else {
				x = int(vf[offset+k-1]) + 1
			}
			y := x - k
			x0, y0 := x, y
			for x < n && y < m && a[x] == b[y] {
				x++
				y++
			}
			vf[offset+k] = int32(x)
			// A reverse point on the same diagonal lies at reverse diagonal
			// delta-k; the paths overlap once the forward x reaches it.
			if odd {
				if kr := delta - k; kr >= -(d-1) && kr <= d-1 && x+int(vr[offset+kr]) >= n {
					return x0, y0, x, y, true
				}
			}
		}
		// Reverse step: the same search over the reversed sequences, with
		// x', y' counted from the ends of a and b.
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && vr[offset+k-1] < vr[offset+k+1]) {
				x = int(vr[offset+k+1])
			} else {
				x = int(vr[offset+k-1]) + 1
			}
			y := x - k
			x0, y0 := x, y
			for x < n && y < m && a[n-1-x] == b[m-1-y] {
				x++
				y++
			}
			vr[offset+k] = int32(x)
			if !odd {
				if kf := delta - k; kf >= -d && kf <= d && x+int(vf[offset+kf]) >= n {
					return n - x, m - y, n - x0, m - y0, true
				}
			}
		}
	}
	return 0, 0, 0, 0, false
}

// commonSuffixLineBoundary returns the length of the longest common byte
// suffix of a and b that starts immediately after a '\n' or at the start of
// a side. Trimming it leaves remainders that end on a line boundary on both
// sides, so line-based diffing of the remainders is equivalent to diffing
// the full inputs.
func commonSuffixLineBoundary(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[len(a)-1-i] == b[len(b)-1-i] {
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
