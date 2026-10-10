// commit_order.go
//
// Topological commit ordering using Kahn's algorithm backed by a min-heap.
//
// The primary goal is to produce a deterministic parent-before-child ordering
// of commits so that every parent is visited before any of its children. This
// is the natural requirement for incremental secret-scanning: we want to scan
// a parent's tree changes before its child's, so the "seen" set grows in a
// predictable order.
//
// The algorithm works in three steps:
//  1. Build in-degree and child lists over commits whose parents are in the
//     input set.
//  2. Seed a min-heap with all root commits (in-degree == 0).
//  3. Pop the minimum-timestamp commit, decrement children's in-degree, and
//     push newly zero-in-degree children onto the heap.
//
// If the input contains cycles (which can happen with grafted or corrupt
// history), a deterministic timestamp-then-OID fallback appends the remaining
// commits so we always return every commit exactly once.
package objstore

import (
	"bytes"
	"slices"
)

// commitOrderHeap is a min-heap of commit indices ordered by ascending
// committer timestamp, with lexicographic OID comparison as the tie-breaker
// so the output order is fully deterministic regardless of input order.
// Indices refer to the commits slice; the heap holds int32 values so pushes
// and pops move four bytes and allocate nothing.
type commitOrderHeap struct {
	commits []commitInfo
	items   []int32
}

func (h *commitOrderHeap) less(a, b int32) bool {
	ca, cb := &h.commits[a], &h.commits[b]
	if ca.Timestamp != cb.Timestamp {
		return ca.Timestamp < cb.Timestamp
	}
	return bytes.Compare(ca.OID[:], cb.OID[:]) < 0
}

func (h *commitOrderHeap) push(i int32) {
	h.items = append(h.items, i)
	j := len(h.items) - 1
	for j > 0 {
		parent := (j - 1) / 2
		if !h.less(h.items[j], h.items[parent]) {
			break
		}
		h.items[j], h.items[parent] = h.items[parent], h.items[j]
		j = parent
	}
}

func (h *commitOrderHeap) pop() int32 {
	top := h.items[0]
	last := len(h.items) - 1
	h.items[0] = h.items[last]
	h.items = h.items[:last]
	j := 0
	for {
		l := 2*j + 1
		if l >= last {
			break
		}
		m := l
		if r := l + 1; r < last && h.less(h.items[r], h.items[l]) {
			m = r
		}
		if !h.less(h.items[m], h.items[j]) {
			break
		}
		h.items[j], h.items[m] = h.items[m], h.items[j]
		j = m
	}
	return top
}

// commitParents holds every commit's parents as positions in the commits
// slice, -1 for a parent outside it, in one flat array:
// idx[start[i]:start[i+1]] are commit i's parents, in header order.
type commitParents struct {
	start []int32
	idx   []int32
}

// orderCommitsParentFirstIndexed is orderCommitsParentFirst over parents
// already resolved to positions. It also returns perm, the position in
// commits of each element of the result, so callers that index the result
// can remap positions without a second OID lookup.
func orderCommitsParentFirstIndexed(commits []commitInfo, parents commitParents) (out []commitInfo, perm []int32) {
	n := len(commits)
	if n < 2 {
		out = make([]commitInfo, n)
		copy(out, commits)
		perm = make([]int32, n)
		for i := range perm {
			perm[i] = int32(i)
		}
		return out, perm
	}

	// Count in-degrees and children per parent, then lay the child lists
	// out contiguously: children of commit p are
	// childList[childStart[p]:childStart[p+1]].
	inDegree := make([]int32, n)
	childStart := make([]int32, n+1)
	for i := range commits {
		for _, pi := range parents.idx[parents.start[i]:parents.start[i+1]] {
			if pi < 0 {
				continue
			}
			inDegree[i]++
			childStart[pi+1]++
		}
	}
	for i := 1; i <= n; i++ {
		childStart[i] += childStart[i-1]
	}
	childList := make([]int32, childStart[n])
	fill := make([]int32, n)
	copy(fill, childStart[:n])
	for i := range commits {
		for _, pi := range parents.idx[parents.start[i]:parents.start[i+1]] {
			if pi < 0 {
				continue
			}
			childList[fill[pi]] = int32(i)
			fill[pi]++
		}
	}

	q := commitOrderHeap{commits: commits, items: make([]int32, 0, n)}
	for i := range commits {
		if inDegree[i] == 0 {
			q.push(int32(i))
		}
	}

	out = make([]commitInfo, 0, n)
	perm = make([]int32, 0, n)
	emitted := make([]bool, n)
	for len(q.items) > 0 {
		i := q.pop()
		out = append(out, commits[i])
		perm = append(perm, i)
		emitted[i] = true
		for _, child := range childList[childStart[i]:childStart[i+1]] {
			inDegree[child]--
			if inDegree[child] == 0 {
				q.push(child)
			}
		}
	}

	// Defensive fallback for malformed input with cycles.
	if len(out) < n {
		rest := make([]int32, 0, n-len(out))
		for i := range commits {
			if emitted[i] {
				continue
			}
			rest = append(rest, int32(i))
		}

		slices.SortFunc(rest, func(a, b int32) int {
			ca, cb := &commits[a], &commits[b]
			if ca.Timestamp < cb.Timestamp {
				return -1
			}
			if ca.Timestamp > cb.Timestamp {
				return 1
			}
			return bytes.Compare(ca.OID[:], cb.OID[:])
		})
		for _, i := range rest {
			out = append(out, commits[i])
			perm = append(perm, i)
		}
	}

	return out, perm
}
