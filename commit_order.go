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
	"encoding/binary"
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

// orderCommitsParentFirst returns a deterministic parent-before-child ordering
// of the input commits using Kahn's algorithm with a min-heap priority queue.
//
// Algorithm:
//  1. Index every commit by OID and compute its in-degree (number of parents
//     that are also in the input set). Child lists are stored in one flat
//     array indexed by parent (CSR layout), so the graph costs two int32
//     slices and one OID-to-index map.
//  2. Push all zero-in-degree (root) commits into a min-heap ordered by
//     (timestamp, OID).
//  3. Repeatedly pop the minimum element, emit it, and decrement the
//     in-degree of each of its children. When a child reaches in-degree 0,
//     push it onto the heap.
//
// Cycle fallback: if the input graph contains cycles (e.g. grafted history),
// the main loop will terminate before emitting every commit. The remaining
// commits are sorted by (timestamp, OID) and appended so that the caller
// always receives exactly len(commits) results.
//
// Determinism guarantee: for any fixed input set the output order is fully
// reproducible, regardless of Go map iteration order, because the heap
// tie-breaks on OID bytes.
func orderCommitsParentFirst(commits []commitInfo) []commitInfo {
	if len(commits) < 2 {
		out := make([]commitInfo, len(commits))
		copy(out, commits)
		return out
	}
	parents := commitParentIndexes(commits)
	out, _ := orderCommitsParentFirstIndexed(commits, parents)
	return out
}

// commitParents holds every commit's parents as positions in the commits
// slice, -1 for a parent outside it, in one flat array:
// idx[start[i]:start[i+1]] are commit i's parents, in header order.
type commitParents struct {
	start []int32
	idx   []int32
}

// commitParentIndexes resolves the parents of commits to positions through
// a temporary OID index.
func commitParentIndexes(commits []commitInfo) commitParents {
	byOID := newCommitIndex(commits)
	start := make([]int32, len(commits)+1)
	for i := range commits {
		start[i+1] = start[i] + int32(len(commits[i].ParentOIDs))
	}
	idx := make([]int32, start[len(commits)])
	for i := range commits {
		dst := idx[start[i]:start[i+1]]
		for j, p := range commits[i].ParentOIDs {
			if pi, ok := byOID.lookup(p); ok {
				dst[j] = pi
			} else {
				dst[j] = -1
			}
		}
	}
	return commitParents{start: start, idx: idx}
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

// commitIndex maps commit OIDs to their positions in a commits slice with an
// open-addressing table keyed by the OID's leading word. Slots hold the
// position plus one, so zero marks an empty slot; a probe compares the full
// OID against the commits slice before accepting a hit.
type commitIndex struct {
	commits []commitInfo
	slots   []int32
	mask    uint64
}

func newCommitIndex(commits []commitInfo) *commitIndex {
	size := 1
	for size < 2*len(commits) {
		size <<= 1
	}
	ix := &commitIndex{commits: commits, slots: make([]int32, size), mask: uint64(size - 1)}
	for i := range commits {
		j := binary.BigEndian.Uint64(commits[i].OID[:8]) & ix.mask
		for ix.slots[j] != 0 {
			j = (j + 1) & ix.mask
		}
		ix.slots[j] = int32(i) + 1
	}
	return ix
}

func (ix *commitIndex) lookup(oid Hash) (int32, bool) {
	for j := binary.BigEndian.Uint64(oid[:8]) & ix.mask; ; j = (j + 1) & ix.mask {
		slot := ix.slots[j]
		if slot == 0 {
			return 0, false
		}
		if ix.commits[slot-1].OID == oid {
			return slot - 1, true
		}
	}
}
