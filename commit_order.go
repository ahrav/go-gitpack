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
//  1. Build an in-degree map over commits whose parents are in the input set.
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
	"container/heap"
	"slices"
)

// commitQueueItem is a lightweight pair of (object-ID, committer-timestamp)
// used as the element type for the min-heap priority queue. Keeping only
// these two fields avoids storing full commitInfo values in the heap, which
// would increase memory pressure during large traversals.
type commitQueueItem struct {
	oid Hash
	ts  int64
}

// commitQueue implements heap.Interface as a min-heap of commitQueueItems.
// The ordering is by ascending timestamp first, with lexicographic OID
// comparison as a tie-breaker to guarantee deterministic output regardless
// of map iteration order.
type commitQueue []commitQueueItem

func (q commitQueue) Len() int { return len(q) }

// Less defines the min-heap ordering: earlier timestamps surface first.
// When two commits share the same timestamp (common in squash-merges and
// fast-imports), the raw OID bytes are compared lexicographically so that
// the output order is fully deterministic and reproducible across runs.
func (q commitQueue) Less(i, j int) bool {
	if q[i].ts != q[j].ts {
		return q[i].ts < q[j].ts
	}
	return bytes.Compare(q[i].oid[:], q[j].oid[:]) < 0
}

func (q commitQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *commitQueue) Push(x any) {
	*q = append(*q, x.(commitQueueItem))
}

func (q *commitQueue) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[:n-1]
	return item
}

// orderCommitsParentFirst returns a deterministic parent-before-child ordering
// of the input commits using Kahn's algorithm with a min-heap priority queue.
//
// Algorithm:
//  1. Index every commit by OID and compute its in-degree (number of parents
//     that are also in the input set).
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

	byOID := make(map[Hash]commitInfo, len(commits))
	children := make(map[Hash][]Hash, len(commits))
	inDegree := make(map[Hash]int, len(commits))

	for _, c := range commits {
		byOID[c.OID] = c
		inDegree[c.OID] = 0
	}

	for _, c := range commits {
		for _, p := range c.ParentOIDs {
			if _, ok := byOID[p]; !ok {
				continue
			}
			inDegree[c.OID]++
			children[p] = append(children[p], c.OID)
		}
	}

	q := make(commitQueue, 0, len(commits))
	for _, c := range commits {
		if inDegree[c.OID] == 0 {
			q = append(q, commitQueueItem{oid: c.OID, ts: c.Timestamp})
		}
	}
	heap.Init(&q)

	out := make([]commitInfo, 0, len(commits))
	for q.Len() > 0 {
		item := heap.Pop(&q).(commitQueueItem)
		c := byOID[item.oid]
		out = append(out, c)

		for _, child := range children[item.oid] {
			inDegree[child]--
			if inDegree[child] == 0 {
				cc := byOID[child]
				heap.Push(&q, commitQueueItem{oid: child, ts: cc.Timestamp})
			}
		}
	}

	// Defensive fallback for malformed input with cycles.
	if len(out) < len(commits) {
		seen := make(map[Hash]struct{}, len(out))
		for _, c := range out {
			seen[c.OID] = struct{}{}
		}

		rest := make([]commitInfo, 0, len(commits)-len(out))
		for _, c := range commits {
			if _, ok := seen[c.OID]; ok {
				continue
			}
			rest = append(rest, c)
		}

		slices.SortFunc(rest, func(a, b commitInfo) int {
			if a.Timestamp < b.Timestamp {
				return -1
			}
			if a.Timestamp > b.Timestamp {
				return 1
			}
			return bytes.Compare(a.OID[:], b.OID[:])
		})
		out = append(out, rest...)
	}

	return out
}
