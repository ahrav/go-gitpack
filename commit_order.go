package objstore

import (
	"bytes"
	"container/heap"
	"slices"
)

type commitQueueItem struct {
	oid Hash
	ts  int64
}

type commitQueue []commitQueueItem

func (q commitQueue) Len() int { return len(q) }

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

// orderCommitsParentFirst returns a deterministic parent-before-child order.
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
