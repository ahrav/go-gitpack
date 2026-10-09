package objstore

import (
	"bytes"
	"container/heap"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// referenceOrderItem and referenceOrderQueue implement Kahn's algorithm over
// Hash-keyed maps with container/heap; referenceOrderParentFirst defines the
// (timestamp, OID) parent-first order orderCommitsParentFirst must produce.
type referenceOrderItem struct {
	oid Hash
	ts  int64
}

type referenceOrderQueue []referenceOrderItem

func (q referenceOrderQueue) Len() int { return len(q) }
func (q referenceOrderQueue) Less(i, j int) bool {
	if q[i].ts != q[j].ts {
		return q[i].ts < q[j].ts
	}
	return bytes.Compare(q[i].oid[:], q[j].oid[:]) < 0
}
func (q referenceOrderQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *referenceOrderQueue) Push(x any)   { *q = append(*q, x.(referenceOrderItem)) }
func (q *referenceOrderQueue) Pop() any {
	old := *q
	item := old[len(old)-1]
	*q = old[:len(old)-1]
	return item
}

func referenceOrderParentFirst(commits []commitInfo) []commitInfo {
	byOID := make(map[Hash]commitInfo, len(commits))
	children := make(map[Hash][]Hash, len(commits))
	inDegree := make(map[Hash]int, len(commits))
	for _, c := range commits {
		byOID[c.OID] = c
		inDegree[c.OID] = 0
	}
	for _, c := range commits {
		for _, p := range c.ParentOIDs {
			if _, ok := byOID[p]; ok {
				inDegree[c.OID]++
				children[p] = append(children[p], c.OID)
			}
		}
	}
	var q referenceOrderQueue
	for _, c := range commits {
		if inDegree[c.OID] == 0 {
			q = append(q, referenceOrderItem{c.OID, c.Timestamp})
		}
	}
	heap.Init(&q)
	out := make([]commitInfo, 0, len(commits))
	for q.Len() > 0 {
		item := heap.Pop(&q).(referenceOrderItem)
		out = append(out, byOID[item.oid])
		for _, child := range children[item.oid] {
			inDegree[child]--
			if inDegree[child] == 0 {
				heap.Push(&q, referenceOrderItem{child, byOID[child].Timestamp})
			}
		}
	}
	if len(out) < len(commits) {
		seen := make(map[Hash]bool, len(out))
		for _, c := range out {
			seen[c.OID] = true
		}
		var rest []commitInfo
		for _, c := range commits {
			if !seen[c.OID] {
				rest = append(rest, c)
			}
		}
		slices.SortFunc(rest, func(a, b commitInfo) int {
			if a.Timestamp != b.Timestamp {
				if a.Timestamp < b.Timestamp {
					return -1
				}
				return 1
			}
			return bytes.Compare(a.OID[:], b.OID[:])
		})
		out = append(out, rest...)
	}
	return out
}

// randomHistory builds a DAG of n commits with timestamps that collide often
// and the occasional merge, shuffled so input order carries no information.
func randomHistory(rng *rand.Rand, n int) []commitInfo {
	commits := make([]commitInfo, n)
	for i := range commits {
		var oid Hash
		rng.Read(oid[:])
		commits[i] = commitInfo{OID: oid, TreeOID: Hash{byte(i)}, Timestamp: int64(rng.Intn(n/4 + 1))}
		if i > 0 {
			parents := 1
			if rng.Intn(5) == 0 {
				parents = 2
			}
			for k := 0; k < parents; k++ {
				commits[i].ParentOIDs = append(commits[i].ParentOIDs, commits[rng.Intn(i)].OID)
			}
		}
	}
	rng.Shuffle(len(commits), func(i, j int) { commits[i], commits[j] = commits[j], commits[i] })
	return commits
}

func TestOrderCommitsParentFirst_MatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for iter := 0; iter < 200; iter++ {
		commits := randomHistory(rng, rng.Intn(400)+2)
		if iter%7 == 0 {
			// A cycle with an external parent exercises the fallback.
			commits = append(commits,
				commitInfo{OID: Hash{0xaa}, ParentOIDs: []Hash{{0xbb}, {0xcc}}, Timestamp: 5},
				commitInfo{OID: Hash{0xbb}, ParentOIDs: []Hash{{0xaa}}, Timestamp: 4})
		}
		got := orderCommitsParentFirst(commits)
		want := referenceOrderParentFirst(commits)
		require.Equal(t, len(want), len(got))
		for i := range want {
			require.Equalf(t, want[i].OID, got[i].OID, "iteration %d position %d", iter, i)
		}
	}
}

func TestCommitIndexLookup(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	commits := randomHistory(rng, 3000)
	// Hashes sharing a leading word probe past collisions.
	commits[1].OID = commits[0].OID
	commits[1].OID[19] ^= 0xff
	ix := newCommitIndex(commits)
	for i, c := range commits {
		pos, ok := ix.lookup(c.OID)
		require.True(t, ok)
		assert.Equal(t, int32(i), pos)
	}
	var missing Hash
	rng.Read(missing[:])
	_, ok := ix.lookup(missing)
	assert.False(t, ok)
}

func TestCompactParentOIDsPreservesParents(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	commits := randomHistory(rng, 500)
	want := make([][]Hash, len(commits))
	for i, c := range commits {
		want[i] = append([]Hash(nil), c.ParentOIDs...)
	}
	compactParentOIDs(commits)
	for i, c := range commits {
		assert.Equal(t, want[i], c.ParentOIDs)
		assert.Equal(t, len(c.ParentOIDs), cap(c.ParentOIDs), "compacted lists have no spare capacity")
	}
}

func TestWalkCommitsFromRefs_VisitsEveryCommitOnce(t *testing.T) {
	for _, repo := range []string{"simple-linear", "with-merges", "large-repo", "very-large-repo-1k"} {
		s := createScannerForRepo(t, repo)
		seen := make(map[Hash]int)
		require.NoError(t, s.walkCommitsFromRefs(func(c commitInfo) error {
			seen[c.OID]++
			return nil
		}))
		for oid, n := range seen {
			assert.Equalf(t, 1, n, "%s: commit %s visited %d times", repo, oid, n)
		}
		commits, err := s.loadAllCommits()
		require.NoError(t, err)
		assert.Len(t, seen, len(commits), repo)
		s.Close()
	}
}

func TestWalkCommitsFromRefs_PropagatesVisitError(t *testing.T) {
	s := createScannerForRepo(t, "very-large-repo-1k")
	defer s.Close()
	want := assert.AnError
	err := s.walkCommitsFromRefs(func(c commitInfo) error { return want })
	assert.ErrorIs(t, err, want)
}

func TestSortOffsetsMatchesSlicesSort(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for _, n := range []int{0, 1, 2, 1023, 1024, 1025, 50000} {
		for _, maxBits := range []uint{8, 20, 33, 63} {
			offs := make([]uint64, n)
			for i := range offs {
				offs[i] = rng.Uint64() >> (64 - maxBits)
			}
			want := slices.Clone(offs)
			slices.Sort(want)
			sortOffsets(offs)
			require.Equalf(t, want, offs, "n=%d bits=%d", n, maxBits)
		}
	}
	// Duplicates and already-sorted input.
	offs := make([]uint64, 4096)
	for i := range offs {
		offs[i] = uint64(i / 3)
	}
	want := slices.Clone(offs)
	sortOffsets(offs)
	assert.Equal(t, want, offs)
}

func TestSearchHashesMatchesLinearSearch(t *testing.T) {
	rng := rand.New(rand.NewSource(13))
	hashes := make([]Hash, 2000)
	for i := range hashes {
		rng.Read(hashes[i][:])
		if i > 0 && i%10 == 0 {
			// Share the leading word with a neighbour to force the tail compare.
			copy(hashes[i][:8], hashes[i-1][:8])
		}
	}
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
	for i, h := range hashes {
		pos, ok := searchHashes(hashes, h)
		require.True(t, ok)
		assert.Equal(t, h, hashes[pos])
		if i > 0 && hashes[i-1] != h {
			assert.Equal(t, i, pos)
		}
	}
	for iter := 0; iter < 1000; iter++ {
		var probe Hash
		rng.Read(probe[:])
		pos, ok := searchHashes(hashes, probe)
		want, found := slices.BinarySearchFunc(hashes, probe, func(a, b Hash) int { return bytes.Compare(a[:], b[:]) })
		assert.Equal(t, found, ok)
		assert.Equal(t, want, pos)
	}
	_, ok := searchHashes(nil, Hash{1})
	assert.False(t, ok)
}
