package objstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Parents inside the loaded commit set resolve through the synthesized
// commit graph without touching the treeOIDs memo, so the memo stays empty
// across a scan over a complete history.
func TestFirstParentTreeIn_ResolvesLoadedCommitsThroughGraph(t *testing.T) {
	for _, repo := range []string{"with-merges", "large-repo"} {
		s := createScannerForRepo(t, repo)
		commits, graph, err := s.loadCommitsAndGraph()
		require.NoError(t, err)
		require.NotNil(t, graph)
		for _, c := range commits {
			tree, err := s.firstParentTreeIn(graph, c)
			require.NoError(t, err)
			if len(c.ParentOIDs) == 0 {
				assert.Equal(t, Hash{}, tree)
				continue
			}
			idx, ok := graph.OIDToIndex[c.ParentOIDs[0]]
			require.True(t, ok)
			assert.Equal(t, graph.TreeOIDs[idx], tree)
		}
		memoized := 0
		s.treeOIDs.Range(func(_, _ any) bool {
			memoized++
			return true
		})
		assert.Zerof(t, memoized, "%s: parents outside the loaded set", repo)
		s.Close()
	}
}

// loadCommitsAndGraph and loadAllCommits agree on the commit list, and the
// list is in parent-first order.
func TestLoadCommitsAndGraph_MatchesLoadAllCommits(t *testing.T) {
	s := createScannerForRepo(t, "with-merges")
	defer s.Close()
	commits, graph, err := s.loadCommitsAndGraph()
	require.NoError(t, err)
	again, err := s.loadAllCommits()
	require.NoError(t, err)
	assert.Equal(t, again, commits)
	assert.Equal(t, len(commits), len(graph.OrderedOIDs))
	pos := make(map[Hash]int, len(commits))
	for i, c := range commits {
		pos[c.OID] = i
	}
	for i, c := range commits {
		for _, p := range c.ParentOIDs {
			if j, ok := pos[p]; ok {
				assert.Less(t, j, i, "parent precedes child")
			}
		}
	}
}

func TestWithHunkDedupRetainedBudget(t *testing.T) {
	s := createScannerForRepo(t, "with-merges")
	defer s.Close()
	assert.Equal(t, int64(dedupRetainedBytesCap), s.dedupLimits.retainedBytesCap)
	WithHunkDedupRetainedBudget(128 << 20)(s)
	assert.Equal(t, int64(128<<20), s.dedupLimits.retainedBytesCap)
	WithHunkDedupRetainedBudget(0)(s)
	assert.Equal(t, int64(1<<20), s.dedupLimits.retainedBytesCap, "values below 1 MiB are raised to 1 MiB")

	gitDir := repeatedPairRepo(t)
	want := collectHunkScan(t, gitDir, WithHunkLineDedup(true))
	got := collectHunkScan(t, gitDir, WithHunkLineDedup(true), WithHunkDedupRetainedBudget(1))
	assert.Equal(t, want, got, "a tight budget leaves the emission multiset unchanged")
}
