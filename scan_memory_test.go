package objstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Parents inside the loaded commit set resolve through the synthesized
// commit graph; the treeOIDs memo only ever holds parents resolved from
// their headers, so a scan over a complete history leaves it empty.
func TestFirstParentTree_ResolvesLoadedCommitsThroughGraph(t *testing.T) {
	for _, repo := range []string{"with-merges", "large-repo"} {
		s := createScannerForRepo(t, repo)
		hunks, errC := s.DiffHistoryHunks()
		for range hunks {
		}
		require.NoError(t, <-errC)
		memoized := 0
		s.treeOIDs.Range(func(_, _ any) bool {
			memoized++
			return true
		})
		assert.Zerof(t, memoized, "%s: parents outside the loaded set", repo)

		commits, err := s.loadAllCommits()
		require.NoError(t, err)
		for _, c := range commits {
			if len(c.ParentOIDs) == 0 {
				continue
			}
			tree, err := s.firstParentTree(c)
			require.NoError(t, err)
			idx, ok := s.graphData.OIDToIndex[c.ParentOIDs[0]]
			require.True(t, ok)
			assert.Equal(t, s.graphData.TreeOIDs[idx], tree)
		}
		s.Close()
	}
}

// get serves pack objects through the offset cache and leaves the delta
// window to the loose-object and getMaterialized paths.
func TestGet_PackObjectsBypassDeltaWindow(t *testing.T) {
	s := createScannerForRepo(t, "simple-linear")
	defer s.Close()
	commits, err := s.loadAllCommits()
	require.NoError(t, err)
	require.NotEmpty(t, commits)

	tree := commits[0].TreeOID
	data, typ, err := s.store.get(tree)
	require.NoError(t, err)
	require.Equal(t, ObjTree, typ)
	require.NotEmpty(t, data)

	_, inWindow := s.store.dw.acquire(tree)
	assert.False(t, inWindow, "pack object read through get is served by the offset cache")
	p, off, ok := s.store.findPackedObject(tree)
	require.True(t, ok)
	cached, _, ok := s.store.offCache.get(p, off)
	require.True(t, ok)
	assert.Equal(t, data, cached)
}
