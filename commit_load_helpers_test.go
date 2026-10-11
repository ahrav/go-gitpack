package objstore

func (hs *HistoryScanner) loadAllCommits() ([]commitInfo, error) {
	commits, _, err := hs.loadCommitsAndGraph()
	return commits, err
}

func (hs *HistoryScanner) collectCommitPairs(c commitInfo, stopCh <-chan struct{}, admit func() error) ([]blobPairWork, error) {
	return hs.collectCommitPairsIn(nil, c, stopCh, admit)
}

func commitParentIndexes(commits []commitInfo) commitParents {
	byOID := make(map[Hash]int32, len(commits))
	for i := range commits {
		byOID[commits[i].OID] = int32(i)
	}
	start := make([]int32, len(commits)+1)
	for i := range commits {
		start[i+1] = start[i] + int32(len(commits[i].ParentOIDs))
	}
	idx := make([]int32, start[len(commits)])
	for i := range commits {
		dst := idx[start[i]:start[i+1]]
		for j, p := range commits[i].ParentOIDs {
			if pi, ok := byOID[p]; ok {
				dst[j] = pi
			} else {
				dst[j] = -1
			}
		}
	}
	return commitParents{start: start, idx: idx}
}

func orderCommitsParentFirst(commits []commitInfo) []commitInfo {
	out, _ := orderCommitsParentFirstIndexed(commits, commitParentIndexes(commits))
	return out
}

func buildCommitGraphFromCommits(commits []commitInfo) *commitGraphData {
	parents := commitParentIndexes(commits)
	perm := make([]int32, len(commits))
	for i := range perm {
		perm[i] = int32(i)
	}
	return buildCommitGraphIndexed(commits, parents, perm)
}
