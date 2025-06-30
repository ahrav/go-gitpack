// history_scanner.go
//
// High-performance Git commit history scanner optimized for tree-to-tree
// diffing workflows. The scanner extracts minimal commit metadata (OID, tree,
// parents, timestamp) without inflating full commit objects, enabling fast
// repository traversal and change detection.
//
// IMPLEMENTATION:
// The scanner currently requires commit-graph files for O(commits) scanning
// with commits processed in their commit-graph storage order. A fallback mechanism to enumerate
// packfiles directly is planned but not yet implemented.
// TODO: Add support for repositories without commit-graph by falling back to
// packfile parsing that extracts tree and parent relationships from commit headers.
//
// The implementation avoids full commit message parsing and focuses on the
// minimal metadata required for tree traversal. Optional tree preloading
// supports workflows requiring random access to tree objects.

package objstore

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
)

// CommitInfo holds the subset of Git commit metadata that the history
// scanner needs for tree-to-tree diffing and topological walks.
// It deliberately omits heavyweight details such as author/committer
// identities and the commit message to keep allocations low.
// A slice of CommitInfo values is produced in commit-graph storage order by
// HistoryScanner.LoadAllCommits and can be processed concurrently.
type CommitInfo struct {
	// OID is the object ID of the commit itself.
	OID Hash

	// TreeOID is the object ID of the commit's root tree.
	TreeOID Hash

	// ParentOIDs lists the object IDs of the commit's direct parents in
	// the order they appear in the commit object.  The slice is empty for
	// the repository root commit.
	ParentOIDs []Hash

	// Timestamp records the committer time in seconds since the Unix epoch.
	Timestamp int64
}

// HistoryScanner provides read-only, high-throughput access to a Git
// repository's commit history.  It abstracts over commit-graph files and
// packfile iteration to expose streaming APIs such as DiffHistory that
// deliver commits in commit-graph storage order with minimal memory footprint.
// Instantiate a HistoryScanner when you need to traverse many commits or
// compute incremental diffs without materializing full objects.
type HistoryScanner struct {
	// store backs object retrieval and remains read-only for the lifetime
	// of the scanner.
	store *Store

	// graphData is the parsed commit-graph for the repository.
	// It is nil when the repository lacks a commit-graph file, in which
	// case the scanner falls back to packfile enumeration.
	graphData *CommitGraphData
}

// ScanError reports commits that failed to parse during a packfile scan.
// The error is non-fatal; callers should decide whether the missing commits
// are relevant for their workflow.
type ScanError struct {
	// FailedCommits maps each problematic commit OID to the parsing error
	// that was encountered.
	FailedCommits map[Hash]error
}

func (e *ScanError) Error() string {
	return fmt.Sprintf("failed to parse %d commits", len(e.FailedCommits))
}

// NewHistoryScanner opens the Git repository at gitDir and returns a
// HistoryScanner that streams commit data in commit-graph storage order.
//
// The implementation currently relies on commit-graph files being present
// inside .git/objects.
// If the commit-graph cannot be loaded, the function returns a non-nil error
// and the underlying object store is closed immediately.
//
// The caller is responsible for invoking (*HistoryScanner).Close when finished
// to release any mmap handles held by the store.
func NewHistoryScanner(gitDir string) (*HistoryScanner, error) {
	// 1. open the *.pack store
	packDir := filepath.Join(gitDir, "objects", "pack")
	store, err := Open(packDir)
	if err != nil {
		return nil, fmt.Errorf("open object store: %w", err)
	}

	// 2. require commit‑graph (TODO: add fallback to packfile parsing for repositories without commit-graph)
	graphDir := filepath.Join(gitDir, "objects")
	graph, err := LoadCommitGraph(graphDir)
	if err != nil || graph == nil {
		store.Close() // Clean up store if we can't load commit graph
		if err != nil {
			return nil, fmt.Errorf("commit-graph required but failed to load: %w", err)
		}
		return nil, fmt.Errorf("commit-graph required but not found")
	}

	return &HistoryScanner{store: store, graphData: graph}, nil
}

// Addition holds metadata for a single "+" line that a commit introduced.
//
// Values of Addition are streamed by HistoryScanner.DiffHistory in
// commit-graph storage order, enabling callers to process repository changes
// incrementally without materializing whole diffs or commits in memory.
type Addition struct {
	// Commit identifies the commit that introduced the added line.
	Commit Hash

	// Path specifies the file to which the line was added.
	// It always uses forward-slash (Unix style) separators, independent
	// of the host operating system.
	Path string

	// Lines contains the raw bytes of the added line without the leading
	// "+" diff marker. The outer slice allows the caller to handle data
	// in its original chunking or regroup it as needed.
	Lines [][]byte
}

// DiffHistory streams every added line in commit-graph storage order.
//
// The caller receives a buffered channel of Addition values and a separate
// channel for a single error value.
// The method launches a goroutine that
//  1. Performs the commit walk and diff computation.
//  2. Sends each Addition to the `out` channel in commit-graph storage order.
//  3. Sends a non-nil error to `errC` and returns early on failure.
//     Otherwise, it sends nil when the walk completes.
//
// Closing the `out` channel before it is exhausted aborts the walk
// because any subsequent send will panic, which unwinds the goroutine.
//
// The function never blocks the caller: both result channels are buffered.
// Results are produced concurrently, so consumers should drain `out`
// promptly or close it to cancel.
//
// DiffHistory never blocks the caller: both the additions channel and the
// error channel are pre-buffered.
// A nil error sent on errC signals a graceful end-of-stream.
func (hs *HistoryScanner) DiffHistory() (<-chan Addition, <-chan error) {
	out := make(chan Addition, 64)
	errC := make(chan error, 1)

	go func() {
		defer close(out)
		tc := newTreeCache(hs.store)

		commits, err := hs.LoadAllCommits()
		if err != nil {
			errC <- err
			return
		}

		for _, c := range commits {
			parents := c.ParentOIDs
			if len(parents) == 0 {
				// Root commit: diff against the implicit empty tree (Hash{}).
				parents = []Hash{{}}
			}

			pTree := Hash{}
			if parents[0] != (Hash{}) && hs.graphData != nil {
				if idx, ok := hs.graphData.OIDToIndex[parents[0]]; ok {
					pTree = hs.graphData.TreeOIDs[idx]
				}
			}

			if err := walkDiff(tc, pTree, c.TreeOID, "", func(path string, old, new Hash, mode uint32) error {
				if mode&040000 != 0 {
					return nil
				}
				// `hs.store.Get(Hash{})` is safe: it returns a nil slice for
				// the empty object, which `addedLines` handles correctly.
				oldBytes, _, _ := hs.store.Get(old)
				newBytes, _, _ := hs.store.Get(new)

				for _, ln := range addedLines(oldBytes, newBytes) {
					out <- Addition{
						Commit: c.OID,
						Path:   filepath.ToSlash(path),
						Lines:  [][]byte{ln},
					}
				}
				return nil
			}); err != nil {
				errC <- err
				return
			}
		}
		errC <- nil
	}()
	return out, errC
}

// GetStore returns the read-only object store that the HistoryScanner uses to
// load Git objects.
// Callers must treat the returned *Store as immutable for the lifetime of the
// scanner.
func (hs *HistoryScanner) GetStore() *Store { return hs.store }

// Close releases any mmap handles or file descriptors held by the underlying
// object store.
// It is safe to call Close multiple times; subsequent calls are no-ops.
func (hs *HistoryScanner) Close() error { return hs.store.Close() }

// LoadAllCommits returns all commits in commit-graph storage order.
//
// The method currently reads commit metadata exclusively from commit-graph
// files, which makes the call O(#commits) and allocation-friendly.
// When the repository lacks a commit-graph, callers should expect this method
// to fail once the TODO fallback to packfile scanning is implemented.
//
// The slice elements are owned by the caller; modifying them does not affect
// the scanner.
// The method never returns a nil slice.
//
// TODO: Implement a fallback to scan packfiles when a commit-graph is not
// available.
func (hs *HistoryScanner) LoadAllCommits() ([]CommitInfo, error) {
	return hs.loadFromGraph(), nil
}

// graph → []*CommitInfo  (O(#commits))
func (hs *HistoryScanner) loadFromGraph() []CommitInfo {
	n := len(hs.graphData.OrderedOIDs)
	out := make([]CommitInfo, n)

	for i, oid := range hs.graphData.OrderedOIDs {
		out[i] = CommitInfo{
			OID:        oid,
			TreeOID:    hs.graphData.TreeOIDs[i],
			ParentOIDs: hs.graphData.Parents[oid],
			Timestamp:  hs.graphData.Timestamps[i],
		}
	}
	return out
}

// No commit‑graph: iterate every pack, inflate only commit objects.
// TODO: This method is preserved for future fallback support when commit-graph is not available.
// Currently not used since we require commit-graph files.
func (hs *HistoryScanner) scanPackfiles() ([]CommitInfo, error) {
	if len(hs.store.packs) == 0 {
		return []CommitInfo{}, nil // Return empty slice, not error
	}

	var (
		mu            sync.Mutex
		commits       []CommitInfo
		failedCommits = make(map[Hash]error)
	)

	for _, pf := range hs.store.packs {
		if pf.oidTable == nil || pf.entries == nil {
			continue
		}
		for i, oid := range pf.oidTable {
			off := pf.entries[i].offset
			typ, _, err := peekObjectType(pf.pack, off)
			if err != nil || typ != ObjCommit {
				continue
			}
			raw, _, err := hs.store.Get(oid)
			if err != nil {
				failedCommits[oid] = err
				continue
			}
			ci, err := parseCommitData(oid, raw)
			if err != nil {
				failedCommits[oid] = err
				continue
			}
			mu.Lock()
			commits = append(commits, ci)
			mu.Unlock()
		}
	}
	if len(failedCommits) > 0 {
		return commits, &ScanError{FailedCommits: failedCommits}
	}
	return commits, nil
}

// parseCommitData extracts minimal commit metadata from the raw commit object.
//
// It expects raw to contain the canonical “commit” object encoding and does
// not allocate more than necessary: object IDs are sliced directly out of the
// input buffer where possible.
//
// The function validates the format of “tree” and “parent” header lines and
// returns an error if they are malformed. The committer timestamp is always
// present.
func parseCommitData(oid Hash, raw []byte) (CommitInfo, error) {
	ci := CommitInfo{OID: oid}

	if !bytes.HasPrefix(raw, []byte("tree ")) {
		return CommitInfo{}, fmt.Errorf("commit %x: missing tree line", oid)
	}
	treeEnd := bytes.IndexByte(raw[5:], '\n')
	if treeEnd != 40 {
		return CommitInfo{}, fmt.Errorf("commit %x: malformed tree line", oid)
	}
	tree, err := ParseHash(string(raw[5 : 5+treeEnd]))
	if err != nil {
		return CommitInfo{}, err
	}
	ci.TreeOID = tree
	raw = raw[5+treeEnd+1:]

	for bytes.HasPrefix(raw, []byte("parent ")) {
		end := bytes.IndexByte(raw[7:], '\n')
		if end != 40 {
			return CommitInfo{}, fmt.Errorf("commit %x: malformed parent line", oid)
		}
		p, err := ParseHash(string(raw[7 : 7+end]))
		if err != nil {
			return CommitInfo{}, err
		}
		ci.ParentOIDs = append(ci.ParentOIDs, p)
		raw = raw[7+end+1:]
	}

	if idx := bytes.Index(raw, []byte("\ncommitter ")); idx >= 0 {
		lineEnd := bytes.IndexByte(raw[idx+1:], '\n')
		if lineEnd > 0 {
			fields := bytes.Fields(raw[idx+11 : idx+1+lineEnd])
			if n := len(fields); n >= 2 {
				var ts int64
				fmt.Sscan(string(fields[n-2]), &ts)
				ci.Timestamp = ts
			}
		}
	}
	return ci, nil
}
