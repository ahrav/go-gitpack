// history_scanner.go
//
// High-performance Git commit history scanner optimized for tree-to-tree
// diffing workflows. The scanner extracts minimal commit metadata (OID, tree,
// parents, timestamp) without inflating full commit objects, enabling fast
// repository traversal and change detection.
//
// USAGE:
//
// The HistoryScanner provides the main entry point for accessing Git repository
// data. It wraps an internal object store and exposes a clean public API:
//
//	// Open a Git repository
//	scanner, err := objstore.NewHistoryScanner(".git")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer scanner.Close()
//
//	// Configure scanner options
//	scanner.SetMaxDeltaDepth(100)      // Adjust delta chain limits
//	scanner.SetVerifyCRC(true)         // Enable integrity checking
//
//	// Access individual Git objects
//	data, objType, err := scanner.Get(oid)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Iterate over tree contents
//	iter, err := scanner.TreeIter(treeOID)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for {
//	    name, childOID, mode, ok, err := iter.Next()
//	    if err != nil || !ok {
//	        break
//	    }
//	    fmt.Printf("%s: %x (mode: %o)\n", name, childOID, mode)
//	}
//
//	// Load all commits from commit-graph
//	commits, err := scanner.LoadAllCommits()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Stream commit diffs concurrently as hunks
//	hunks, errors := scanner.DiffHistoryHunks()
//	go func() {
//	    for hunk := range hunks {
//	        fmt.Printf("+ %s: %d-%d (%d lines)\n", hunk.Path(), hunk.StartLine(), hunk.EndLine(), len(hunk.Lines()))
//	    }
//	}()
//	if err := <-errors; err != nil {
//	    log.Fatal(err)
//	}
//
// IMPLEMENTATION:
// The scanner currently requires commit-graph files for O(commits) scanning
// with commits processed concurrently for maximum throughput. A fallback mechanism to enumerate
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
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
)

// commitInfo holds the subset of Git commit metadata that the history
// scanner needs for tree-to-tree diffing and topological walks.
// It deliberately omits heavyweight details such as author/committer
// identities and the commit message to keep allocations low.
// A slice of commitInfo values is produced by HistoryScanner.LoadAllCommits
// and can be processed concurrently.
type commitInfo struct {
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
// packfile iteration to expose streaming APIs such as DiffHistoryHunks that
// deliver commits concurrently with minimal memory footprint.
// Instantiate a HistoryScanner when you need to traverse many commits or
// compute incremental diffs without materializing full objects.
type HistoryScanner struct {
	// store backs object retrieval and remains read-only for the lifetime
	// of the scanner.
	store *store

	// graphData is the parsed commit-graph for the repository.
	// It is nil when the repository lacks a commit-graph file, in which
	// case the scanner falls back to packfile enumeration.
	graphData *commitGraphData

	// meta provides cached access to commit author/committer metadata
	meta *metaCache
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

var ErrCommitGraphRequired = errors.New("commit-graph required but not found")

// NewHistoryScanner opens the Git repository at gitDir and returns a
// HistoryScanner that streams commit data concurrently.
//
// The implementation currently relies on commit-graph files being present
// inside .git/objects.
// If the commit-graph cannot be loaded, the function returns a non-nil error
// and the underlying object store is closed immediately.
//
// The caller is responsible for invoking (*HistoryScanner).Close when finished
// to release any mmap handles held by the store.
func NewHistoryScanner(gitDir string) (*HistoryScanner, error) {
	packDir := filepath.Join(gitDir, "objects", "pack")
	store, err := open(packDir)
	if err != nil {
		return nil, fmt.Errorf("open object store: %w", err)
	}

	// Require commit‑graph (TODO: add fallback to packfile parsing for repositories without commit-graph)
	graphDir := filepath.Join(gitDir, "objects")
	graph, err := loadCommitGraph(graphDir)
	if err != nil || graph == nil {
		store.Close() // Clean up store if we can't load commit graph
		if err != nil {
			return nil, fmt.Errorf("commit-graph required but failed to load: %w", err)
		}
		return nil, ErrCommitGraphRequired
	}

	mc := newMetaCache(graph, store)

	return &HistoryScanner{
		store:     store,
		graphData: graph,
		meta:      mc,
	}, nil
}

// HunkAddition holds metadata for a contiguous block of added lines that a commit introduced.
//
// Values of HunkAddition are streamed by HistoryScanner.DiffHistoryHunks concurrently,
// enabling callers to process repository changes in larger chunks for better context
// and performance when dealing with large additions.
type HunkAddition struct {
	// commit identifies the commit that introduced the added hunk.
	commit Hash

	// path specifies the file to which the hunk was added.
	// It always uses forward-slash (Unix style) separators, independent
	// of the host operating system.
	path string

	// startLine is the 1-based line number where this hunk begins
	// in the new version of the file.
	startLine int

	// endLine is the 1-based line number where this hunk ends
	// in the new version of the file.
	endLine int

	// lines contains all the added lines in this hunk without the leading
	// "+" diff markers.
	lines [][]byte
}

// String returns a human-readable representation of the hunk addition.
func (h *HunkAddition) String() string {
	return fmt.Sprintf("%s: %s:%d-%d (%d lines)", h.commit, h.path, h.startLine, h.endLine, len(h.lines))
}

// Lines returns all the added lines in this hunk without the leading
// "+" diff markers.
func (h *HunkAddition) Lines() [][]byte { return h.lines }

// StartLine returns the 1-based line number where this hunk begins
// in the new version of the file.
func (h *HunkAddition) StartLine() int { return h.startLine }

// EndLine returns the 1-based line number where this hunk ends
// in the new version of the file.
func (h *HunkAddition) EndLine() int { return h.endLine }

// Commit returns the commit that introduced the added hunk.
func (h *HunkAddition) Commit() Hash { return h.commit }

// Path returns the file to which the hunk was added.
// It always uses forward-slash (Unix style) separators, independent
// of the host operating system.
func (h *HunkAddition) Path() string { return h.path }

// DiffHistoryHunks streams every added hunk from all commits.
//
// The caller receives a buffered channel of HunkAddition values and a separate
// channel for a single error value.
// The method launches a goroutine that performs the commit walk and diff computation
// concurrently, grouping consecutive line additions into hunks for better context
// and performance when processing large additions.
//
// The function never blocks the caller: both result channels are buffered.
// Results are produced concurrently, so consumers should drain the output
// promptly or close it to cancel.
//
// A nil error sent on errC signals a graceful end-of-stream.
func (hs *HistoryScanner) DiffHistoryHunks() (<-chan HunkAddition, <-chan error) {
	numWorkers := runtime.NumCPU()

	out := make(chan HunkAddition, numWorkers)
	errC := make(chan error, 1)

	go func() {
		defer close(out)

		if hs.graphData == nil {
			errC <- ErrCommitGraphRequired
			return
		}

		// Work item for each commit.
		type workItem struct {
			oid        Hash
			treeOID    Hash
			parentOIDs []Hash
		}

		workChan := make(chan workItem, numWorkers)
		errorChan := make(chan error, numWorkers)

		var wg sync.WaitGroup

		for range numWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for work := range workChan {
					commit := commitInfo{
						OID:        work.oid,
						TreeOID:    work.treeOID,
						ParentOIDs: work.parentOIDs,
					}
					if err := hs.processCommitStreamingHunks(hs.store, commit, out); err != nil {
						errorChan <- err
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(errorChan)
		}()

		// Stream commits directly from graph data to workers.
		go func() {
			defer close(workChan)
			for i, oid := range hs.graphData.OrderedOIDs {
				workChan <- workItem{
					oid:        oid,
					treeOID:    hs.graphData.TreeOIDs[i],
					parentOIDs: hs.graphData.Parents[oid],
				}
			}
		}()

		var firstErr error
		for err := range errorChan {
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}

		errC <- firstErr
	}()

	return out, errC
}

// processCommitStreamingHunks handles diff computation for a single commit and streams
// hunk additions directly to the output channel.
func (hs *HistoryScanner) processCommitStreamingHunks(tc *store, c commitInfo, out chan<- HunkAddition) error {
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

	return walkDiff(tc, pTree, c.TreeOID, "", func(path string, old, newH Hash, mode uint32) error {
		if mode&040000 != 0 {
			return nil
		}
		// `hs.store.Get(Hash{})` is safe: it returns a nil slice for
		// the empty object, which `addedHunksWithPos` handles correctly.
		oldBytes, _, _ := hs.store.Get(old)
		newBytes, _, _ := hs.store.Get(newH)

		for _, hunk := range addedHunksWithPos(oldBytes, newBytes) {
			out <- HunkAddition{
				commit:    c.OID,
				path:      filepath.ToSlash(path),
				startLine: hunk.StartLine,
				endLine:   hunk.EndLine(),
				lines:     hunk.Lines,
			}
		}
		return nil
	})
}

// get returns the fully materialized Git object identified by oid together
// with its on-disk ObjectType.
//
// This method delegates to the underlying store and provides the same
// functionality as the store's get method. See store.get for detailed
// documentation on caching, delta resolution, and CRC verification.
func (hs *HistoryScanner) get(oid Hash) ([]byte, ObjectType, error) {
	return hs.store.Get(oid)
}

// SetMaxDeltaDepth sets the maximum delta chain depth for object retrieval.
//
// This method delegates to the underlying store. See store.SetMaxDeltaDepth
// for detailed documentation on the implications of different depth values.
func (hs *HistoryScanner) SetMaxDeltaDepth(depth int) { hs.store.SetMaxDeltaDepth(depth) }

// SetVerifyCRC enables or disables CRC-32 validation for all object reads.
//
// When enabled, the scanner validates CRC-32 checksums for each object read
// from packfiles. This provides additional integrity checking at the cost of
// some performance. See the store documentation for details on CRC verification
// behavior with different pack types.
func (hs *HistoryScanner) SetVerifyCRC(verify bool) { hs.store.VerifyCRC = verify }

// Close releases any mmap handles or file descriptors held by the underlying
// object store.
// It is safe to call Close multiple times; subsequent calls are no-ops.
func (hs *HistoryScanner) Close() error { return hs.store.Close() }

// CommitMetadata bundles the author identity and commit timestamp for a
// single Git commit.
//
// Callers obtain a CommitMetadata instance through HistoryScanner.
// GetCommitMetadata when they need lightweight identity data without
// performing a full object walk.  On the first request for a given commit
// the scanner inflates only the commit header, parses the “author”
// (or fallback “committer”) line, and caches the result.  Later requests
// return the cached value with zero additional I/O.
//
// The type is immutable and therefore safe for concurrent reads.
type CommitMetadata struct {
	// Author describes the commit author exactly as recorded in the
	// commit header.  The field is never modified after creation and can
	// be shared freely between goroutines.
	Author AuthorInfo

	// Timestamp holds the committer time in seconds since the Unix epoch.
	// It originates from the commit-graph if available; otherwise it is
	// parsed from the commit header on the first cache miss.
	Timestamp int64
}

// GetCommitMetadata returns the commit's author and committer time (Unix seconds).
// This method lazily inflates the commit object and caches the result.
func (s *HistoryScanner) GetCommitMetadata(oid Hash) (CommitMetadata, error) {
	return s.meta.get(oid)
}

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
func (hs *HistoryScanner) LoadAllCommits() ([]commitInfo, error) {
	return hs.loadFromGraph(), nil
}

// graph → []*CommitInfo  (O(#commits))
func (hs *HistoryScanner) loadFromGraph() []commitInfo {
	n := len(hs.graphData.OrderedOIDs)
	out := make([]commitInfo, n)

	for i, oid := range hs.graphData.OrderedOIDs {
		out[i] = commitInfo{
			OID:        oid,
			TreeOID:    hs.graphData.TreeOIDs[i],
			ParentOIDs: hs.graphData.Parents[oid],
		}
	}
	return out
}

// No commit‑graph: iterate every pack, inflate only commit objects.
// TODO: This method is preserved for future fallback support when commit-graph is not available.
// Currently not used since we require commit-graph files.
func (hs *HistoryScanner) scanPackfiles() ([]commitInfo, error) {
	if len(hs.store.packs) == 0 {
		return []commitInfo{}, nil // Return empty slice, not error
	}

	var (
		mu            sync.Mutex
		commits       []commitInfo
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
// It expects raw to contain the canonical "commit" object encoding and does
// not allocate more than necessary: object IDs are sliced directly out of the
// input buffer where possible.
//
// The function validates the format of "tree" and "parent" header lines and
// returns an error if they are malformed. The committer timestamp is always
// present.
func parseCommitData(oid Hash, raw []byte) (commitInfo, error) {
	ci := commitInfo{OID: oid}

	if !bytes.HasPrefix(raw, []byte("tree ")) {
		return commitInfo{}, fmt.Errorf("commit %x: missing tree line", oid)
	}
	treeEnd := bytes.IndexByte(raw[5:], '\n')
	if treeEnd != 40 {
		return commitInfo{}, fmt.Errorf("commit %x: malformed tree line", oid)
	}
	tree, err := ParseHash(string(raw[5 : 5+treeEnd]))
	if err != nil {
		return commitInfo{}, err
	}
	ci.TreeOID = tree
	raw = raw[5+treeEnd+1:]

	for bytes.HasPrefix(raw, []byte("parent ")) {
		end := bytes.IndexByte(raw[7:], '\n')
		if end != 40 {
			return commitInfo{}, fmt.Errorf("commit %x: malformed parent line", oid)
		}
		p, err := ParseHash(string(raw[7 : 7+end]))
		if err != nil {
			return commitInfo{}, err
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
