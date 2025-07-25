// Package objstore offers a content‑addressable store optimized for Git
// packfiles and commit‑graph data.
//
// This file defines HistoryScanner, a high‑throughput helper that streams
// commit‑level information and tree‑to‑tree diffs without inflating full commit
// objects.
//
// # Overview
//
// A HistoryScanner wraps an internal object store plus (optionally) the
// repository's commit‑graph.  It exposes a composable API layer focused on
// **read‑only** analytics workloads such as:
//
//   - Scanning every commit once to extract change hunks.
//   - Iterating trees to build custom indexes.
//   - Fetching lightweight commit metadata (author, timestamp) on demand.
//
// Callers should construct exactly one HistoryScanner per repository and reuse
// it for the lifetime of the program.  All methods are safe for concurrent
// use unless their doc comment states otherwise.
//
// # Quick start
//
//	// Open an existing repository.
//	s, err := objstore.NewHistoryScanner(".git")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer s.Close()
//
//	s.SetMaxDeltaDepth(100) // Tune delta resolution
//	s.SetVerifyCRC(true)    // Extra integrity checking
//
//	// Stream added hunks from every commit.
//	hunks, errs := s.DiffHistoryHunks()
//	go func() {
//	    for h := range hunks {
//	        fmt.Println(h)
//	    }
//	}()
//	if err := <-errs; err != nil {
//	    log.Fatal(err)
//	}
package objstore

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// commitInfo holds the minimal subset of commit metadata needed for
// HistoryScanner operations.
//
// Only OID, tree, parents, and committer timestamp are retained, which keeps
// allocation pressure low during repository‑wide walks.
// The struct is produced by HistoryScanner.LoadAllCommits and can be processed
// concurrently.
//
// NOTE: commitInfo is unexported because it is an implementation detail; the
// public API returns the slice directly but does not force callers to spell
// the type name.
type commitInfo struct {
	// OID is the object ID of the commit itself.
	OID Hash

	// TreeOID identifies the root tree of the commit.
	TreeOID Hash

	// ParentOIDs lists the direct parents in the order stored in the commit
	// object. The slice is empty for a repository root commit.
	ParentOIDs []Hash

	// Timestamp records the committer time in seconds since the Unix epoch.
	Timestamp int64
}

// HistoryScanner provides read‑only, high‑throughput access to a Git
// repository's commit history.
//
// It abstracts over commit‑graph files and packfile iteration to expose
// streaming APIs such as DiffHistoryHunks that deliver results concurrently
// while holding only a small working set in memory.
//
// Instantiate a HistoryScanner when you need to traverse many commits or
// compute incremental diffs without materializing full commit objects.
// The zero value is invalid; use NewHistoryScanner.
type HistoryScanner struct {
	// store backs object retrieval for the lifetime of the scanner.
	store *store

	// graphData is the parsed commit‑graph. A nil value signals that the
	// repository lacks a commit‑graph; packfile fallbacks will eventually be
	// implemented to cover this case.
	graphData *commitGraphData

	// meta caches author/committer lines for cheap GetCommitMetadata calls.
	meta *metaCache

	// profiling holds optional profiling configuration.
	// When non-nil, enables HTTP profiling server and/or trace.
	profiling *ProfilingConfig

	// profileServer is the HTTP server for pprof endpoints.
	profileServer *http.Server

	// traceFile holds the file handle for execution trace output.
	traceFile *os.File
}

// ScanError reports commits that failed to parse during a packfile scan.
//
// The error is non‑fatal; callers decide whether the missing commits are
// relevant for their workflow.
type ScanError struct {
	// FailedCommits maps each problematic commit OID to the error encountered
	// while decoding it.
	FailedCommits map[Hash]error
}

// Error implements the error interface.
func (e *ScanError) Error() string {
	return fmt.Sprintf("failed to parse %d commits", len(e.FailedCommits))
}

// ErrCommitGraphRequired is returned by NewHistoryScanner and DiffHistoryHunks
// when the caller requested an operation that currently depends on
// commit‑graph files but the repository does not provide one.
var ErrCommitGraphRequired = errors.New("commit‑graph required but not found")

// NewHistoryScanner opens gitDir and returns a HistoryScanner that streams
// commit data concurrently.
//
// The current implementation **requires** a commit‑graph under
//
//	<gitDir>/objects/commit‑graph
//
// If the commit‑graph cannot be loaded the function returns a non‑nil error
// and any resources acquired by the underlying object store are released.
//
// Options can be provided to configure scanner behavior, such as enabling
// profiling with WithProfiling.
//
// The caller must invoke (*HistoryScanner).Close when finished to free mmap
// handles and file descriptors.
func NewHistoryScanner(gitDir string, opts ...ScannerOption) (*HistoryScanner, error) {
	packDir := filepath.Join(gitDir, "objects", "pack")
	store, err := open(packDir)
	if err != nil {
		return nil, fmt.Errorf("open object store: %w", err)
	}

	// Require commit‑graph (TODO: add fallback to packfile parsing).
	graphDir := filepath.Join(gitDir, "objects")
	graph, err := loadCommitGraph(graphDir)
	if err != nil || graph == nil {
		store.Close()
		if err != nil {
			return nil, fmt.Errorf("commit‑graph required but failed to load: %w", err)
		}
		return nil, ErrCommitGraphRequired
	}

	mc := newMetaCache(graph, store)

	hs := &HistoryScanner{
		store:     store,
		graphData: graph,
		meta:      mc,
	}

	for _, opt := range opts {
		opt(hs)
	}

	return hs, nil
}

// HunkAddition describes a contiguous block of added lines introduced by a
// commit.
//
// Values are streamed by HistoryScanner.DiffHistoryHunks and can be consumed
// concurrently by the caller.
type HunkAddition struct {
	// lines holds the added lines without leading '+' markers.
	lines []string

	// commit is the commit that introduced the hunk.
	commit Hash

	// path is the file path using forward‑slash separators, regardless of OS.
	path string

	// startLine is the 1‑based line number where the hunk begins in the new
	// version of the file.
	startLine int

	// endLine is the 1‑based line number where the hunk ends.
	endLine int

	// isBinary indicates whether this hunk contains binary data.
	isBinary bool
}

// String returns a human‑readable representation.
func (h *HunkAddition) String() string {
	return fmt.Sprintf("%s: %s:%d-%d (%d lines)", h.commit, h.path, h.startLine, h.endLine, len(h.lines))
}

// Lines returns all added lines without leading '+' markers.
func (h *HunkAddition) Lines() []string { return h.lines }

// StartLine returns the first line number (1‑based) of the hunk.
func (h *HunkAddition) StartLine() int { return h.startLine }

// EndLine returns the last line number (1‑based) of the hunk.
func (h *HunkAddition) EndLine() int { return h.endLine }

// Commit returns the commit that introduced the hunk.
func (h *HunkAddition) Commit() Hash { return h.commit }

// Path returns the file to which the hunk was added, using forward‑slash
// separators.
func (h *HunkAddition) Path() string { return h.path }

// IsBinary returns whether this hunk contains binary data.
func (h *HunkAddition) IsBinary() bool { return h.isBinary }

// DiffHistoryHunks streams every added hunk from all commits.
//
// It returns two buffered channels: one for HunkAddition values and one for a
// single error.
// The function never blocks the caller; all writes to the channels are
// non‑blocking.
//
// A nil error sent on errC signals a graceful end‑of‑stream.
func (hs *HistoryScanner) DiffHistoryHunks() (<-chan HunkAddition, <-chan error) {
	numWorkers := runtime.NumCPU()

	out := make(chan HunkAddition, numWorkers)
	errC := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errC)
		defer hs.stopProfiling() // Ensure profiling is stopped even on error

		if err := hs.startProfiling(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to start profiling: %v\n", err)
		}

		if hs.graphData == nil {
			errC <- ErrCommitGraphRequired
			return
		}

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
					c := commitInfo{
						OID:        work.oid,
						TreeOID:    work.treeOID,
						ParentOIDs: work.parentOIDs,
					}
					if err := hs.processCommitStreamingHunks(hs.store, c, out); err != nil {
						errorChan <- fmt.Errorf("failed processing commit %s (tree: %s): %w", c.OID, c.TreeOID, err)
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(errorChan)
		}()

		// Feed commits to the workers.
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

		if firstErr != nil {
			errC <- firstErr
		}
	}()

	return out, errC
}

// processCommitStreamingHunks diffs a single commit against its first parent
// (or the empty tree for a root commit) and streams added hunks to out.
func (hs *HistoryScanner) processCommitStreamingHunks(tc *store, c commitInfo, out chan<- HunkAddition) error {
	parents := c.ParentOIDs
	if len(parents) == 0 {
		parents = []Hash{{}} // Root commit: diff against empty tree.
	}

	pTree := Hash{}
	if !parents[0].IsZero() && hs.graphData != nil {
		if idx, ok := hs.graphData.OIDToIndex[parents[0]]; ok {
			pTree = hs.graphData.TreeOIDs[idx]
		}
	}

	return walkDiff(tc, pTree, c.TreeOID, "", func(path string, old, newH Hash, mode uint32) error {
		if mode&040000 != 0 { // Skip sub‑trees; we care only about blobs.
			return nil
		}

		hunks, err := computeAddedHunks(hs.store, old, newH)
		if err != nil {
			return fmt.Errorf("compute added hunks: %w", err)
		}

		for _, hunk := range hunks {
			if hunk.IsBinary { // Don't fuse binary hunks
				// Binary files are always sent as a single hunk.
				out <- HunkAddition{
					commit:    c.OID,
					path:      filepath.ToSlash(path),
					startLine: int(hunk.StartLine),
					endLine:   int(hunk.StartLine), // Binary files are one "line"
					lines:     hunk.Lines,
					isBinary:  true,
				}
				continue
			}

			fusedHunks := fuseHunks([]AddedHunk{hunk}, 3, 3)
			for _, fused := range fusedHunks {
				out <- HunkAddition{
					commit:    c.OID,
					path:      filepath.ToSlash(path),
					startLine: int(fused.StartLine),
					endLine:   int(fused.EndLine()),
					lines:     fused.Lines,
					isBinary:  false,
				}
			}
		}
		return nil
	})
}

// get returns the fully materialized object identified by oid plus its type.
func (hs *HistoryScanner) get(oid Hash) ([]byte, ObjectType, error) {
	return hs.store.get(oid)
}

// SetVerifyCRC enables or disables CRC‑32 verification on all object reads.
func (hs *HistoryScanner) SetVerifyCRC(verify bool) { hs.store.VerifyCRC = verify }

// Close releases any mmap handles or file descriptors held by the scanner.
// It is idempotent; subsequent calls are no‑ops.
func (hs *HistoryScanner) Close() error { return hs.store.Close() }

// CommitMetadata bundles the author identity and commit timestamp for a single
// commit.
//
// Instances are immutable and therefore safe for concurrent reads.
type CommitMetadata struct {
	// Author records the commit author exactly as stored in the commit header.
	Author AuthorInfo

	// Timestamp holds the committer time in seconds since the Unix epoch.
	Timestamp int64
}

// GetCommitMetadata returns (and caches) the commit's author and timestamp.
func (s *HistoryScanner) GetCommitMetadata(oid Hash) (CommitMetadata, error) {
	return s.meta.get(oid)
}

// LoadAllCommits returns all commits in commit‑graph topological order.
// The slice is never nil; it may be empty when the repository contains no
// commits.
func (hs *HistoryScanner) LoadAllCommits() ([]commitInfo, error) {
	return hs.loadFromGraph(), nil
}

// loadFromGraph converts commit‑graph rows into commitInfo values.
func (hs *HistoryScanner) loadFromGraph() []commitInfo {
	n := len(hs.graphData.OrderedOIDs)
	out := make([]commitInfo, n)

	for i, oid := range hs.graphData.OrderedOIDs {
		out[i] = commitInfo{
			OID:        oid,
			TreeOID:    hs.graphData.TreeOIDs[i],
			ParentOIDs: hs.graphData.Parents[oid],
			Timestamp:  hs.graphData.Timestamps[i], // if available
		}
	}
	return out
}
