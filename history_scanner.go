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
// The struct is produced by internal commit-walk helpers and can be processed
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
	// gitDir is the repository .git directory used for ref-based fallbacks.
	gitDir string

	// scanMode controls which scanning strategy is used by Scan.
	// Blob mode is the default.
	scanMode ScanMode

	// store backs object retrieval for the lifetime of the scanner.
	store *store

	// graphData is the parsed commit‑graph. A nil value signals that the
	// repository lacks a commit‑graph and ref-walk fallback is used.
	graphData *commitGraphData

	// meta caches author/committer lines for cheap GetCommitMetadata calls.
	meta *metaCache

	// pairs memoizes computeAddedHunks results by (old,new) blob OID pair.
	// Merge commits and long-lived branches replay identical transitions,
	// so ~1/3 of pair diffs in a typical history walk are repeats.
	pairs *pairCache

	// treeOIDs memoizes commit OID -> root tree OID. Every commit header
	// is otherwise inflated twice per scan: once when the walk visits the
	// commit and once when its child resolves firstParentTree.
	treeOIDs sync.Map

	// profiling holds optional profiling configuration.
	// When non-nil, enables HTTP profiling server and/or trace.
	profiling *ProfilingConfig

	// skipMergeDiffs, when true, makes hunk scans yield no diffs for merge
	// commits, matching `git log -p` (which emits no patch text for merges
	// unless -m/--first-parent diffing is requested). Non-merge commits are
	// unaffected. This both aligns semantics with git-based scanners and
	// skips redundant work: merge diffs against the first parent mostly
	// replay hunks already seen on the merged branch.
	skipMergeDiffs bool

	// profileServer is the HTTP server for pprof endpoints.
	profileServer *http.Server

	// traceFile holds the file handle for execution trace output.
	traceFile *os.File

	// commitsOnce caches commit enumeration for repeated history walks.
	commitsOnce sync.Once
	commits     []commitInfo
	commitsErr  error
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

// ErrCommitGraphRequired is kept for backward compatibility.
// HistoryScanner now always builds commit metadata in memory from ref walks.
var ErrCommitGraphRequired = errors.New("commit‑graph required but not found")

// NewHistoryScanner opens gitDir and returns a HistoryScanner that streams
// commit data concurrently.
//
// The scanner always builds an in-memory commit graph from a ref walk and does
// not consume on-disk commit-graph files.
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

	mc := newMetaCache(nil, store)

	hs := &HistoryScanner{
		gitDir:    gitDir,
		scanMode:  ScanModeBlob,
		store:     store,
		graphData: nil,
		meta:      mc,
		pairs:     newPairCache(),
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

// DiffHistoryHunks streams every added hunk from all commits, diffing each
// commit against its first parent only (i.e. merge commits are treated as a
// single diff against the first parent, matching `git log --first-parent`
// semantics). This keeps output deterministic and avoids duplicate hunks from
// merge base reconstruction.
//
// It returns two buffered channels: one for HunkAddition values and one for a
// single error. The function never blocks the caller; all writes to the
// channels are non-blocking.
//
// Goroutine ownership: DiffHistoryHunks spawns a background goroutine that
// owns the returned channels and closes them when the walk completes. The
// caller MUST drain the HunkAddition channel to completion (or read until the
// errC channel delivers a value) to avoid leaking goroutines. Failing to
// drain will block the internal worker pool indefinitely.
//
// The HunkAddition channel is buffered to runtime.NumCPU() to allow workers
// to make progress without waiting for the consumer on every hunk. The errC
// channel is buffered to 1 so the producer goroutine can always send its
// final error without blocking.
//
// A nil error sent on errC signals a graceful end-of-stream.
func (hs *HistoryScanner) DiffHistoryHunks() (<-chan HunkAddition, <-chan error) {
	// A deep output buffer decouples producer bursts (a whale commit can
	// emit tens of thousands of hunks) from consumer scheduling; at ~100
	// bytes per HunkAddition header the buffer costs single-digit MiB and
	// removes the futex traffic that a small buffer caused.
	out := make(chan HunkAddition, 16384)
	errC := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errC)
		errC <- hs.DiffHistoryHunksFunc(func(h HunkAddition) error {
			out <- h
			return nil
		})
	}()

	return out, errC
}

// DiffHistoryHunksFunc streams every added hunk from all commits to fn,
// using the same first-parent semantics as DiffHistoryHunks.
//
// fn is invoked CONCURRENTLY from multiple internal workers (up to
// runtime.NumCPU simultaneous calls) and must be safe for concurrent use.
// Returning a non-nil error from fn aborts the scan; the first error is
// returned. Compared to draining the DiffHistoryHunks channel with one
// consumer goroutine, this eliminates the channel hand-off entirely and
// lets hunk processing scale across every worker — the preferred API for
// CPU-bound consumers.
func (hs *HistoryScanner) DiffHistoryHunksFunc(fn func(HunkAddition) error) error {
	numWorkers := runtime.NumCPU()

	defer hs.stopProfiling() // Ensure profiling is stopped even on error

	if err := hs.startProfiling(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to start profiling: %v\n", err)
	}

	{
		type workItem struct {
			commit commitInfo
		}

		// Two-stage pipeline. Real histories are heavily skewed: a handful
		// of "whale" commits (vendored trees, generated code, mass renames)
		// carry a large share of all file changes. With commit-granularity
		// work items one whale pins a single worker while the rest idle, so
		// the scan cannot saturate the machine. Stage 1 walks tree diffs
		// (cheap) and fans out per-file blob pairs; stage 2 computes hunks
		// (expensive: inflation + line diff) at blob-pair granularity, which
		// spreads a whale commit across every worker.
		// workChan is deep because the commit-walk's visit callback sends
		// here while holding the walk's internal visit mutex: if the send
		// blocks, every walk worker serializes behind it. A few thousand
		// commitInfo headers (~100 bytes each) buy full walk/tree-stage
		// decoupling for typical repositories.
		workChan := make(chan workItem, 8192)
		blobChan := make(chan blobPairWork, 4096)
		stopCh := make(chan struct{})
		var (
			stopOnce sync.Once
			treeWG   sync.WaitGroup
			blobWG   sync.WaitGroup
			firstErr error
		)
		setError := func(err error) {
			if err == nil {
				return
			}
			stopOnce.Do(func() {
				firstErr = err
				close(stopCh)
			})
		}

		for range numWorkers {
			treeWG.Add(1)
			go func() {
				defer treeWG.Done()
				for {
					select {
					case <-stopCh:
						return
					case work, ok := <-workChan:
						if !ok {
							return
						}
						// Resolve the first-parent tree here rather than in
						// the producer: the header inflation it requires is
						// the dominant cost of the walk and parallelizes
						// cleanly across the tree workers.
						parentTree, err := hs.firstParentTree(work.commit)
						if err != nil {
							c := work.commit
							setError(fmt.Errorf("resolve first-parent tree for commit %s: %w", c.OID, err))
							return
						}
						if err := hs.emitCommitBlobPairs(work.commit, parentTree, blobChan, stopCh); err != nil {
							c := work.commit
							setError(fmt.Errorf("failed processing commit %s (tree: %s): %w", c.OID, c.TreeOID, err))
							return
						}
					}
				}
			}()
		}

		for range numWorkers {
			blobWG.Add(1)
			go func() {
				defer blobWG.Done()
				for {
					select {
					case <-stopCh:
						return
					case work, ok := <-blobChan:
						if !ok {
							return
						}
						if err := hs.streamBlobPairHunks(work, fn); err != nil {
							setError(fmt.Errorf("failed diffing %s in commit %s: %w", work.path, work.commit, err))
							return
						}
					}
				}
			}()
		}

		walkErr := hs.walkCommitsFromRefs(func(c commitInfo) error {
			// Publish the tree OID before dispatch so children resolving
			// firstParentTree find it without re-inflating the header.
			hs.treeOIDs.Store(c.OID, c.TreeOID)
			if hs.skipMergeDiffs && len(c.ParentOIDs) > 1 {
				return nil
			}
			select {
			case <-stopCh:
				return errScanAborted
			case workChan <- workItem{commit: c}:
				return nil
			}
		})
		close(workChan)
		treeWG.Wait()
		close(blobChan)
		blobWG.Wait()

		if walkErr != nil && !errors.Is(walkErr, errScanAborted) {
			setError(walkErr)
		}

		return firstErr
	}
}

// errScanAborted marks an internal early-stop condition used to unwind commit walks.
var errScanAborted = errors.New("scan aborted")

// firstParentTree resolves the tree OID for a commit's first parent.
//
// When the commit has no parents (i.e. it is a root commit), the zero Hash{}
// is returned. The caller interprets the zero hash as the empty tree, so
// diffing against it produces additions for every file in the root commit's
// tree. This avoids special-casing root commits in the diff pipeline.
//
// When the first parent cannot be found (ErrObjectNotFound) or is not a
// commit object (ErrObjectNotCommit), the zero hash is returned silently.
// This gracefully handles shallow clones and truncated history where parent
// objects may be absent.
func (hs *HistoryScanner) firstParentTree(c commitInfo) (Hash, error) {
	if len(c.ParentOIDs) == 0 {
		return Hash{}, nil
	}

	parentOID := c.ParentOIDs[0]
	// The commit walk records every visited commit's tree OID; a hit here
	// avoids re-inflating the parent's header (which would otherwise
	// happen once per child).
	if t, ok := hs.treeOIDs.Load(parentOID); ok {
		return t.(Hash), nil
	}

	hdr, err := hs.store.readCommitHeader(parentOID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrObjectNotCommit) {
			return Hash{}, nil
		}
		return Hash{}, fmt.Errorf("read parent header %s: %w", parentOID, err)
	}

	parentInfo, err := parseCommitInfoFromHeader(parentOID, hdr)
	if err != nil {
		return Hash{}, err
	}
	hs.treeOIDs.Store(parentOID, parentInfo.TreeOID)
	return parentInfo.TreeOID, nil
}

// blobPairWork identifies one changed file within a commit whose added hunks
// still need to be computed. It is the unit of work for stage 2 of the
// DiffHistoryHunks pipeline.
type blobPairWork struct {
	commit Hash
	path   string
	oldOID Hash
	newOID Hash
}

// emitCommitBlobPairs walks the first-parent tree diff of a single commit and
// fans out one blobPairWork per changed blob. Tree walking is cheap relative
// to blob diffing, so this stage keeps the expensive stage-2 workers supplied
// with fine-grained work even when one commit touches thousands of files.
func (hs *HistoryScanner) emitCommitBlobPairs(c commitInfo, parentTree Hash, blobs chan<- blobPairWork, stopCh <-chan struct{}) error {
	return walkDiff(hs.store, parentTree, c.TreeOID, "", func(path string, old, newH Hash, mode uint32) error {
		if !isBlobMode(mode) {
			return nil
		}
		select {
		case <-stopCh:
			return errScanAborted
		case blobs <- blobPairWork{commit: c.OID, path: path, oldOID: old, newOID: newH}:
			return nil
		}
	})
}

// streamBlobPairHunks computes the added hunks for one changed file and
// delivers them to fn. Results are memoized by OID pair: the diff depends
// only on the two blob contents, and histories with merges replay the same
// transition repeatedly.
func (hs *HistoryScanner) streamBlobPairHunks(work blobPairWork, fn func(HunkAddition) error) error {
	pk := makePairKey(work.oldOID, work.newOID)
	hunks, cached := hs.pairs.get(pk)
	if !cached {
		var err error
		hunks, err = computeAddedHunks(hs.store, work.oldOID, work.newOID)
		if err != nil {
			return fmt.Errorf("compute added hunks: %w", err)
		}
		hs.pairs.add(pk, hunks)
	}

	for _, hunk := range hunks {
		if hunk.IsBinary { // Don't fuse binary hunks
			// Binary files are always sent as a single hunk.
			// Convention: for binary hunks, startLine == endLine to signal
			// that line-based range semantics do not apply. The value
			// comes from hunk.StartLine and is repeated for endLine to
			// communicate "this is a single indivisible blob" rather than
			// a contiguous line range.
			if err := fn(HunkAddition{
				commit:    work.commit,
				path:      filepath.ToSlash(work.path),
				startLine: int(hunk.StartLine),
				endLine:   int(hunk.StartLine),
				lines:     hunk.Lines,
				isBinary:  true,
			}); err != nil {
				return err
			}
			continue
		}

		// Emit each hunk on its own; fusing a single hunk is a no-op
		// (fuseHunks leaves slices shorter than 2 unchanged). Cross-hunk
		// fusion, if ever wanted, must run over the whole per-file hunk
		// slice instead.
		if err := fn(HunkAddition{
			commit:    work.commit,
			path:      filepath.ToSlash(work.path),
			startLine: int(hunk.StartLine),
			endLine:   int(hunk.EndLine()),
			lines:     hunk.Lines,
			isBinary:  false,
		}); err != nil {
			return err
		}
	}
	return nil
}

// get returns the fully materialized (i.e. delta-resolved, decompressed)
// object identified by oid plus its type. "Materialized" means that all
// delta chains have been walked and applied, producing the final byte content
// as if `git cat-file -p <oid>` were invoked.
func (hs *HistoryScanner) get(oid Hash) ([]byte, ObjectType, error) {
	return hs.store.get(oid)
}

// SetMaxDeltaDepth sets the maximum number of delta hops while materializing objects.
func (hs *HistoryScanner) SetMaxDeltaDepth(depth int) { hs.store.SetMaxDeltaDepth(depth) }

// SetMaxDeltaObjectSize bounds reconstructed delta targets in bytes.
// Passing zero disables the bound.
func (hs *HistoryScanner) SetMaxDeltaObjectSize(maxBytes uint64) {
	hs.store.SetMaxDeltaObjectSize(maxBytes)
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

// loadAllCommits is an internal helper used by package tests. Production scan
// paths should use streaming traversal.
//
// The method uses sync.Once to ensure the expensive commit enumeration is
// performed at most once, even under concurrent calls. After the first
// successful load, subsequent calls return a fresh copy of the cached slice.
//
// Copy semantics: the returned []commitInfo is a shallow copy of the internal
// cache. This prevents callers from mutating the scanner's cached state (e.g.
// reordering or truncating the slice). The commitInfo values themselves are
// safe to share because their mutable parts (the ParentOIDs slice) are never
// modified after construction.
func (hs *HistoryScanner) loadAllCommits() ([]commitInfo, error) {
	hs.commitsOnce.Do(func() {
		if hs.graphData != nil {
			hs.commits = hs.loadFromGraph()
			return
		}
		hs.commits, hs.commitsErr = hs.loadFromRefs()
		if hs.commitsErr != nil {
			return
		}
		hs.graphData = buildCommitGraphFromCommits(hs.commits)
		if hs.meta != nil {
			hs.meta.attachGraph(hs.graphData)
		}
	})

	if hs.commitsErr != nil {
		return nil, hs.commitsErr
	}

	out := make([]commitInfo, len(hs.commits))
	copy(out, hs.commits)
	return out, nil
}

// loadFromGraph converts commit‑graph rows into commitInfo values.
func (hs *HistoryScanner) loadFromGraph() []commitInfo {
	n := len(hs.graphData.OrderedOIDs)
	out := make([]commitInfo, n)

	for i, oid := range hs.graphData.OrderedOIDs {
		out[i] = commitInfo{
			OID:        oid,
			TreeOID:    hs.graphData.TreeOIDs[i],
			ParentOIDs: hs.graphData.parentsOf(i),
			Timestamp:  hs.graphData.Timestamps[i],
		}
	}
	return orderCommitsParentFirst(out)
}
