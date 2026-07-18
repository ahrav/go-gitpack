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
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
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
	// commit and once when its child resolves firstParentTree. The memo is
	// scoped to one walk: each scan entry point clears it on return so a
	// long-lived scanner does not hold O(commit-count) memory.
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

// WithOffsetCacheBudget bounds the bytes of materialized pack objects the
// scanner's (pack, offset) cache may retain (default 256 MiB). Each scanner
// owns an independent cache, so processes that open many repositories
// concurrently should lower the budget to bound aggregate memory growth.
// A budget <= 0 disables the cache entirely.
func WithOffsetCacheBudget(bytes int) ScannerOption {
	return func(hs *HistoryScanner) {
		hs.store.offCache.setBudget(bytes)
	}
}

// WithPairCacheBudget bounds the bytes of computed diff hunks the scanner's
// (oldOID, newOID) memo may retain (default 128 MiB). Each scanner owns an
// independent cache, so processes that open many repositories concurrently
// should lower the budget to bound aggregate memory growth. A budget <= 0
// disables the memo entirely: every pair is recomputed.
//
// Disabling costs throughput on histories with merges — the memo exists
// because ~1/3 of pair diffs in a typical walk are repeats — but it does not
// change delivered hunks or their retention: a hunk's lines are compacted into
// their own buffer whether or not the memo stores them.
func WithPairCacheBudget(bytes int) ScannerOption {
	return func(hs *HistoryScanner) {
		hs.pairs.setBudget(bytes)
	}
}

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
//
// The returned slice and its strings are shared with internal caches and
// other deliveries of the same content; callers must not modify them.
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

// DiffHistoryHunks streams added hunks from all commits, diffing each
// commit against its first parent only (i.e. merge commits are treated as a
// single diff against the first parent, matching `git log --first-parent`
// semantics). This avoids duplicate hunks from merge base reconstruction.
//
// Exact-OID moves are suppressed: when a commit's entry produces bytes whose
// blob identity -- the OID plus the tree entry's type -- matches an unmatched
// deletion in that commit, its added lines are omitted because those
// content-addressed bytes are unchanged. This covers a pure addition and a move
// that overwrites a tracked destination alike. Matching is one-for-one, so each
// deletion suppresses at most one entry. No hunk is emitted for the destination
// path or moving commit.
//
// Renames are detected within each first-parent diff, mirroring Git. A
// delete+add pair carrying identical blob content (an exact-OID rename,
// preferring same-basename matches) emits no hunks, since no line changed.
// When at least two exact renames establish a directory rename, an added
// file under the new directory whose old-path counterpart was deleted is
// diffed against that old blob instead of being reported as a whole-file
// addition. That path-based pairing is content-validated: it is kept only
// when the lines common to both files are at least half of the larger file,
// matching Git's rename similarity threshold. All rename pairing is scoped
// to a single commit's first-parent diff; renames are never tracked across
// commits.
//
// It returns two buffered channels: one for HunkAddition values and one for a
// single error.
//
// Goroutine ownership: DiffHistoryHunks spawns a background goroutine that
// owns the returned channels and closes them when the walk completes. The
// caller MUST drain the HunkAddition channel to completion. Draining is what
// lets the walk finish: the forwarding send has no escape from a full queue,
// so errC delivers its single value only after every produced hunk has been
// forwarded. Waiting on errC without draining the hunk channel deadlocks as
// soon as the queue fills, and abandoning the drain blocks the internal worker
// pool indefinitely.
//
// The HunkAddition channel holds one slot per blob worker, so a worker can
// deposit its current hunk and start its next diff without a rendezvous with
// the consumer, and the queue buffers nothing beyond that. A buffered hunk
// retains its own line bytes and nothing beyond them (see pairCache.add), so
// the queue pins the payload in flight rather than the decompressed blobs the
// hunks were diffed from. The errC channel is buffered to 1 so the producer
// goroutine can always send its final error without blocking.
//
// Memory bound: the queue is bounded in hunk COUNT, not in bytes. Worst-case
// in-flight payload is approximately (queue depth + blobWorkers +
// consumer-held) times the per-hunk payload ceiling, and that ceiling is
// MaxDiffSize because a whole-file addition or a binary result carries the
// entire blob as its payload. The queue is not the dominant term: a queue slot
// holds one finished hunk, while each blob worker holds the hunks it just
// produced plus the decompressed blobs it diffed them from.
// DiffHistoryHunksFunc removes the queue term and only that term — the
// blob-worker term follows from the pipeline width — so it is the API for
// callers who want no queue between a worker and the consumer. Neither API
// lets a caller measure a hunk's retained payload: compactHunks gives all
// hunks of one pair a single shared backing array, so the lengths reported by
// Lines() do not sum to the bytes retained.
//
// Ordering: hunks for one (commit, path) pair are produced in ascending line
// order by a single blob worker, but that worker's sends interleave with every
// other worker's, so a pair's hunks are not contiguous in the stream. No order
// is guaranteed across files or commits. A consumer that groups by
// (commit, path) must key on the pair rather than flush on key change;
// DiffHistoryHunksFunc delivers a pair's hunks back to back and is the API for
// consumers that need that.
//
// Hunk lines may be shared with internal caches and other deliveries of the
// same content; callers must treat Lines() as read-only.
//
// A nil error sent on errC signals a graceful end-of-stream.
func (hs *HistoryScanner) DiffHistoryHunks() (<-chan HunkAddition, <-chan error) {
	// One slot per blob worker: DiffHistoryHunksFunc runs runtime.NumCPU blob
	// workers, and a slot each lets every worker deposit its current hunk and
	// resume its next diff without a rendezvous with the consumer. The queue
	// buffers nothing beyond that. What it can pin is the sum of the buffered
	// hunks' retained line bytes; see the memory-bound paragraph on the method
	// for the byte consequence and for the API that avoids the queue entirely.
	out := make(chan HunkAddition, runtime.NumCPU())
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

// DiffHistoryHunksFunc streams added hunks from all commits to fn, using the
// same first-parent semantics, one-for-one exact-OID move suppression, and
// rename detection as DiffHistoryHunks.
//
// fn is invoked CONCURRENTLY from multiple internal workers (up to
// runtime.NumCPU simultaneous calls) and must be safe for concurrent use.
// Returning a non-nil error from fn aborts the scan; the first error is
// returned. Compared to draining the DiffHistoryHunks channel with one
// consumer goroutine, this eliminates the channel hand-off entirely and
// lets hunk processing scale across every worker — the preferred API for
// CPU-bound consumers.
//
// Ordering: fn receives the hunks for one (commit, path) pair sequentially
// in ascending line order. No order is guaranteed across files or commits.
//
// Hunk lines may be shared with internal caches and other deliveries of the
// same content; fn must treat HunkAddition.Lines() as read-only.
//
// A nil fn is rejected before any worker starts. The workers call fn without
// a nil check on the hot path, so admitting one would surface as a panic in a
// worker goroutine — unrecoverable for the calling process — rather than as
// this method's error return.
func (hs *HistoryScanner) DiffHistoryHunksFunc(fn func(HunkAddition) error) error {
	if fn == nil {
		return errors.New("DiffHistoryHunksFunc: fn must not be nil")
	}

	// Stage widths. Stage 2 gets one worker per CPU because it carries the
	// expensive work (blob inflation plus line diff); stage 1 runs at half
	// that, capped at maxTreeDiffWorkers. Two independent constraints set the
	// stage-1 width and both point the same way. Tree diffing is cheap
	// relative to blob diffing, so stage 1 keeps stage 2 fed at a fraction of
	// its width. And every worker that sits inside a multi-hop delta
	// resolution holds a 32 MiB ping-pong arena: only a fraction of the
	// workers are in that state at any instant, which is why the arena
	// free-list can be smaller than the total worker count — but once the
	// instantaneous holder count crosses deltaArenaMaxRetained the free-list
	// drops arenas on release and re-allocates (and re-zeroes) one on the next
	// acquisition, which is the cost that free-list exists to remove.
	// Widening a stage therefore buys throughput with retained arena bytes
	// and, past that ceiling, with allocation churn. Halving alone still
	// scales the stage-1 arena floor with core count, so the absolute cap
	// applies on top of it; see the measured basis on maxTreeDiffWorkers.
	blobWorkers := runtime.NumCPU()
	treeWorkers := min(max(2, blobWorkers/2), maxTreeDiffWorkers)

	defer hs.stopProfiling() // Ensure profiling is stopped even on error
	// The tree memo only pays off while this walk resolves first-parent
	// trees; dropping it here keeps the scanner's steady-state memory
	// independent of history size.
	defer hs.treeOIDs.Clear()

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
		// workChan is deep so that the walk's visit callback, which runs on
		// a walk worker, hands off without waiting on the tree stage: a few
		// thousand commitInfo headers (~100 bytes each) buy full walk/tree
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

		for range treeWorkers {
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

		for range blobWorkers {
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

// maxTreeDiffWorkers is the absolute ceiling on the stage-1 tree-diff worker
// pool, applied on top of the NumCPU/2 halving in DiffHistoryHunksFunc.
//
// Measured on a stage-1-bound history (BenchmarkDiffHistoryHunksManySmallCommits,
// 3000 single-file commits over a 200-file tree, 32-core arm64): raising the
// cap to NumCPU is ~7% slower and allocates ~17x more bytes per scan (each
// tree worker pins a delta arena, 34 MiB -> 596 MiB), so the cap costs no
// throughput even when stage 1 dominates — stage-2 hunk workers, which are
// uncapped, set pipeline throughput while extra producers only raise the RSS
// floor. Halving alone would still scale that floor with core count, which is
// why an absolute ceiling and not just a ratio. Re-run that benchmark before
// changing this value.
const maxTreeDiffWorkers = 8

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

	// inferredRename marks a pairing produced by directory-rename inference
	// rather than an in-place modification seen in the tree diff. Inferred
	// pairings are path-based guesses, so stage 2 validates them by content
	// before trusting the pair diff (see gateInferredRenameHunks).
	inferredRename bool
}

// blobIdentity names the content a tree entry contributes: the blob OID plus
// the entry's type nibble. Suppression pairs entries by identity rather than
// by OID alone because an OID match is not a content match across types. A
// regular file whose bytes are exactly a path string hashes identically to a
// symlink pointing at that path, so keying on the OID alone lets a deleted
// regular file suppress an added symlink and drop that symlink's target from
// hunk output. Permission bits are masked off: they are not blob content, so
// an exec-bit change does not defeat a move whose bytes are unchanged.
type blobIdentity struct {
	oid  Hash
	kind uint32
}

func makeBlobIdentity(oid Hash, mode uint32) blobIdentity {
	return blobIdentity{oid: oid, kind: mode & modeTypeMask}
}

// blobPairCandidate is one buffered suppression candidate: the stage-2 work
// record plus the type nibble of the entry that produced it. Only the nibble is
// retained rather than a whole blobIdentity because the record already carries
// newOID, and the deletion pool is consulted once per candidate.
type blobPairCandidate struct {
	work blobPairWork
	kind uint32
}

// identity names the bytes this candidate introduces. kind is already masked to
// the type nibble, so this does not re-mask.
func (c blobPairCandidate) identity() blobIdentity {
	return blobIdentity{oid: c.work.newOID, kind: c.kind}
}

// deletedEntry is one deletion observed in a commit's first-parent diff: the
// bytes that left the tree plus the path they left. The path is what makes
// directory-rename inference possible, so it is retained even though
// suppression alone would need only a per-identity credit count.
type deletedEntry struct {
	path string
	oid  Hash
	kind uint32
}

func (d deletedEntry) identity() blobIdentity {
	return blobIdentity{oid: d.oid, kind: d.kind}
}

// Buffering these many suppression candidates and path bytes keeps the
// per-worker record/path budget below 1 MiB while preserving a single tree
// walk for ordinary commits. Commits beyond either limit replay candidates
// instead.
const (
	maxBufferedBlobPairCandidates = 4096
	maxBufferedBlobPairPathBytes  = 512 << 10
)

// maxRetainedDeletePathBytes bounds the deleted PATH bytes one commit may
// retain for directory-rename inference, separately from the candidate budget.
//
// Suppression itself needs only a per-identity credit count, which is O(1) for
// the shape that dominates mass deletions: thousands of byte-identical
// placeholder files (empty __init__.py, .gitkeep, generated boilerplate) all
// collapse to one credit. Inference is what needs the paths, and paths are
// O(deleted files) with no such collapse. Retaining them unconditionally would
// make a deletion-only vendor-tree cleanup — a commit where no candidate exists
// for inference to help — allocate per deleted file for nothing. Past this
// bound the paths are dropped and only the credits are kept.
//
// It is a variable only so tests can reach the drop path without building a
// half-megabyte of paths, matching maxDiffSize. Overriding it is safe only from
// a test that does not call t.Parallel().
var maxRetainedDeletePathBytes = 512 << 10

// exactRenameEvidence records one exact-OID rename observed within a single
// commit's first-parent diff: the deleted old path and the added new path
// carry identical blob content. Accumulated evidence drives
// inferDirectoryRenames.
type exactRenameEvidence struct {
	oldPath string
	newPath string
}

// directoryRenameCandidate is one inferred (oldDir -> newDir) directory
// rename, weighted by the number of exact-OID renames observed between the
// two directories.
type directoryRenameCandidate struct {
	oldDir string
	newDir string
	count  int
}

const (
	// minDirectoryRenameEvidence is the number of exact-OID renames between
	// one (oldDir, newDir) pair required before a directory rename is
	// inferred. A single rename is no evidence of a directory-level move.
	// Because same-identity pairing prefers same-basename matches, requiring
	// two corroborating renames also bounds the false positives that
	// placeholder churn (empty __init__.py, .gitkeep, generated
	// boilerplate) would otherwise produce.
	minDirectoryRenameEvidence = 2

	// maxDirectoryRenameCandidates caps the per-commit candidate set;
	// commits exceeding it skip directory inference entirely (see
	// inferDirectoryRenames).
	maxDirectoryRenameCandidates = 1024
)

// emitCommitBlobPairs walks the first-parent tree diff of a single commit and
// fans out one blobPairWork per content-changing blob entry. It filters
// deletions, unchanged and mode-only entries, and entries whose resulting bytes
// are paired one-for-one with a same-commit deletion of the same blob identity
// (exact-OID moves). Both pure additions and modifications are suppression
// candidates: a move that overwrites a tracked destination surfaces as a
// modification whose resulting blob is the deleted blob. An addition left
// unsuppressed under an inferred directory rename is rewritten into a modify
// pair against the deleted blob, flagged inferredRename so stage 2 can
// content-validate the guess. Tree walking is cheap relative to blob diffing,
// so this stage keeps the expensive stage-2 workers supplied with fine-grained
// work even when one commit touches thousands of files.
//
// Root and shallow-parent commits stream in one pass. Non-root commits index
// deletions in the first pass and retain candidates within a bounded budget,
// replaying them in a second walk only when that budget is exceeded. Replay
// emits inline, so it applies exact-OID suppression but not directory-rename
// inference: inference needs every unsuppressed candidate in hand before it can
// pair any of them, which is exactly the retention the budget refused.
func (hs *HistoryScanner) emitCommitBlobPairs(c commitInfo, parentTree Hash, blobs chan<- blobPairWork, stopCh <-chan struct{}) error {
	emit := func(work blobPairWork) error {
		select {
		case <-stopCh:
			return errScanAborted
		case blobs <- work:
			return nil
		}
	}

	// A zero parent tree (root commit, shallow history) has no old side:
	// every entry is an addition and no deletion can exist, so no rename is
	// possible. Emit during the walk to preserve channel backpressure and
	// avoid retaining one work record per file.
	if parentTree.IsZero() {
		return walkDiff(hs.store, parentTree, c.TreeOID, "", func(path string, old, newH Hash, mode uint32) error {
			// Ahead of the filters, matching the non-root and replay
			// callbacks: entries that fall out here never reach emit, so
			// without this a tree of nothing but gitlinks would traverse to
			// completion after another worker had already failed.
			select {
			case <-stopCh:
				return errScanAborted
			default:
			}
			if !isBlobMode(mode) || old == newH || newH.IsZero() {
				return nil
			}
			return emit(blobPairWork{commit: c.OID, path: path, oldOID: old, newOID: newH})
		})
	}

	var (
		candidates []blobPairCandidate

		// deletes and deletesByIdentity are the deletion pool. Every deletion
		// contributes a credit to its identity's group, which is all
		// suppression needs. The path-bearing deletes slice exists only so a
		// suppressed candidate can name the exact path its bytes came from --
		// the evidence directory-rename inference runs on -- and is dropped
		// past maxRetainedDeletePathBytes.
		deletes                     []deletedEntry
		deletesByIdentity           map[blobIdentity]*deleteGroup
		deletePathBytes             int
		deletePathsDroppedForCommit bool

		retainedPathBytes int
		replayCandidates  bool
	)
	err := walkDiff(hs.store, parentTree, c.TreeOID, "", func(path string, old, newH Hash, mode uint32) error {
		select {
		case <-stopCh:
			return errScanAborted
		default:
		}
		if !isBlobMode(mode) {
			return nil
		}
		if old == newH {
			return nil
		}
		if newH.IsZero() {
			// walkDiff reports a deletion with the deleted entry's own mode,
			// so this identity describes the bytes that left the tree.
			if deletesByIdentity == nil {
				deletesByIdentity = make(map[blobIdentity]*deleteGroup, 4)
			}
			entry := deletedEntry{path: path, oid: old, kind: mode & modeTypeMask}
			g := deletesByIdentity[entry.identity()]
			if g == nil {
				g = &deleteGroup{}
				deletesByIdentity[entry.identity()] = g
			}
			g.remaining++

			if !deletePathsDroppedForCommit &&
				len(path) > maxRetainedDeletePathBytes-deletePathBytes {
				// Give up naming sources for this commit rather than grow the
				// path pool. Credits already collected stay valid; the indices
				// that pointed into the discarded slice must not.
				for _, other := range deletesByIdentity {
					other.indices = nil
				}
				deletes = nil
				deletePathBytes = 0
				deletePathsDroppedForCommit = true
			}
			if deletePathsDroppedForCommit {
				return nil
			}
			g.indices = append(g.indices, len(deletes))
			deletes = append(deletes, entry)
			deletePathBytes += len(path)
			return nil
		}
		// Every surviving entry contributes new bytes at this path and is a
		// suppression candidate, including a modification: when a move
		// overwrites a tracked destination the destination's resulting blob is
		// byte-identical to the blob deleted in the same commit, so its added
		// lines are bytes the history already carries. Neither kind can be
		// judged until the walk has seen every deletion, so both defer.
		if replayCandidates {
			return nil
		}
		if len(candidates) >= maxBufferedBlobPairCandidates ||
			len(path) > maxBufferedBlobPairPathBytes-retainedPathBytes {
			clear(candidates)
			candidates = nil
			retainedPathBytes = 0
			replayCandidates = true
			return nil
		}
		candidates = append(candidates, blobPairCandidate{
			work: blobPairWork{commit: c.OID, path: path, oldOID: old, newOID: newH},
			kind: mode & modeTypeMask,
		})
		retainedPathBytes += len(path)
		return nil
	})
	if err != nil {
		return err
	}

	// usedDeletes tracks which deletions have already been named as a move's
	// source. The credit counts on each group, not this slice, decide
	// suppression; this only keeps one delete from being named twice.
	usedDeletes := make([]bool, len(deletes))

	if !replayCandidates {
		// Inference needs to name each suppressed candidate's source, so it is
		// available only while the delete paths are retained. Without them the
		// pass still suppresses; it just has no evidence to infer from.
		var emitted []blobPairCandidate
		if deletePathsDroppedForCommit {
			emitted = suppressExactMoves(candidates, deletes, deletesByIdentity, usedDeletes)
		} else {
			emitted = pairCommitRenames(candidates, deletes, deletesByIdentity, usedDeletes)
		}
		for _, cand := range emitted {
			select {
			case <-stopCh:
				return errScanAborted
			default:
			}
			if err := emit(cand.work); err != nil {
				return err
			}
		}
		return nil
	}

	// The bounded buffer was discarded, so nothing is held long enough to
	// infer a directory rename from. Replay only the candidates, consuming one
	// deletion credit per matching identity and emitting the rest inline.
	return walkDiff(hs.store, parentTree, c.TreeOID, "", func(path string, old, newH Hash, mode uint32) error {
		select {
		case <-stopCh:
			return errScanAborted
		default:
		}
		if !isBlobMode(mode) || newH.IsZero() || old == newH {
			return nil
		}
		id := makeBlobIdentity(newH, mode)
		if _, ok := takeExactRenameDelete(id, path, deletes, usedDeletes, deletesByIdentity); ok {
			return nil // exact-OID move: content-addressed bytes are unchanged.
		}
		return emit(blobPairWork{commit: c.OID, path: path, oldOID: old, newOID: newH})
	})
}

// pairCommitRenames classifies one commit's buffered suppression candidates
// into the blob pairs that must flow to the hunk stage. A candidate whose
// resulting bytes match an unconsumed same-identity deletion is an exact-OID
// move and is dropped; a surviving pure addition under an inferred directory
// rename becomes a modify pair against the deleted blob (flagged
// inferredRename so stage 2 can content-validate the guess); every other
// candidate passes through unchanged, in input order.
//
// Pure over its inputs — no I/O, no store access — which keeps it directly
// unit-testable and benchmarkable. It filters candidates in place, reusing the
// backing array, and consumes deletesByIdentity and used.
// suppressExactMoves filters out the candidates whose bytes an unconsumed
// same-identity deletion already accounts for, and returns the rest in input
// order. It is pairCommitRenames without the inference half, for commits whose
// delete paths were dropped: suppression needs only the credits, so it stays
// exact where inference cannot run at all.
//
// Filters in place, reusing the candidates backing array.
func suppressExactMoves(
	candidates []blobPairCandidate,
	deletes []deletedEntry,
	deletesByIdentity map[blobIdentity]*deleteGroup,
	used []bool,
) []blobPairCandidate {
	unmatched := candidates[:0]
	for i := range candidates {
		_, suppressed := takeExactRenameDelete(
			candidates[i].identity(), candidates[i].work.path, deletes, used, deletesByIdentity)
		if suppressed {
			continue // exact-OID move: content-addressed bytes are unchanged.
		}
		unmatched = append(unmatched, candidates[i])
	}
	return unmatched
}

func pairCommitRenames(
	candidates []blobPairCandidate,
	deletes []deletedEntry,
	deletesByIdentity map[blobIdentity]*deleteGroup,
	used []bool,
) []blobPairCandidate {
	unmatched := candidates[:0]
	var evidence []exactRenameEvidence
	for i := range candidates {
		deleteIdx, ok := takeExactRenameDelete(
			candidates[i].identity(), candidates[i].work.path, deletes, used, deletesByIdentity)
		if ok {
			evidence = append(evidence, exactRenameEvidence{
				oldPath: deletes[deleteIdx].path,
				newPath: candidates[i].work.path,
			})
			continue // exact-OID move: content-addressed bytes are unchanged.
		}
		unmatched = append(unmatched, candidates[i])
	}

	// A commit whose every candidate was an exact-OID move — a plain directory
	// move — leaves nothing for directory inference to pair, so skip both the
	// inference and the by-path index it feeds.
	if len(unmatched) == 0 {
		return unmatched
	}

	dirRenames := inferDirectoryRenames(evidence)
	if len(dirRenames.ordered) == 0 {
		return unmatched
	}

	// Exactly one delete was consumed per evidence entry, so this sizes the
	// index to the deletes still available rather than to every delete.
	unusedDeletesByPath := make(map[string]int, len(deletes)-len(evidence))
	for i := range deletes {
		if !used[i] {
			unusedDeletesByPath[deletes[i].path] = i
		}
	}

	for i := range unmatched {
		// Only a pure addition can be re-paired. A modification already has a
		// real old side from the tree diff, and overwriting it with a
		// path-inferred guess would replace observed history with a guess.
		if !unmatched[i].work.oldOID.IsZero() {
			continue
		}
		deleteIdx, ok := matchDirectoryRename(
			unmatched[i].work.path, unmatched[i].kind,
			dirRenames, deletes, unusedDeletesByPath, used)
		if !ok {
			continue
		}
		used[deleteIdx] = true
		delete(unusedDeletesByPath, deletes[deleteIdx].path)
		unmatched[i].work.oldOID = deletes[deleteIdx].oid
		unmatched[i].work.inferredRename = true
	}
	return unmatched
}

// deleteGroup tracks the deletes sharing one blob identity within a single
// commit, structured so that pairing A candidates against D same-identity
// deletes costs O(A+D) overall instead of rescanning the group per candidate:
// consumed candidates are never revisited (basename chains pop from the head;
// the fallback cursor only advances).
//
// remaining, not indices, is the suppression authority. The two agree while
// paths are retained, but a commit past maxRetainedDeletePathBytes drops
// indices and keeps remaining, so suppression survives at O(distinct
// identities) while inference is given up.
type deleteGroup struct {
	// remaining is the number of this identity's deletions that have not yet
	// silenced a candidate. It is the only field suppression consults.
	remaining int

	// indices into the commit's deletes slice, in tree-walk order. Nil once
	// the commit's delete paths have been dropped, which is what makes a
	// consumed delete unnameable and therefore inference unavailable.
	indices []int

	// cursor is the first-available fallback scan position over indices.
	// Entries consumed through the basename chains are skipped via the used
	// flags when the cursor reaches them, so total cursor movement is
	// O(len(indices)).
	cursor int

	// baseHead/baseNext form per-basename FIFO chains over positions in
	// indices (head+next representation rather than map[string][]int so a
	// group with thousands of distinct basenames costs two allocations, not
	// one slice per basename). Built lazily on the first lookup that sees
	// more than one candidate: singleton groups — the overwhelmingly common
	// case — never pay for it. baseNext[pos] == -1 terminates a chain.
	baseHead map[string]int
	baseNext []int
}

// takeExactRenameDelete consumes one deletion credit for the bytes identified
// by id, arriving at newPath, and reports the index of the delete that was
// consumed. Matching is one-for-one, so each deletion silences at most one
// candidate.
//
// The returned index names the consumed delete for rename evidence and is
// valid only while the commit's delete paths are retained; it is
// deletePathsDropped when they are not, which callers that infer renames must
// not see because inference is disabled in that case. With paths, the choice
// prefers a same-basename delete and falls back to the first unconsumed one in
// tree-walk order. Amortized O(1) per call.
func takeExactRenameDelete(
	id blobIdentity,
	newPath string,
	deletes []deletedEntry,
	used []bool,
	deletesByIdentity map[blobIdentity]*deleteGroup,
) (int, bool) {
	g := deletesByIdentity[id]
	if g == nil || g.remaining == 0 {
		return 0, false
	}
	g.remaining--

	if g.indices == nil {
		// Paths were dropped: the credit is spent but the source is unnameable.
		return deletePathsDropped, true
	}
	if len(g.indices) == 1 {
		// Basename preference is irrelevant with a single candidate.
		idx := g.indices[0]
		used[idx] = true
		return idx, true
	}

	if g.baseHead == nil {
		g.baseHead = make(map[string]int, len(g.indices))
		g.baseNext = make([]int, len(g.indices))
		// Built in reverse so each chain head is the earliest position and
		// the chain walks forward in tree-walk order.
		for pos := len(g.indices) - 1; pos >= 0; pos-- {
			base := pathBase(deletes[g.indices[pos]].path)
			if head, ok := g.baseHead[base]; ok {
				g.baseNext[pos] = head
			} else {
				g.baseNext[pos] = -1
			}
			g.baseHead[base] = pos
		}
	}
	if base := pathBase(newPath); len(base) > 0 {
		pos, ok := g.baseHead[base]
		for ok && pos >= 0 {
			idx := g.indices[pos]
			pos = g.baseNext[pos]
			// Advance the head past this entry whether it is consumed now
			// (matched) or was consumed earlier via the fallback cursor, so
			// no position is ever visited twice.
			g.baseHead[base] = pos
			if !used[idx] {
				used[idx] = true
				return idx, true
			}
		}
	}

	// Fallback: first unconsumed candidate in tree-walk order. Entries
	// already taken through the basename chains are skipped here exactly
	// once.
	for g.cursor < len(g.indices) {
		idx := g.indices[g.cursor]
		g.cursor++
		if !used[idx] {
			used[idx] = true
			return idx, true
		}
	}
	// remaining was positive, so an unconsumed index must exist while indices
	// is populated. If the two ever disagree, hand the credit back and report
	// no match: the candidate is then emitted rather than suppressed, which
	// can only over-report added lines. Panicking here instead would take down
	// the caller's process from inside a scan worker.
	g.remaining++
	return 0, false
}

// deletePathsDropped is the index takeExactRenameDelete returns when a credit
// was spent but the commit's delete paths are no longer retained.
const deletePathsDropped = -1

// directoryRenameIndex holds the inferred directory-rename candidates both in
// global priority order (for deterministic first-match-wins pairing) and
// chained by newDir, so per-add candidate lookup costs O(path depth) instead
// of a linear scan over every candidate.
type directoryRenameIndex struct {
	ordered []directoryRenameCandidate

	// headByNewDir/nextSameDir form per-newDir chains over positions in
	// ordered, each chain ascending by position (= global priority rank).
	// nextSameDir[pos] == -1 terminates a chain.
	headByNewDir map[string]int
	nextSameDir  []int
}

func inferDirectoryRenames(evidence []exactRenameEvidence) directoryRenameIndex {
	if len(evidence) < minDirectoryRenameEvidence {
		return directoryRenameIndex{}
	}
	counts := make(map[[2]string]int, len(evidence))
	for _, ev := range evidence {
		if pathBase(ev.oldPath) != pathBase(ev.newPath) {
			continue
		}
		oldDir := pathDir(ev.oldPath)
		newDir := pathDir(ev.newPath)
		if oldDir == "" || newDir == "" || oldDir == newDir {
			continue
		}
		counts[[2]string{oldDir, newDir}]++
	}
	if len(counts) == 0 {
		return directoryRenameIndex{}
	}
	candidates := make([]directoryRenameCandidate, 0, len(counts))
	for dirs, count := range counts {
		if count < minDirectoryRenameEvidence {
			continue
		}
		candidates = append(candidates, directoryRenameCandidate{
			oldDir: dirs[0],
			newDir: dirs[1],
			count:  count,
		})
		if len(candidates) > maxDirectoryRenameCandidates {
			// Directory inference is an optimization. Falling back to pure
			// additions keeps work bounded without hiding new content.
			return directoryRenameIndex{}
		}
	}
	if len(candidates) == 0 {
		return directoryRenameIndex{}
	}
	// matchDirectoryRename is first-match-wins over this priority order and
	// the backing map's iteration order is randomized, so the comparator
	// must be a TOTAL order: any tie would let scheduler/map noise pick
	// which delete an ambiguous add pairs with, changing emitted hunks
	// between runs and breaking the determinism DiffHistoryHunks documents.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].count != candidates[j].count {
			return candidates[i].count > candidates[j].count
		}
		if len(candidates[i].newDir) != len(candidates[j].newDir) {
			return len(candidates[i].newDir) > len(candidates[j].newDir)
		}
		if candidates[i].newDir != candidates[j].newDir {
			return candidates[i].newDir < candidates[j].newDir
		}
		// (oldDir, newDir) pairs are unique map keys, so this final
		// comparison makes the order total.
		return candidates[i].oldDir < candidates[j].oldDir
	})

	headByNewDir := make(map[string]int, len(candidates))
	nextSameDir := make([]int, len(candidates))
	// Built in reverse so each chain head is the lowest position (highest
	// global priority) and chains walk in ascending priority order.
	for pos := len(candidates) - 1; pos >= 0; pos-- {
		if head, ok := headByNewDir[candidates[pos].newDir]; ok {
			nextSameDir[pos] = head
		} else {
			nextSameDir[pos] = -1
		}
		headByNewDir[candidates[pos].newDir] = pos
	}
	return directoryRenameIndex{
		ordered:      candidates,
		headByNewDir: headByNewDir,
		nextSameDir:  nextSameDir,
	}
}

// matchDirectoryRename returns the unconsumed delete whose path reconstructs
// this add under the highest-priority applicable directory-rename candidate.
// Only candidates whose newDir is an ancestor directory of addPath can apply,
// so the lookup walks the add's O(depth) ancestor chain against the byNewDir
// index instead of scanning every candidate; applicable candidates are then
// tried in global priority order, preserving the first-match-wins semantics
// of a linear scan over the ordered slice.
//
// A delete only reconstructs the add if its entry TYPE matches kind as well as
// its path. Path alone would let a deleted regular file whose bytes are a path
// string be paired with an added symlink to that path: the two share a blob
// OID, so the resulting pair diff is old == new and emits nothing, dropping the
// symlink's target from the stream. That is the same conflation blobIdentity
// rejects for exact-OID suppression, and inference must not reintroduce it.
func matchDirectoryRename(
	addPath string,
	kind uint32,
	renames directoryRenameIndex,
	deletes []deletedEntry,
	deletesByPath map[string]int,
	used []bool,
) (int, bool) {
	// Collect the priority ranks of candidates rooted at each ancestor dir.
	// Typical paths are a handful of levels deep and few ancestors are
	// candidate roots, so the fixed buffer keeps this allocation-free.
	var ranksBuf [16]int
	ranks := ranksBuf[:0]
	for dir := pathDir(addPath); dir != ""; dir = pathDir(dir) {
		pos, ok := renames.headByNewDir[dir]
		for ok && pos >= 0 {
			ranks = append(ranks, pos)
			pos = renames.nextSameDir[pos]
		}
	}
	if len(ranks) == 0 {
		return 0, false
	}
	sort.Ints(ranks)

	for _, rank := range ranks {
		candidate := renames.ordered[rank]
		rel, ok := trimPathPrefix(addPath, candidate.newDir)
		if !ok {
			continue
		}
		oldPath := joinPath(candidate.oldDir, rel)
		idx, ok := deletesByPath[oldPath]
		if ok && !used[idx] && deletes[idx].kind == kind {
			return idx, true
		}
	}
	return 0, false
}

// pathDir returns the directory portion of a '/'-separated git tree path.
// Unlike path.Dir it returns "" (not ".") for a rootless path, letting
// callers treat "" as "no parent directory" in ancestor walks.
func pathDir(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return ""
	}
	return p[:i]
}

// pathBase returns the final element (the basename) of a '/'-separated git
// tree path.
func pathBase(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return p
	}
	return p[i+1:]
}

// trimPathPrefix returns the remainder of the '/'-separated git tree path p
// beneath the directory prefix, reporting whether p lies strictly beneath
// it; an empty prefix denotes the tree root and matches every path.
func trimPathPrefix(p, prefix string) (string, bool) {
	if prefix == "" {
		return p, true
	}
	if !strings.HasPrefix(p, prefix) || len(p) == len(prefix) || p[len(prefix)] != '/' {
		return "", false
	}
	return p[len(prefix)+1:], true
}

// streamBlobPairHunks computes the added hunks for one changed file and
// delivers them to fn. Results are memoized by OID pair: the diff depends
// only on the two blob contents, and histories with merges replay the same
// transition repeatedly.
func (hs *HistoryScanner) streamBlobPairHunks(work blobPairWork, fn func(HunkAddition) error) error {
	hunks, err := hs.pairAddedHunks(work.oldOID, work.newOID)
	if err != nil {
		return err
	}

	if work.inferredRename {
		hunks, err = hs.gateInferredRenameHunks(work.oldOID, work.newOID, hunks)
		if err != nil {
			return err
		}
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

		// Emit each hunk exactly as computeAddedHunks produced it;
		// adjacent hunks are not merged.
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

// pairAddedHunks returns the added hunks for one (old,new) blob transition,
// memoized by OID pair. The cached value is the raw pair diff, independent of
// how the pairing was discovered, so genuine modifications and inferred
// renames of the same transition share one entry.
func (hs *HistoryScanner) pairAddedHunks(oldOID, newOID Hash) ([]AddedHunk, error) {
	pk := makePairKey(oldOID, newOID)
	hunks, cached := hs.pairs.get(pk)
	if cached {
		return hunks, nil
	}
	computed, err := computeAddedHunks(hs.store, oldOID, newOID)
	if err != nil {
		return nil, fmt.Errorf("compute added hunks: %w", err)
	}
	// Return what the cache hands back, not what computeAddedHunks produced:
	// the computed Lines are zero-copy views into the whole decompressed new
	// blob, so a HunkAddition built from them keeps that blob alive for as long
	// as any consumer holds the hunk.
	return hs.pairs.add(pk, computed), nil
}

// gateInferredRenameHunks validates a directory-rename pairing by content.
// The pairing was inferred purely from paths, so the two blobs may be
// unrelated; trusting the pair diff would silently drop any coincidentally
// shared lines from the added-hunk stream. Mirroring Git's rename detection,
// the pair is kept only when the lines common to both files are at least half
// of the larger file; otherwise the file is reported as a whole-file addition.
//
// The decision is made from the two blobs rather than from the hunks, because
// computeAddedHunks has three outcomes and only one of them describes this
// file's added lines:
//
//   - New side oversized or binary: the whole new content is emitted without
//     consulting the old blob at all, so the guess cannot have shaped it and a
//     pure-add fallback would be identical. Kept as-is.
//   - Old side oversized or binary: the result is a size placeholder or a
//     single binary hunk, a shape chosen by the guessed half. It says nothing
//     about this file, and its one line passes any similarity threshold, so it
//     must be rejected rather than measured.
//   - Either side over SmallFileThreshold: computeAddedHunks used the line-set
//     or hashing algorithm, which is a set-membership test rather than a
//     multiplicity-aware diff, so its added-line count cannot be turned into a
//     similarity score. Rejected rather than measured.
//   - Both sides small text: a real line diff, which the similarity test below
//     can measure.
//
// An empty hunk list is not a shortcut to "trustworthy". Exact-OID moves never
// reach here — stage 1 suppresses them — so a pairing that arrives with zero
// added lines has two different blobs whose new side is a line-wise subset of
// the old one. That is what an unrelated pairing looks like when the new file
// is much smaller, so it is measured like any other.
func (hs *HistoryScanner) gateInferredRenameHunks(oldOID, newOID Hash, hunks []AddedHunk) ([]AddedHunk, error) {
	if oldOID.IsZero() {
		// Not actually a pairing: nothing was guessed.
		return hunks, nil
	}

	newBytes, err := loadBlob(hs.store, newOID)
	if err != nil {
		return nil, fmt.Errorf("load new blob for rename gate: %w", err)
	}
	if int64(len(newBytes)) > maxDiffSize || isBinary(newBytes) {
		return hunks, nil // New-side-determined; old blob was never consulted.
	}

	oldBytes, err := loadBlob(hs.store, oldOID)
	if err != nil {
		return nil, fmt.Errorf("load old blob for rename gate: %w", err)
	}
	if int64(len(oldBytes)) > maxDiffSize || isBinary(oldBytes) {
		// The pair diff was shaped by the guessed side, so it cannot be
		// measured. Report the file on its own terms.
		return hs.pairAddedHunks(Hash{}, newOID)
	}
	if int64(len(oldBytes)) > SmallFileThreshold || int64(len(newBytes)) > SmallFileThreshold {
		// Past SmallFileThreshold computeAddedHunks switches to the line-set
		// and hashing algorithms, which are set-membership tests: one
		// occurrence of a line in the old blob marks EVERY occurrence of it in
		// the new blob as not added. `added` is then an undercount and the
		// `common` derived from it an overcount, so the similarity test below
		// cannot be trusted here — a one-line old file paired with a megabyte
		// of that same line repeated scores as fully common and the whole new
		// file would be suppressed. Measuring it properly needs a
		// multiplicity-aware common-line count over both blobs, which is a
		// second full diff; until then an unmeasurable pairing is rejected the
		// same way an oversized or binary one is. The cost is that an inferred
		// rename of a file over 1 MB is always reported whole rather than as
		// its added lines.
		return hs.pairAddedHunks(Hash{}, newOID)
	}

	newTotal := tokenizedLineCount(newBytes)
	oldTotal := tokenizedLineCount(oldBytes)
	added := 0
	for i := range hunks {
		added += len(hunks[i].Lines)
	}

	// Similarity gate, mirroring Git's rename score: the lines the new file
	// shares with the old one must be at least half of the LARGER side.
	//
	// The denominator has to be max(oldTotal, newTotal) and not newTotal
	// alone, because a new-side-only score cannot see a pairing that merely
	// shrinks. An unrelated one-line new file whose single line happens to
	// occur somewhere in a 100-line deleted file produces ZERO added lines, so
	// scoring added-over-newTotal keeps the pairing and emits no hunk at all —
	// the new file's only line never reaches the stream. Git treats that case
	// as a delete plus a create for the same reason: its similarity
	// denominator is max(src, dst), so a large deletion counts against the
	// pairing.
	common := newTotal - added
	if common*2 >= max(oldTotal, newTotal) {
		return hunks, nil
	}
	return hs.pairAddedHunks(Hash{}, newOID)
}

// tokenizedLineCount counts the lines tokenize would produce for b, so
// similarity arithmetic is expressed in the same units as a hunk's Lines.
func tokenizedLineCount(b []byte) int {
	n := bytes.Count(b, nlByte)
	if len(b) > 0 && b[len(b)-1] != '\n' {
		n++ // Trailing line without a newline, matching tokenize.
	}
	return n
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
//
// The pair cache is cleared as well. Callers may retain a HistoryScanner value
// after Close, and a hunk scan leaves that cache holding up to its full budget
// of hunk lines — plus, for whole-blob entries, the object buffers those lines
// view — which would otherwise stay reachable until the scanner itself does.
// This mirrors store.Close releasing the offset cache's object bytes.
func (hs *HistoryScanner) Close() error {
	hs.pairs.clear()
	return hs.store.Close()
}

// CommitMetadata bundles the author identity, commit timestamp, and commit
// message for a single commit.
//
// Instances are immutable and therefore safe for concurrent reads.
type CommitMetadata struct {
	// Author records the commit author exactly as stored in the commit header.
	Author AuthorInfo

	// Timestamp holds the committer time in seconds since the Unix epoch.
	Timestamp int64

	// Message holds the raw commit message: the bytes after the first blank
	// line of the commit object, byte-faithful to `git cat-file commit`
	// output. No encoding normalization or NUL truncation is applied (those
	// are git-log presentation behaviors, not object format); any such
	// normalization belongs to the consumer. Empty when the commit has no
	// message.
	Message string
}

// GetCommitMetadata returns (and caches) the commit's author, timestamp, and
// message.
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
