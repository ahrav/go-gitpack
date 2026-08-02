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
// Renames are detected within each first-parent diff, mirroring Git. A
// delete+add pair carrying identical blob content (an exact-OID rename,
// preferring same-basename matches) emits no hunks, since no line changed.
// When at least two exact renames establish a directory rename, an added
// file under the new directory whose old-path counterpart was deleted is
// diffed against that old blob instead of being reported as a whole-file
// addition. That path-based pairing is content-validated: it is kept only
// when at least half of the new file's lines are common with the old file,
// matching Git's rename similarity threshold. All rename pairing is scoped
// to a single commit's first-parent diff; renames are never tracked across
// commits.
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
// The HunkAddition channel is deeply buffered (see the sizing rationale in
// the implementation) so workers can make progress without waiting for the
// consumer on every hunk. The errC channel is buffered to 1 so the producer
// goroutine can always send its final error without blocking.
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
// using the same first-parent and rename-detection semantics as
// DiffHistoryHunks.
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
	// Capping the producer stage prevents one 32 MiB delta arena per CPU
	// from becoming a hidden RSS floor on machines with many cores. Even on
	// stage-1-bound histories the cap costs no throughput; see the measured
	// basis on maxTreeDiffWorkers.
	treeWorkers := min(numWorkers, maxTreeDiffWorkers)

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

// maxTreeDiffWorkers caps the stage-1 tree-diff worker pool.
//
// Measured on a stage-1-bound history (BenchmarkDiffHistoryHunksManySmallCommits,
// 3000 single-file commits over a 200-file tree, 32-core arm64): raising the
// cap to NumCPU is ~7% slower and allocates ~17x more bytes per scan (each
// tree worker pins a delta arena, 34 MiB -> 596 MiB), so the cap costs no
// throughput even when stage 1 dominates — stage-2 hunk workers, which are
// uncapped, set pipeline throughput while extra producers only raise the RSS
// floor. Re-run that benchmark before changing this value.
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
	// Because same-OID pairing prefers same-basename matches, requiring two
	// corroborating renames also bounds the false positives that
	// placeholder churn (empty __init__.py, .gitkeep, generated
	// boilerplate) would otherwise produce.
	minDirectoryRenameEvidence = 2

	// maxDirectoryRenameCandidates caps the per-commit candidate set;
	// commits exceeding it skip directory inference entirely (see
	// inferDirectoryRenames).
	maxDirectoryRenameCandidates = 1024
)

// emitCommitBlobPairs walks the first-parent tree diff of a single commit and
// fans out the blobPairWork the hunk stage must compute. In-place
// modifications are emitted as they are seen; pure adds and deletes are
// buffered and classified through pairCommitRenames, so exact-OID renames
// emit nothing, adds under an inferred directory rename become modify pairs
// against the deleted blob, and the remaining adds are emitted unchanged.
// Tree walking is cheap relative to blob diffing, so this stage keeps the
// expensive stage-2 workers supplied with fine-grained work even when one
// commit touches thousands of files.
func (hs *HistoryScanner) emitCommitBlobPairs(c commitInfo, parentTree Hash, blobs chan<- blobPairWork, stopCh <-chan struct{}) error {
	emit := func(work blobPairWork) error {
		select {
		case <-stopCh:
			return errScanAborted
		case blobs <- work:
			return nil
		}
	}

	var (
		adds         []blobPairWork
		deletes      []blobPairWork
		deletesByOID map[Hash]*deleteGroup
	)
	err := walkDiff(hs.store, parentTree, c.TreeOID, "", func(path string, old, newH Hash, mode uint32) error {
		if !isBlobMode(mode) {
			return nil
		}
		if old == newH {
			return nil
		}
		work := blobPairWork{commit: c.OID, path: path, oldOID: old, newOID: newH}
		switch {
		case newH.IsZero():
			if deletesByOID == nil {
				deletesByOID = make(map[Hash]*deleteGroup, 4)
			}
			g := deletesByOID[old]
			if g == nil {
				g = &deleteGroup{}
				deletesByOID[old] = g
			}
			g.indices = append(g.indices, len(deletes))
			deletes = append(deletes, work)
			return nil
		case old.IsZero():
			adds = append(adds, work)
			return nil
		default:
			return emit(work)
		}
	})
	if err != nil {
		return err
	}

	for _, work := range pairCommitRenames(adds, deletes, deletesByOID) {
		if err := emit(work); err != nil {
			return err
		}
	}
	return nil
}

// pairCommitRenames classifies one commit's pure adds and deletes into the
// blob pairs that must flow to the hunk stage. Exact-OID renames are
// suppressed entirely; adds under an inferred directory rename become modify
// pairs against the deleted blob (flagged inferredRename so stage 2 can
// content-validate the guess); every other add passes through unchanged, in
// input order.
//
// Pure over its inputs — no I/O, no store access — which keeps it directly
// unit-testable and benchmarkable. It reuses adds' backing array for the
// result and consumes deletesByOID.
func pairCommitRenames(adds, deletes []blobPairWork, deletesByOID map[Hash]*deleteGroup) []blobPairWork {
	usedDeletes := make([]bool, len(deletes))
	unmatchedAdds := adds[:0]
	var evidence []exactRenameEvidence
	for i := range adds {
		deleteIdx, ok := takeExactRenameDelete(adds[i], deletes, usedDeletes, deletesByOID)
		if ok {
			usedDeletes[deleteIdx] = true
			evidence = append(evidence, exactRenameEvidence{
				oldPath: deletes[deleteIdx].path,
				newPath: adds[i].path,
			})
			continue // exact-OID rename: content-addressed bytes are unchanged.
		}
		unmatchedAdds = append(unmatchedAdds, adds[i])
	}

	// A commit whose every add was an exact-OID rename — a plain directory
	// move — leaves nothing for directory inference to pair, so skip both the
	// inference and the by-path index it feeds.
	if len(unmatchedAdds) == 0 {
		return unmatchedAdds
	}

	dirRenames := inferDirectoryRenames(evidence)
	if len(dirRenames.ordered) == 0 {
		return unmatchedAdds
	}

	// Exactly one delete was consumed per evidence entry, so this sizes the
	// index to the deletes still available rather than to every delete.
	unusedDeletesByPath := make(map[string]int, len(deletes)-len(evidence))
	for i := range deletes {
		if !usedDeletes[i] {
			unusedDeletesByPath[deletes[i].path] = i
		}
	}

	for i := range unmatchedAdds {
		if deleteIdx, ok := matchDirectoryRename(unmatchedAdds[i].path, dirRenames, unusedDeletesByPath, usedDeletes); ok {
			usedDeletes[deleteIdx] = true
			delete(unusedDeletesByPath, deletes[deleteIdx].path)
			unmatchedAdds[i].oldOID = deletes[deleteIdx].oldOID
			unmatchedAdds[i].inferredRename = true
		}
	}
	return unmatchedAdds
}

// deleteGroup tracks the deletes sharing one blob OID within a single commit,
// structured so that pairing A adds against D same-OID deletes costs O(A+D)
// overall instead of rescanning the group per add: consumed candidates are
// never revisited (basename chains pop from the head; the fallback cursor
// only advances).
type deleteGroup struct {
	// indices into the commit's deletes slice, in tree-walk order.
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

// takeExactRenameDelete returns the delete this add should pair with as an
// exact-OID rename: a same-basename delete when one is unconsumed, else the
// first unconsumed delete in tree-walk order. Amortized O(1) per call.
func takeExactRenameDelete(add blobPairWork, deletes []blobPairWork, used []bool, deletesByOID map[Hash]*deleteGroup) (int, bool) {
	g := deletesByOID[add.newOID]
	if g == nil {
		return 0, false
	}
	if len(g.indices) == 1 {
		// Basename preference is irrelevant with a single candidate.
		idx := g.indices[0]
		if used[idx] {
			return 0, false
		}
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
	if base := pathBase(add.path); len(base) > 0 {
		pos, ok := g.baseHead[base]
		for ok && pos >= 0 {
			idx := g.indices[pos]
			pos = g.baseNext[pos]
			// Advance the head past this entry whether it is consumed now
			// (matched) or was consumed earlier via the fallback cursor, so
			// no position is ever visited twice.
			g.baseHead[base] = pos
			if !used[idx] {
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
			return idx, true
		}
	}
	return 0, false
}

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
func matchDirectoryRename(addPath string, renames directoryRenameIndex, deletesByPath map[string]int, used []bool) (int, bool) {
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
		if ok && !used[idx] {
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
	hunks, err := computeAddedHunks(hs.store, oldOID, newOID)
	if err != nil {
		return nil, fmt.Errorf("compute added hunks: %w", err)
	}
	hs.pairs.add(pk, hunks)
	return hunks, nil
}

// gateInferredRenameHunks validates a directory-rename pairing by content.
// The pairing was inferred purely from paths, so the two blobs may be
// unrelated; trusting the pair diff would silently drop any coincidentally
// shared lines from the added-hunk stream. Mirroring Git's rename detection,
// the pair is kept only when at least half of the new file's lines are common
// with the old file; otherwise the file is reported as a whole-file addition.
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
//   - Both sides small text: a real line diff, which the similarity test below
//     can measure.
func (hs *HistoryScanner) gateInferredRenameHunks(oldOID, newOID Hash, hunks []AddedHunk) ([]AddedHunk, error) {
	if len(hunks) == 0 {
		// Identical or near-identical content; the pairing is trustworthy.
		return hunks, nil
	}
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

	total := bytes.Count(newBytes, nlByte)
	if len(newBytes) > 0 && newBytes[len(newBytes)-1] != '\n' {
		total++ // Trailing line without a newline, matching tokenize.
	}
	added := 0
	for i := range hunks {
		added += len(hunks[i].Lines)
	}

	// Similarity gate: common = total - added. Keep the pairing only when
	// common*2 >= total, i.e. the pair diff re-created at most half the file.
	if added*2 <= total {
		return hunks, nil
	}
	return hs.pairAddedHunks(Hash{}, newOID)
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
