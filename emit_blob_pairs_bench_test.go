// emit_blob_pairs_bench_test.go
//
// Measurement substrate for stage-1 fan-out (emitCommitBlobPairs) on
// add-heavy commits.
//
// Exact-OID suppression must know a commit's deletion counts before deciding
// which additions to emit. Root commits cannot contain deletions and stream in
// one pass. Non-root commits retain additions within a bounded record/path
// budget and emit them after the first walk. If either limit is exceeded, they
// discard that buffer and replay unmatched additions in a second walk.
// With this fixture's short paths, 4096 additions exercise the bounded
// single-walk path and 16384 additions cross the record limit and replay.
//
// These benchmarks make the root fast path and the non-root time/allocation
// tradeoff visible so implementations can be compared like for like:
//
//   - BenchmarkEmitCommitBlobPairs/RootCommit_* is the zero-parent control,
//     where suppression is impossible.
//   - BenchmarkEmitCommitBlobPairs/NonRootAddCommit_* is the target case: a
//     non-root commit with many additions and no deletions. Both variants run
//     stage 1 alone against a drained channel and report ns/op, B/op, and
//     "first-work-ns" (how long stage 2 waits for its first unit of work).
//   - BenchmarkDiffHistoryHunksColdRootHeavy runs the full pipeline on a
//     fresh scanner per iteration and reports ns/op plus "first-hunk-ns".
//   - BenchmarkDiffHistoryHunksColdNonRootAddHeavy runs the bounded and replay
//     non-root cases through the full pipeline and reports
//     "bulk-first-hunk-ns" for the add-heavy child commit.
//
// File contents are unique per path so no two additions share an OID and the
// pair memo cannot collapse stage-2 work.
package objstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// rootHeavyRepo builds a repository whose root commit adds fileCount text
// files of roughly fileSize bytes under nested directories, followed by one
// commit that modifies a single file. The nesting keeps individual trees
// small, which matches real repositories better than one flat directory
// with thousands of entries.
//
// fileSize controls the stage-2 cost per work unit, which is the variable
// that decides whether overlapping the tree walk with blob diffing helps
// (expensive diffs: workers would otherwise idle during the walk) or hurts
// (cheap diffs: workers spin on a near-empty channel).
func rootHeavyRepo(tb testing.TB, fx *hunkBenchFixtures, fileCount, fileSize int) string {
	tb.Helper()
	name := fmt.Sprintf("root-heavy-%dfiles-%dB", fileCount, fileSize)
	return fx.repo(tb, name, func(tb testing.TB, work string) {
		writeAddHeavyFiles(tb, work, "", fileCount, fileSize)
		gitTB(tb, work, "add", "--", ".")
		date := "1700000000 +0000"
		gitEnvTB(tb, work,
			[]string{"GIT_AUTHOR_DATE=" + date, "GIT_COMMITTER_DATE=" + date},
			"commit", "--quiet", "-m", "root import")

		// One trailing modification so the history is not a single commit.
		path := filepath.Join(work, "d00", "e00", "f00000.txt")
		if err := os.WriteFile(path, []byte("modified\n"), 0o644); err != nil {
			tb.Fatalf("write %s: %v", path, err)
		}
		gitCommitTB(tb, work, filepath.Join("d00", "e00", "f00000.txt"), 1)
	})
}

// nonRootAddHeavyRepo builds a two-commit repository: a one-file root commit
// followed by a commit that adds fileCount files without modifying or deleting
// any existing path. The second commit is the add-heavy non-root shape for
// which exact-OID suppression remains possible in principle, even though this
// fixture has no deletions to match.
func nonRootAddHeavyRepo(tb testing.TB, fx *hunkBenchFixtures, fileCount, fileSize int) string {
	tb.Helper()
	name := fmt.Sprintf("non-root-add-heavy-%dfiles-%dB", fileCount, fileSize)
	return fx.repo(tb, name, func(tb testing.TB, work string) {
		const seed = "seed.txt"
		if err := os.WriteFile(filepath.Join(work, seed), []byte("seed\n"), 0o644); err != nil {
			tb.Fatalf("write seed: %v", err)
		}
		gitCommitTB(tb, work, seed, 0)

		writeAddHeavyFiles(tb, work, "bulk", fileCount, fileSize)
		gitCommitAllTB(tb, work, 1)
	})
}

// writeAddHeavyFiles writes fileCount unique text files below prefix. Two
// directory levels keep individual Git trees small while preserving stable
// lexical ordering across fixture sizes.
func writeAddHeavyFiles(tb testing.TB, work, prefix string, fileCount, fileSize int) {
	tb.Helper()
	for i := range fileCount {
		dir := filepath.Join(work, prefix, fmt.Sprintf("d%02d", i%64), fmt.Sprintf("e%02d", (i/64)%64))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			tb.Fatalf("mkdir %s: %v", dir, err)
		}
		path := filepath.Join(dir, fmt.Sprintf("f%05d.txt", i))
		var body strings.Builder
		for line := 0; body.Len() < fileSize; line++ {
			fmt.Fprintf(&body, "file %05d line %04d %s\n", i, line, textFiller)
		}
		if err := os.WriteFile(path, []byte(body.String()), 0o644); err != nil {
			tb.Fatalf("write %s: %v", path, err)
		}
	}
}

// rootCommitOf returns the root commit of the fixture plus its (zero)
// first-parent tree, so the unit benchmark can call emitCommitBlobPairs with
// exactly the arguments the pipeline would use.
func rootCommitOf(tb testing.TB, hs *HistoryScanner) (commitInfo, Hash) {
	tb.Helper()
	return commitWithParentCount(tb, hs, 0)
}

// nonRootCommitOf returns the fixture's sole non-root commit and its parent
// tree, matching the arguments stage 1 receives from the full pipeline.
func nonRootCommitOf(tb testing.TB, hs *HistoryScanner) (commitInfo, Hash) {
	tb.Helper()
	return commitWithParentCount(tb, hs, 1)
}

func commitWithParentCount(tb testing.TB, hs *HistoryScanner, parentCount int) (commitInfo, Hash) {
	tb.Helper()
	commits, err := hs.loadAllCommits()
	if err != nil {
		tb.Fatalf("load commits: %v", err)
	}
	var (
		found commitInfo
		ok    bool
	)
	for _, c := range commits {
		if len(c.ParentOIDs) != parentCount {
			continue
		}
		if ok {
			tb.Fatalf("fixture has multiple commits with %d parents", parentCount)
		}
		found, ok = c, true
	}
	if !ok {
		tb.Fatalf("fixture has no commit with %d parents", parentCount)
	}
	parentTree, err := hs.firstParentTree(found)
	if err != nil {
		tb.Fatalf("first-parent tree: %v", err)
	}
	return found, parentTree
}

// BenchmarkEmitCommitBlobPairs measures stage 1 alone on root and non-root
// add-heavy commits.
//
// A drain goroutine consumes the work channel and records the arrival time
// of the first unit of work per iteration; the mean is reported as
// "first-work-ns". For roots this is the cost of walking to the first blob.
// For a non-root add-only commit it includes the complete first walk, followed
// by either buffered emission or the walk to the first replayed addition.
func BenchmarkEmitCommitBlobPairs(b *testing.B) {
	fx := newHunkBenchFixtures(b.TempDir())

	for _, fileCount := range []int{4096, 16384} {
		b.Run(fmt.Sprintf("RootCommit_%dfiles", fileCount), func(b *testing.B) {
			gitDir := rootHeavyRepo(b, fx, fileCount, 64)
			hs, err := NewHistoryScanner(gitDir)
			if err != nil {
				b.Fatalf("open scanner: %v", err)
			}
			defer hs.Close()

			root, parentTree := rootCommitOf(b, hs)
			runEmitCommitBlobPairsBench(b, hs, root, parentTree, fileCount)
		})

		b.Run(fmt.Sprintf("NonRootAddCommit_%dfiles", fileCount), func(b *testing.B) {
			gitDir := nonRootAddHeavyRepo(b, fx, fileCount, 64)
			hs, err := NewHistoryScanner(gitDir)
			if err != nil {
				b.Fatalf("open scanner: %v", err)
			}
			defer hs.Close()

			addCommit, parentTree := nonRootCommitOf(b, hs)
			runEmitCommitBlobPairsBench(b, hs, addCommit, parentTree, fileCount)
		})
	}
}

type emitDrainResult struct {
	firstWork int64
	count     int
}

func runEmitCommitBlobPairsBench(b *testing.B, hs *HistoryScanner, commit commitInfo, parentTree Hash, wantCount int) {
	b.Helper()
	stopCh := make(chan struct{})
	var firstWorkTotal, iters int64
	b.ReportAllocs()
	for b.Loop() {
		blobs := make(chan blobPairWork, 4096)
		ready := make(chan struct{})
		done := make(chan emitDrainResult, 1)
		var start time.Time
		go func() {
			close(ready)
			result := emitDrainResult{firstWork: -1}
			for range blobs {
				if result.count == 0 {
					result.firstWork = int64(time.Since(start))
				}
				result.count++
			}
			done <- result
		}()
		<-ready
		start = time.Now()
		if err := hs.emitCommitBlobPairs(commit, parentTree, blobs, stopCh); err != nil {
			b.Fatalf("emitCommitBlobPairs: %v", err)
		}
		close(blobs)
		result := <-done
		if result.count != wantCount {
			b.Fatalf("emitted %d pairs, want %d", result.count, wantCount)
		}
		if result.firstWork < 0 {
			b.Fatal("emitCommitBlobPairs produced no first-work latency")
		}
		firstWorkTotal += result.firstWork
		iters++
	}
	if iters > 0 {
		b.ReportMetric(float64(firstWorkTotal)/float64(iters), "first-work-ns")
	}
}

// BenchmarkDiffHistoryHunksColdRootHeavy measures the full two-stage
// pipeline end to end on the root-heavy fixture with a fresh scanner per
// iteration, so no pair memo or tree cache carries over. "first-hunk-ns" is
// the time from scan start until any stage-2 worker delivers the first
// hunk — the end-to-end view of the stage-2 stall.
//
// The matrix crosses file count with per-file size because stage-2 cost per
// work unit reveals whether streaming the root walk overlaps useful stage-2
// work as per-file diff cost rises.
func BenchmarkDiffHistoryHunksColdRootHeavy(b *testing.B) {
	fx := newHunkBenchFixtures(b.TempDir())

	cases := []struct {
		fileCount, fileSize int
	}{
		{4096, 64},
		{16384, 64},
		{4096, 16 << 10},
		{1024, 256 << 10},
	}
	for _, tc := range cases {
		b.Run(fmt.Sprintf("%dfiles_%dB", tc.fileCount, tc.fileSize), func(b *testing.B) {
			gitDir := rootHeavyRepo(b, fx, tc.fileCount, tc.fileSize)

			var firstHunkTotal, iters int64
			b.ReportAllocs()
			for b.Loop() {
				hs, err := NewHistoryScanner(gitDir)
				if err != nil {
					b.Fatalf("open scanner: %v", err)
				}

				var count atomic.Int64
				var firstHunk atomic.Int64
				firstHunk.Store(-1)
				start := time.Now()
				err = hs.DiffHistoryHunksFunc(func(HunkAddition) error {
					if count.Add(1) == 1 {
						firstHunk.Store(int64(time.Since(start)))
					}
					return nil
				})
				if err != nil {
					b.Fatalf("scan: %v", err)
				}
				// fileCount adds in the root commit plus one modification.
				if got := count.Load(); got < int64(tc.fileCount) {
					b.Fatalf("saw %d hunks, want at least %d", got, tc.fileCount)
				}
				if first := firstHunk.Load(); first >= 0 {
					firstHunkTotal += first
					iters++
				}
				if err := hs.Close(); err != nil {
					b.Fatalf("close scanner: %v", err)
				}
			}
			if iters > 0 {
				b.ReportMetric(float64(firstHunkTotal)/float64(iters), "first-hunk-ns")
			}
		})
	}
}

// BenchmarkDiffHistoryHunksColdNonRootAddHeavy adjudicates the full-pipeline
// time, allocation, and target-commit latency tradeoff between the bounded
// single-walk path (4096 additions) and replay (16384 additions). Each timed
// iteration uses a fresh scanner so pair and tree caches cannot carry over.
//
// The fixture's seed root also emits a hunk. "bulk-first-hunk-ns" therefore
// waits specifically for the first concurrent callback attributed to the bulk
// child commit instead of measuring whichever commit happens to emit first.
func BenchmarkDiffHistoryHunksColdNonRootAddHeavy(b *testing.B) {
	fx := newHunkBenchFixtures(b.TempDir())

	for _, fileCount := range []int{4096, 16384} {
		b.Run(fmt.Sprintf("%dfiles_64B", fileCount), func(b *testing.B) {
			gitDir := nonRootAddHeavyRepo(b, fx, fileCount, 64)

			setupScanner, err := NewHistoryScanner(gitDir)
			if err != nil {
				b.Fatalf("open setup scanner: %v", err)
			}
			bulkCommit, _ := nonRootCommitOf(b, setupScanner)
			if err := setupScanner.Close(); err != nil {
				b.Fatalf("close setup scanner: %v", err)
			}
			bulkOID := bulkCommit.OID

			var bulkFirstHunkTotal, iters int64
			b.ReportAllocs()
			for b.Loop() {
				hs, err := NewHistoryScanner(gitDir)
				if err != nil {
					b.Fatalf("open scanner: %v", err)
				}

				var count atomic.Int64
				var bulkSeen atomic.Bool
				var bulkFirstHunk atomic.Int64
				start := time.Now()
				scanErr := hs.DiffHistoryHunksFunc(func(h HunkAddition) error {
					count.Add(1)
					if h.Commit() == bulkOID &&
						!bulkSeen.Load() && bulkSeen.CompareAndSwap(false, true) {
						bulkFirstHunk.Store(int64(time.Since(start)))
					}
					return nil
				})
				closeErr := hs.Close()
				if scanErr != nil {
					b.Fatalf("scan: %v", scanErr)
				}
				if closeErr != nil {
					b.Fatalf("close scanner: %v", closeErr)
				}

				want := int64(fileCount + 1) // seed root plus one hunk per bulk file.
				if got := count.Load(); got != want {
					b.Fatalf("saw %d hunks, want %d", got, want)
				}
				if !bulkSeen.Load() {
					b.Fatalf("saw no hunk from bulk commit %s", bulkOID)
				}
				bulkFirstHunkTotal += bulkFirstHunk.Load()
				iters++
			}
			if iters > 0 {
				b.ReportMetric(float64(bulkFirstHunkTotal)/float64(iters), "bulk-first-hunk-ns")
			}
		})
	}
}
