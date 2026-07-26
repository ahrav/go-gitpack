// emit_blob_pairs_bench_test.go
//
// Measurement substrate for stage-1 fan-out (emitCommitBlobPairs) on
// add-heavy commits.
//
// The rename-suppression change buffers every addition until the commit's
// tree walk completes so it can match additions against same-commit
// deletions by OID. Two costs follow, and both concentrate in the root
// commit of a scan (where every file is an addition and no deletion can
// exist):
//
//   - stage-2 workers receive no work until the whole walk finishes, and
//   - the buffered adds slice holds one blobPairWork per added file.
//
// These benchmarks make both costs visible so the buffering change and any
// later fast-path can be compared like for like:
//
//   - BenchmarkEmitCommitBlobPairs/RootCommit_* runs stage 1 alone against a
//     drained channel and reports ns/op plus B/op (the buffering shows up as
//     allocations) and "first-work-ns" (how long stage 2 waits for its first
//     unit of work).
//   - BenchmarkDiffHistoryHunksColdRootHeavy runs the full pipeline on a
//     fresh scanner per iteration and reports ns/op plus "first-hunk-ns".
//
// The fixture is deliberately add-only: a single root commit adding many
// small distinct files, plus one trailing modification commit so histories
// with more than one commit stay representative. File contents are unique
// per path so no two adds share an OID and the pair memo cannot collapse
// stage-2 work.
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
		for i := range fileCount {
			dir := filepath.Join(work, fmt.Sprintf("d%02d", i%64), fmt.Sprintf("e%02d", (i/64)%64))
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

// rootCommitOf returns the root commit of the fixture plus its (zero)
// first-parent tree, so the unit benchmark can call emitCommitBlobPairs with
// exactly the arguments the pipeline would use.
func rootCommitOf(tb testing.TB, hs *HistoryScanner) (commitInfo, Hash) {
	tb.Helper()
	commits, err := hs.loadAllCommits()
	if err != nil {
		tb.Fatalf("load commits: %v", err)
	}
	for _, c := range commits {
		if len(c.ParentOIDs) == 0 {
			parentTree, err := hs.firstParentTree(c)
			if err != nil {
				tb.Fatalf("first-parent tree: %v", err)
			}
			return c, parentTree
		}
	}
	tb.Fatal("fixture has no root commit")
	return commitInfo{}, Hash{}
}

// BenchmarkEmitCommitBlobPairs measures stage 1 alone on a root commit.
//
// A drain goroutine consumes the work channel and records the arrival time
// of the first unit of work per iteration; the mean is reported as
// "first-work-ns". Under streaming emission this is the cost of walking to
// the first blob; under buffered emission it includes the entire tree walk.
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
			stopCh := make(chan struct{})

			var firstWorkTotal, iters int64
			b.ReportAllocs()
			for b.Loop() {
				blobs := make(chan blobPairWork, 4096)
				done := make(chan int64, 1)
				start := time.Now()
				go func() {
					var first int64 = -1
					n := 0
					for range blobs {
						if n == 0 {
							first = int64(time.Since(start))
						}
						n++
					}
					if n != fileCount {
						b.Errorf("emitted %d pairs, want %d", n, fileCount)
					}
					done <- first
				}()
				if err := hs.emitCommitBlobPairs(root, parentTree, blobs, stopCh); err != nil {
					b.Fatalf("emitCommitBlobPairs: %v", err)
				}
				close(blobs)
				if first := <-done; first >= 0 {
					firstWorkTotal += first
					iters++
				}
			}
			if iters > 0 {
				b.ReportMetric(float64(firstWorkTotal)/float64(iters), "first-work-ns")
			}
		})
	}
}

// BenchmarkDiffHistoryHunksColdRootHeavy measures the full two-stage
// pipeline end to end on the root-heavy fixture with a fresh scanner per
// iteration, so no pair memo or tree cache carries over. "first-hunk-ns" is
// the time from scan start until any stage-2 worker delivers the first
// hunk — the end-to-end view of the stage-2 stall.
//
// The matrix crosses file count with per-file size because stage-2 cost per
// work unit decides which stage-1 emission strategy wins: tiny files make
// stage 2 outrun the walk regardless of emission order, while larger files
// make stage-2 idle time during a buffered walk unrecoverable.
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
