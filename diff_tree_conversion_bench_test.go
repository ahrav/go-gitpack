// diff_tree_conversion_bench_test.go
//
// Measurement substrate for the object reads an entry-type transition adds.
//
// A transition is reported per side, so walkDiff recurses into the new subtree
// of a file→directory conversion and into the old subtree of a
// directory→file conversion. Both are tree reads no other shape performs, and
// the extra additions they surface become extra stage-1 work units.
//
// The fixture's child commit converts every path it touches, which is the
// worst case rather than a realistic mix: real histories convert a handful of
// paths at most. Two questions therefore need different benchmarks, and only
// one of them lives here:
//
//   - Cost on a conversion-heavy commit: the benchmarks below, at two levels.
//     BenchmarkWalkDiffConversions counts callbacks and nothing else, so it is
//     the pure walk cost. BenchmarkEmitCommitBlobPairsConversions runs stage 1
//     against a drained channel, adding suppression bookkeeping over the
//     larger candidate set the conversions produce.
//   - Regression on ordinary histories with no conversions: the conversion-free
//     benchmarks already in this package -- BenchmarkWalkDiff_SmallTree and
//     _LargeTree here, BenchmarkEmitCommitBlobPairs in
//     emit_blob_pairs_bench_test.go, BenchmarkDiffHistoryHunksStage in
//     hunk_rename_bench_test.go. Nothing is duplicated for them.
//
// Every generated body is unique to its path, so no two entries share a blob
// OID and exact-OID suppression cannot silence a candidate. That keeps the
// oracle counts a function of the fixture's shape alone.

package objstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// conversionKind names the in-place type transition the child commit of
// conversionRepo applies to one path.
type conversionKind uint8

const (
	convertFileToDir conversionKind = iota
	convertDirToFile
	convertFileToSymlink
	conversionKindCount
)

// conversionKindOf spreads the transitions evenly over the converted paths, so
// one fixture exercises all three and the mix does not depend on the path
// count.
func conversionKindOf(index int) conversionKind {
	return conversionKind(index % int(conversionKindCount))
}

// conversionName names the index'th converted path. Fixed-width numbering keeps
// Git tree order equal to index order, and no name is a prefix of another, so
// tree ordering does not shift with the path count.
func conversionName(index int) string { return fmt.Sprintf("c%05d", index) }

// conversionRepo builds a two-commit repository whose child commit converts the
// entry type of every one of paths root-level names, mixing file→directory,
// directory→file, and file→symlink. Each converted directory holds filesPerDir
// files, which is what gives the recursion something to enumerate.
func conversionRepo(tb testing.TB, fx *hunkBenchFixtures, paths, filesPerDir int) string {
	tb.Helper()
	name := fmt.Sprintf("conversion-%dpaths-%dfiles", paths, filesPerDir)
	return fx.repo(tb, name, func(tb testing.TB, work string) {
		for i := range paths {
			path := filepath.Join(work, conversionName(i))
			switch conversionKindOf(i) {
			case convertDirToFile:
				writeConversionDir(tb, path, i, filesPerDir)
			default:
				writeConversionFile(tb, path, fmt.Sprintf("root %05d", i))
			}
		}
		gitCommitAllTB(tb, work, 0)

		for i := range paths {
			path := filepath.Join(work, conversionName(i))
			if err := os.RemoveAll(path); err != nil {
				tb.Fatalf("remove %s: %v", path, err)
			}
			switch conversionKindOf(i) {
			case convertFileToDir:
				writeConversionDir(tb, path, i, filesPerDir)
			case convertDirToFile:
				writeConversionFile(tb, path, fmt.Sprintf("converted %05d", i))
			case convertFileToSymlink:
				// The target is a path string no generated file body can
				// equal, so the symlink's blob OID is its own.
				if err := os.Symlink("target/"+conversionName(i), path); err != nil {
					tb.Fatalf("symlink %s: %v", path, err)
				}
			}
		}
		gitCommitAllTB(tb, work, 1)
	})
}

// writeConversionFile writes a regular file whose body is unique to tag.
func writeConversionFile(tb testing.TB, path, tag string) {
	tb.Helper()
	body := fmt.Sprintf("%s\n%s\n", tag, textFiller)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		tb.Fatalf("write %s: %v", path, err)
	}
}

// writeConversionDir writes a directory of filesPerDir files, each with a body
// unique to its (index, position) pair.
func writeConversionDir(tb testing.TB, path string, index, filesPerDir int) {
	tb.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		tb.Fatalf("mkdir %s: %v", path, err)
	}
	for j := range filesPerDir {
		inner := filepath.Join(path, fmt.Sprintf("inner%02d.txt", j))
		writeConversionFile(tb, inner, fmt.Sprintf("inner %05d %02d", index, j))
	}
}

// conversionCounts derives what a conversionRepo child commit must produce,
// from the same kind assignment the fixture uses:
//
//   - events is the number of walkDiff callbacks. Each conversion is one
//     deletion plus one addition per side, and a converted directory
//     contributes one event per file it gains or loses.
//   - works is the number of stage-1 work units, i.e. the blob-mode additions.
//     Deletions carry no added side, and unique bodies mean nothing is
//     suppressed.
//
// Deriving both keeps the oracle tied to the fixture instead of to a number
// that a change in reporting shape would silently invalidate.
func conversionCounts(paths, filesPerDir int) (events, works int) {
	for i := range paths {
		switch conversionKindOf(i) {
		case convertFileToDir:
			events += 1 + filesPerDir
			works += filesPerDir
		case convertDirToFile:
			events += filesPerDir + 1
			works++
		case convertFileToSymlink:
			events += 2
			works++
		}
	}
	return events, works
}

// conversionBenchPaths are the converted-path counts every benchmark here runs.
// 64 is a large but conceivable conversion commit; 256 exaggerates it so the
// per-conversion cost dominates fixed per-commit work.
var conversionBenchPaths = []int{64, 256}

// filesPerConvertedDir is small on purpose: the point is that a conversion
// recurses at all, not how wide the subtree is.
const filesPerConvertedDir = 4

// BenchmarkWalkDiffConversions measures the tree walk alone over a commit whose
// every changed path changes entry type. The callback only counts, so the
// number is tree reads, iteration, and path joining with no consumer work
// behind it.
func BenchmarkWalkDiffConversions(b *testing.B) {
	fx := newHunkBenchFixtures(b.TempDir())

	for _, paths := range conversionBenchPaths {
		b.Run(fmt.Sprintf("%dpaths", paths), func(b *testing.B) {
			gitDir := conversionRepo(b, fx, paths, filesPerConvertedDir)
			hs, err := NewHistoryScanner(gitDir)
			if err != nil {
				b.Fatalf("open scanner: %v", err)
			}
			defer hs.Close()

			child, parentTree := nonRootCommitOf(b, hs)
			wantEvents, _ := conversionCounts(paths, filesPerConvertedDir)

			b.ReportAllocs()
			for b.Loop() {
				events := 0
				err := walkDiff(hs.store, parentTree, child.TreeOID, "",
					func(string, Hash, Hash, uint32) error {
						events++
						return nil
					})
				if err != nil {
					b.Fatalf("walkDiff: %v", err)
				}
				if events != wantEvents {
					b.Fatalf("walk emitted %d events, want %d", events, wantEvents)
				}
			}

			// One op is one whole walk, so this is the per-op work normalizer:
			// a time delta is comparable only while it holds still.
			b.ReportMetric(float64(wantEvents), "events/op")
		})
	}
}

// BenchmarkEmitCommitBlobPairsConversions measures stage 1 over the same
// commit, which adds the deletion bookkeeping and candidate retention that the
// conversions' extra additions feed. It reuses the harness
// BenchmarkEmitCommitBlobPairs uses, so the two are read side by side.
func BenchmarkEmitCommitBlobPairsConversions(b *testing.B) {
	fx := newHunkBenchFixtures(b.TempDir())

	for _, paths := range conversionBenchPaths {
		b.Run(fmt.Sprintf("%dpaths", paths), func(b *testing.B) {
			gitDir := conversionRepo(b, fx, paths, filesPerConvertedDir)
			hs, err := NewHistoryScanner(gitDir)
			if err != nil {
				b.Fatalf("open scanner: %v", err)
			}
			defer hs.Close()

			child, parentTree := nonRootCommitOf(b, hs)
			_, wantWorks := conversionCounts(paths, filesPerConvertedDir)
			runEmitCommitBlobPairsBench(b, hs, child, parentTree, wantWorks)
			b.ReportMetric(float64(wantWorks), "works/op")
		})
	}
}
