// hunk_rename_bench_test.go
//
// Measurement substrate for the commit-walk stage of the hunk pipeline
// (HistoryScanner.emitCommitBlobPairs), which decides which of a commit's
// changed blobs reach the stage-2 hunk workers at all.
//
// Three pair classes leave a tree diff, and they cost different amounts:
//
//   - A modification (both OIDs non-zero and unequal) has an added side that
//     only a real diff can produce, so it must reach stage 2.
//   - A pure deletion (new OID zero) has no added side, so the most it can
//     ever produce is an empty hunk set.
//   - A pure addition (old OID zero) whose new OID also appears as a deletion
//     in the same commit is an exact-OID rename: content addressing makes the
//     bytes under the new path identical to the bytes the old path already
//     reported.
//
// No repository under testdata/repos contains a rename or a deletion --
// generate_testdata.sh only ever adds files -- so the deletion and rename
// classes are unreachable from every other benchmark in this package, and the
// cost of handling them is measured by none of them. The fixtures below supply
// both, plus a modification-only control.
//
// Memo state brackets the answer instead of picking one number. A rename's
// added side carries the same (zero, newOID) pair key as the original addition
// of that content, so a populated diff memo serves it and only the delivery
// remains, while an empty memo pays the full tokenize-and-diff -- reading its
// blobs back from the store's object caches, which stay warm in both states
// (see runHunkStageBench, which explains why and what that excludes). NoMemo is
// therefore the upper bound on what stage-1 filtering can save and Memo the
// lower bound; a real repository sits between them.
//
// hunks/op and linebytes/op are reported next to time because the three classes
// differ in how many hunks they emit. Two runs are a like-for-like speed
// comparison only while hunks/op is unchanged; when it moves, the time delta is
// work avoided by emitting less, and the two numbers must be read together.
// For that reason the oracle here is stability plus non-emptiness rather than a
// hardcoded count: the count is exactly what a change to the filtering rules is
// allowed to move.

package objstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// renameTree builds a repository whose root commit adds files text files under
// src/ and whose second commit moves that directory to dst/.
//
// git mv rewrites tree entries and touches no content, so every path in the
// second commit's diff is a deletion under src/ paired with an addition under
// dst/ carrying the identical blob OID. That is one exact-OID rename per file,
// and the only shape that reaches the rename branch of emitCommitBlobPairs.
func (f *hunkBenchFixtures) renameTree(tb testing.TB, files, fileSize int) string {
	tb.Helper()
	name := fmt.Sprintf("rename-%dfiles-%dB", files, fileSize)
	return f.repo(tb, name, func(tb testing.TB, work string) {
		writeTreeFiles(tb, work, "src", files, fileSize)
		gitCommitAllTB(tb, work, 0)
		gitTB(tb, work, "mv", "src", "dst")
		gitCommitAllTB(tb, work, 1)
	})
}

// deleteTree builds a repository whose root commit adds files text files under
// src/ and whose second commit removes the directory.
//
// Every path in the second commit's diff is a pure deletion, the class whose
// added side is empty by construction.
func (f *hunkBenchFixtures) deleteTree(tb testing.TB, files, fileSize int) string {
	tb.Helper()
	name := fmt.Sprintf("delete-%dfiles-%dB", files, fileSize)
	return f.repo(tb, name, func(tb testing.TB, work string) {
		writeTreeFiles(tb, work, "src", files, fileSize)
		gitCommitAllTB(tb, work, 0)
		gitTB(tb, work, "rm", "-r", "-q", "--", "src")
		gitCommitAllTB(tb, work, 1)
	})
}

// modifyTree builds a repository whose root commit adds files text files under
// src/ and whose later revisions each rewrite one line in a fixed subset.
//
// Nothing is renamed, deleted, or chmod'd, so the set of emitted hunks is
// identical under any filtering rule that only reasons about additions,
// deletions, and equal-OID pairs. This is the control whose hunks/op must not
// move, which makes it the one fixture here whose time delta is a pure
// like-for-like comparison.
func (f *hunkBenchFixtures) modifyTree(tb testing.TB, files, fileSize, revisions int) string {
	tb.Helper()
	name := fmt.Sprintf("modify-%dfiles-%dB-%drev", files, fileSize, revisions)
	return f.repo(tb, name, func(tb testing.TB, work string) {
		writeTreeFiles(tb, work, "src", files, fileSize)
		gitCommitAllTB(tb, work, 0)

		// Touch a fixed slice of the tree per revision rather than the whole
		// tree: a commit that rewrites every file would make the modification
		// class dwarf everything else and hide the per-commit walk cost this
		// fixture exists to hold constant.
		const touched = 64
		for rev := 1; rev < revisions; rev++ {
			for i := 0; i < touched && i < files; i++ {
				index := (rev*touched + i) % files
				body := treeFileBody(index, fileSize)
				body = append(body, []byte(fmt.Sprintf("rev %d edit %s\n", rev, textFiller))...)
				path := filepath.Join(work, "src", treeFileName(index))
				if err := os.WriteFile(path, body, 0o644); err != nil {
					tb.Fatalf("write %s: %v", path, err)
				}
			}
			gitCommitAllTB(tb, work, rev)
		}
	})
}

// writeTreeFiles creates dir under work and fills it with files text files of
// at least fileSize bytes each.
func writeTreeFiles(tb testing.TB, work, dir string, files, fileSize int) {
	tb.Helper()
	full := filepath.Join(work, dir)
	if err := os.MkdirAll(full, 0o755); err != nil {
		tb.Fatalf("create %s: %v", full, err)
	}
	for i := range files {
		path := filepath.Join(full, treeFileName(i))
		if err := os.WriteFile(path, treeFileBody(i, fileSize), 0o644); err != nil {
			tb.Fatalf("write %s: %v", path, err)
		}
	}
}

// treeFileName names the index'th generated file. Fixed-width numbering keeps
// Git tree order equal to index order, so a fixture's pack layout does not
// depend on how many files it holds.
func treeFileName(index int) string { return fmt.Sprintf("f%06d.txt", index) }

// treeFileBody returns deterministic text unique to index, at least size bytes
// long and free of NUL bytes so isBinary reports false.
//
// Uniqueness across index is load-bearing: two files with identical content
// share one blob OID, which would turn a directory move into a commit that
// deletes one OID and adds it back under several paths -- duplicate-content
// additions rather than one rename per file -- and change the very thing the
// rename fixture is built to present.
func treeFileBody(index, size int) []byte {
	var b strings.Builder
	for i := 0; b.Len() < size; i++ {
		fmt.Fprintf(&b, "file %06d line %06d %s\n", index, i, textFiller)
	}
	return []byte(b.String())
}

// gitCommitAllTB stages every change in the work tree, renames and deletions
// included, and commits it with a timestamp derived from rev.
//
// gitCommitTB stages one named path, which cannot express a directory move or
// a removal. Pinning the date matches that helper and keeps commit OIDs, and
// therefore pack layout, identical across runs and machines.
func gitCommitAllTB(tb testing.TB, dir string, rev int) {
	tb.Helper()
	gitTB(tb, dir, "add", "-A")
	date := fmt.Sprintf("%d +0000", 1700000000+int64(rev)*60)
	gitEnvTB(tb, dir,
		[]string{"GIT_AUTHOR_DATE=" + date, "GIT_COMMITTER_DATE=" + date},
		"commit", "--quiet", "-m", fmt.Sprintf("rev %d", rev))
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// BenchmarkDiffHistoryHunksStage runs a full hunk scan over one fixture per
// pair class, so the cost of the commit-walk stage's filtering is attributable
// to the class it filters.
//
// DiffHistoryHunksFunc is the consumer entry point rather than the channel API
// because it removes the single-consumer hand-off, leaving the pipeline itself
// as the dominant term.
func BenchmarkDiffHistoryHunksStage(b *testing.B) {
	// Fixtures live under the parent's temp dir, so they are built once per
	// invocation and removed when this function returns. A parent benchmark
	// that only calls Run is not itself measured, so fixture construction
	// never lands in a reported number.
	fx := newHunkBenchFixtures(b.TempDir())

	const (
		files    = 1000
		fileSize = 2 << 10
	)

	b.Run("RenameTree", func(b *testing.B) {
		runHunkStageBench(b, fx.renameTree(b, files, fileSize))
	})
	b.Run("DeleteTree", func(b *testing.B) {
		runHunkStageBench(b, fx.deleteTree(b, files, fileSize))
	})
	b.Run("ModifyTree", func(b *testing.B) {
		runHunkStageBench(b, fx.modifyTree(b, files, fileSize, 8))
	})
}

// runHunkStageBench scans gitDir under both memo states.
//
// Exactly one thing varies: the pair-cache budget. The offset cache, the ARC
// and the delta window all keep their defaults in both arms, so the delta is
// the diff memo and nothing else. Zeroing the offset cache alongside it buys no
// coldness on this fixture -- the ARC and the delta window sit ahead of
// inflation and already serve these blobs back (see below) -- while its own
// bookkeeping moves the number in its own right, so a second varying budget
// would leave the state names describing something other than what differs
// between the arms.
//
// What no state varies is the store's OID-keyed object caches. The ARC (16K
// entries) and the delta window (32 MiB over 64 shards) have no budget option,
// both dwarf a 1000x2 KiB fixture, and both sit ahead of inflation in store.get.
// The untimed priming scan below fills them, so NEITHER state re-inflates:
// purging just the ARC between iterations makes the identical NoMemo scan ~37%
// slower, which is only possible if it was reading through that cache inside the
// timed region.
//
// That is deliberate, not an oversight. Holding inflation constant across both
// arms is what makes the delta attributable to the memo, and it is the reason a
// single scanner is reused: a fresh HistoryScanner per iteration would pull
// index mapping into the timed region (~5x the allocations here) and swamp the
// signal this benchmark exists to isolate. For a genuinely cold end-to-end scan,
// including index mapping and real inflation, see
// BenchmarkDiffHistoryHunksColdRootHeavy and
// BenchmarkDiffHistoryHunksColdNonRootAddHeavy in emit_blob_pairs_bench_test.go,
// which do build a fresh scanner per iteration. Do not read a number from here
// as a cold-cache cost.
func runHunkStageBench(b *testing.B, gitDir string) {
	states := []struct {
		name       string
		pairBudget int
	}{
		{name: "NoMemo", pairBudget: 0},
		{name: "Memo", pairBudget: defaultPairCacheBudget},
	}

	for _, st := range states {
		b.Run(st.name, func(b *testing.B) {
			scanner, err := NewHistoryScanner(gitDir,
				WithScanMode(ScanModeHunks),
				WithPairCacheBudget(st.pairBudget))
			if err != nil {
				b.Fatalf("NewHistoryScanner(%s): %v", gitDir, err)
			}
			defer func() {
				if err := scanner.Close(); err != nil {
					b.Errorf("Close: %v", err)
				}
			}()

			// One untimed scan settles the commit, tree and metadata caches so
			// both states measure a steady state, and populates the diff memo
			// when a budget was given. Setup before b.Loop is not timed.
			wantHunks, wantBytes := scanCountHunks(b, scanner)
			if wantHunks == 0 {
				b.Fatalf("scan of %s delivered no hunks", gitDir)
			}

			b.SetBytes(wantBytes)
			b.ReportAllocs()
			for b.Loop() {
				gotHunks, gotBytes := scanCountHunks(b, scanner)
				if gotHunks != wantHunks || gotBytes != wantBytes {
					b.Fatalf("scan delivered %d hunks / %d line bytes, want %d / %d",
						gotHunks, gotBytes, wantHunks, wantBytes)
				}
			}

			// One op is one whole scan, so the per-scan totals are already the
			// per-op figures. They are the work normalizer: a time delta is
			// only comparable while these hold still.
			b.ReportMetric(float64(wantHunks), "hunks/op")
			b.ReportMetric(float64(wantBytes), "linebytes/op")
		})
	}
}

// scanCountHunks drains one full scan and reports the hunks delivered and the
// total length of their lines.
//
// DiffHistoryHunksFunc calls fn from every worker concurrently, so the
// counters are atomic; line bytes are summed per hunk and added once to keep
// the contended updates proportional to hunks rather than to lines.
func scanCountHunks(b *testing.B, scanner *HistoryScanner) (hunks, lineBytes int64) {
	b.Helper()
	var (
		hunkCount atomic.Int64
		byteCount atomic.Int64
	)
	err := scanner.DiffHistoryHunksFunc(func(h HunkAddition) error {
		n := 0
		for _, line := range h.Lines() {
			n += len(line)
		}
		hunkCount.Add(1)
		byteCount.Add(int64(n))
		return nil
	})
	if err != nil {
		b.Fatalf("DiffHistoryHunksFunc: %v", err)
	}
	return hunkCount.Load(), byteCount.Load()
}

// ---------------------------------------------------------------------------
// Reachability proofs
// ---------------------------------------------------------------------------

// TestHunkStageBench_RenameFixtureProducesExactOIDRenames proves the rename
// fixture presents the shape the benchmark claims to measure: a second commit
// whose diff is one deletion and one addition per file, pairwise sharing a blob
// OID.
//
// Without this, a fixture whose files collided on content, or a Git that
// materialized the move as edits, would be benchmarked anyway and the numbers
// would describe a different workload.
func TestHunkStageBench_RenameFixtureProducesExactOIDRenames(t *testing.T) {
	requireGit(t)

	const (
		files    = 16
		fileSize = 512
	)
	fx := newHunkBenchFixtures(t.TempDir())
	gitDir := fx.renameTree(t, files, fileSize)

	scanner, err := NewHistoryScanner(gitDir, WithScanMode(ScanModeHunks))
	if err != nil {
		t.Fatalf("NewHistoryScanner: %v", err)
	}
	defer func() {
		if err := scanner.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	commits, err := scanner.loadAllCommits()
	if err != nil {
		t.Fatalf("loadAllCommits: %v", err)
	}
	if len(commits) != 2 {
		t.Fatalf("fixture has %d commits, want 2", len(commits))
	}

	// The move commit is the one with a parent; the walk's order is not part of
	// any contract, so select by parent count rather than by position.
	var move commitInfo
	for _, c := range commits {
		if len(c.ParentOIDs) == 1 {
			move = c
		}
	}
	if move.TreeOID.IsZero() {
		t.Fatal("fixture has no single-parent commit")
	}
	parentTree, err := scanner.firstParentTree(move)
	if err != nil {
		t.Fatalf("firstParentTree: %v", err)
	}

	deletes := make(map[Hash]int, files)
	var adds []Hash
	err = walkDiff(scanner.store, parentTree, move.TreeOID, "",
		func(path string, old, newH Hash, mode uint32) error {
			if !isBlobMode(mode) {
				return nil
			}
			switch {
			case newH.IsZero():
				deletes[old]++
			case old.IsZero():
				adds = append(adds, newH)
			default:
				t.Errorf("%s changed in place (old %s, new %s); the move should be pure adds and deletes",
					path, old, newH)
			}
			return nil
		})
	if err != nil {
		t.Fatalf("walkDiff: %v", err)
	}

	if len(deletes) != files {
		t.Errorf("move commit deleted %d distinct OIDs, want %d (files sharing content?)", len(deletes), files)
	}
	if len(adds) != files {
		t.Errorf("move commit added %d paths, want %d", len(adds), files)
	}
	matched := 0
	for _, oid := range adds {
		if deletes[oid] > 0 {
			deletes[oid]--
			matched++
		}
	}
	if matched != files {
		t.Errorf("%d of %d additions matched a deleted OID, want all of them", matched, files)
	}
}

// TestHunkStageBench_ControlFixtureHasNoRenamesOrDeletions proves the
// modification-only fixture is the control it claims to be: if any commit in it
// deleted or added a path after the root, its hunk count would depend on the
// filtering rules and it could no longer serve as the fixed-semantics leg of a
// comparison.
func TestHunkStageBench_ControlFixtureHasNoRenamesOrDeletions(t *testing.T) {
	requireGit(t)

	const (
		files     = 8
		fileSize  = 512
		revisions = 4
	)
	fx := newHunkBenchFixtures(t.TempDir())
	gitDir := fx.modifyTree(t, files, fileSize, revisions)

	scanner, err := NewHistoryScanner(gitDir, WithScanMode(ScanModeHunks))
	if err != nil {
		t.Fatalf("NewHistoryScanner: %v", err)
	}
	defer func() {
		if err := scanner.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	commits, err := scanner.loadAllCommits()
	if err != nil {
		t.Fatalf("loadAllCommits: %v", err)
	}
	if len(commits) != revisions {
		t.Fatalf("fixture has %d commits, want %d", len(commits), revisions)
	}

	modifications := 0
	for _, c := range commits {
		if len(c.ParentOIDs) == 0 {
			continue // the root add is every file; only later commits are constrained.
		}
		parentTree, err := scanner.firstParentTree(c)
		if err != nil {
			t.Fatalf("firstParentTree: %v", err)
		}
		err = walkDiff(scanner.store, parentTree, c.TreeOID, "",
			func(path string, old, newH Hash, mode uint32) error {
				if !isBlobMode(mode) {
					return nil
				}
				switch {
				case old.IsZero():
					t.Errorf("commit %s added %s; the control fixture must only modify", c.OID, path)
				case newH.IsZero():
					t.Errorf("commit %s deleted %s; the control fixture must only modify", c.OID, path)
				case old == newH:
					t.Errorf("commit %s reported %s unchanged; the control fixture must only modify", c.OID, path)
				default:
					modifications++
				}
				return nil
			})
		if err != nil {
			t.Fatalf("walkDiff: %v", err)
		}
	}
	if modifications == 0 {
		t.Error("control fixture produced no modifications")
	}
}
