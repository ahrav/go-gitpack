// hunk_dedup_bench_test.go
//
// Measurement substrate for the WithHunkLineDedup pipeline (hunk_dedup.go).
//
// The dedup path had no benchmark coverage: no benchmark in the package
// enabled WithHunkLineDedup, and every checked-in fixture under
// testdata/repos adds exactly one distinct line per commit, so none of them
// can reach the suppression path at all (0% duplicate added lines). Both
// gaps are closed here.
//
// Two tiers, because they answer different questions:
//
//	dedupHunkEmission micro   what one hunk's verdict costs, by hunk size and
//	                          duplicate regime — isolates the decision
//	                          function from git, I/O, and the pipeline.
//	DiffHistoryHunksFunc E2E  whether that cost is visible end-to-end, as
//	                          dedup-off vs dedup-on over a fixture built to
//	                          have the duplicate density the feature targets.
package objstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// dedupBenchLines returns n deterministic, mutually distinct lines of exactly
// width bytes, tagged with tag so two regimes can be given disjoint line sets
// without collisions.
//
// Width is explicit and load-bearing, because lineFingerprint's cost is a step
// function of it: farm.Hash64 dispatches on input length, and on this package's
// measurements the 65-96 byte band costs about twice the <=64 byte band
// (~19.5 ns vs ~9.9 ns for a single hash). Real source lines are mostly short —
// this repository's own Go sources measure p50=24, p75=50, p90=73, with 84% at
// or under 64 bytes — so a benchmark built only from wide lines overstates
// hashing's share of the dedup decision and flatters any change that removes
// hash calls. Cover both bands.
func dedupBenchLines(tag string, n, width int) []string {
	lines := make([]string, n)
	for i := range lines {
		head := fmt.Sprintf("%s %09d ", tag, i)
		if len(head) > width {
			panic(fmt.Sprintf("width %d too small for tag %q", width, tag))
		}
		lines[i] = head + strings.Repeat("x", width-len(head))
	}
	return lines
}

// dedupBenchWidths spans farm.Hash64's two relevant length bands: a realistic
// short line, and one in the more expensive 65-96 byte band.
var dedupBenchWidths = []int{32, 78}

// dedupBenchSet returns a set pre-populated with every line in seen, sized
// large enough that it never saturates or grows during a benchmark: growth
// mid-measurement would put a rehash of the whole table inside the timed
// region and make the result depend on iteration count.
func dedupBenchSet(tb testing.TB, seen []string) *lineFingerprintSet {
	tb.Helper()
	set := newLineFingerprintSet(dedupInitialSlotsLog2)
	for _, line := range seen {
		set.markNew(lineFingerprint(line))
	}
	if set.saturated {
		tb.Fatalf("set saturated while priming %d lines; raise the table size", len(seen))
	}
	return set
}

// dedupBenchHunkSizes spans the hunk sizes that matter: 1 line (the shape
// every checked-in fixture produces), through the multi-hundred-line blocks
// that vendored files and license headers actually re-introduce.
var dedupBenchHunkSizes = []int{1, 8, 64, 512}

// BenchmarkDedupHunkEmission measures one dedup verdict in the all-duplicate
// regime: every line of the hunk was already seen, so the hunk is suppressed.
//
// This is both the worst case and the case the feature exists to produce. It
// is also the only regime that is idempotent across iterations — the set
// already contains every line, so no iteration inserts, grows, or saturates,
// and iteration N costs exactly what iteration 1 did. Regimes that insert
// would either drift or force a fresh 256 KiB table per iteration.
//
// The verdict hashes and probes each line once (markNew), so this is also
// the per-line floor of the decision: N hashes and N probes for N lines.
func BenchmarkDedupHunkEmission(b *testing.B) {
	for _, n := range dedupBenchHunkSizes {
		for _, w := range dedupBenchWidths {
			b.Run(fmt.Sprintf("AllDup/N=%d/W=%d", n, w), func(b *testing.B) {
				lines := dedupBenchLines("dup", n, w)
				set := dedupBenchSet(b, lines)
				h := HunkAddition{lines: lines}

				// Guard the regime: a true verdict here would mean some line was
				// unseen, so the benchmark would be timing an insert under an
				// "AllDup" label.
				if dedupHunkEmission(h, set) {
					b.Fatalf("N=%d: hunk reported new in the all-duplicate regime", n)
				}

				for b.Loop() {
					dedupHunkEmission(h, set)
				}
			})
		}
	}
}

// dupChurn builds a repository whose every revision adds two NEW files: a
// copy of a shared boilerplate block, and a one-line file unique to the
// revision.
//
// That shape is what drives dedup: revision 0 introduces the block, and every
// later revision re-adds the same block under a new path as one pure-addition
// hunk made entirely of seen lines, which whole-hunk dedup suppresses. The
// unique file keeps every revision's own hunk emitted, so the two counters
// the E2E benchmark reports separate suppressed from surviving work. It is
// the vendored-file / license-header / copied-config shape the feature
// targets, and it is precisely what the checked-in fixtures lack — they add
// one distinct line per commit and never repeat content.
//
// blockLines controls the hunk size, lineWidth the per-line hashing cost (see
// dedupBenchLines — farm.Hash64's cost steps at 64 bytes), and revisions how
// many duplicate hunks the scan sees.
func (f *hunkBenchFixtures) dupChurn(tb testing.TB, blockLines, lineWidth, revisions int) string {
	tb.Helper()
	name := fmt.Sprintf("dup-%dlines-%dw-%drev", blockLines, lineWidth, revisions)
	return f.repo(tb, name, func(tb testing.TB, work string) {
		block := strings.Join(dedupBenchLines("shared", blockLines, lineWidth), "\n") + "\n"
		for rev := range revisions {
			files := map[string]string{
				fmt.Sprintf("vendored_%04d.txt", rev): block,
				fmt.Sprintf("unique_%04d.txt", rev):   fmt.Sprintf("unique to rev %d\n", rev),
			}
			for file, body := range files {
				path := filepath.Join(work, file)
				if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
					tb.Fatalf("write %s: %v", path, err)
				}
			}
			gitCommitTB(tb, work, ".", rev)
		}
	})
}

// BenchmarkDiffHistoryHunksDedup measures a full hunk scan with dedup off and
// on over the same fixture, so the delta is the dedup pipeline's cost.
//
// Off vs on is not a pure overhead comparison — dedup suppresses hunks, so it
// also does strictly less consumer work. Both counters are reported so the
// two effects can be separated: hunks is what reached fn, and a large drop
// with a small time delta means the decision stage is eating the savings.
func BenchmarkDiffHistoryHunksDedup(b *testing.B) {
	fx := newHunkBenchFixtures(b.TempDir())

	cases := []struct{ blockLines, lineWidth, revisions int }{
		// Wide hunks at a realistic line width (near this repo's p75), so the
		// per-hunk decision cost dominates without inflating hashing: at 32
		// bytes farm.Hash64 takes its cheap <=64 byte path, which is what
		// most real source lines take.
		{blockLines: 512, lineWidth: 32, revisions: 64},
		// The same shape with lines in farm's more expensive 65-96 byte band.
		// Read against the case above, the pair bounds how much of any
		// dedup-side result is line-width artifact rather than pipeline cost.
		{blockLines: 512, lineWidth: 78, revisions: 64},
		// Narrow hunks over more commits, so pipeline overhead dominates.
		{blockLines: 8, lineWidth: 32, revisions: 512},
	}

	for _, c := range cases {
		gitDir := fx.dupChurn(b, c.blockLines, c.lineWidth, c.revisions)
		name := fmt.Sprintf("Block%d_W%d_Rev%d", c.blockLines, c.lineWidth, c.revisions)

		for _, dedup := range []bool{false, true} {
			label := "DedupOff"
			if dedup {
				label = "DedupOn"
			}
			b.Run(name+"/"+label, func(b *testing.B) {
				scanner, err := NewHistoryScanner(gitDir,
					WithScanMode(ScanModeHunks),
					WithHunkLineDedup(dedup))
				if err != nil {
					b.Fatalf("NewHistoryScanner(%s): %v", gitDir, err)
				}
				defer func() {
					if err := scanner.Close(); err != nil {
						b.Errorf("Close: %v", err)
					}
				}()

				// fn is invoked concurrently from multiple workers even in
				// dedup mode — only the dedup decisions are serialized — so
				// these counters must be atomic. Plain increments here race,
				// and the lost updates look exactly like the scan emitting a
				// different number of hunks each iteration.
				var hunks, lines atomic.Int64
				count := func(h HunkAddition) error {
					hunks.Add(1)
					lines.Add(int64(len(h.lines)))
					return nil
				}

				// One untimed priming scan settles the commit, tree, and blob
				// caches so the timed region measures a steady state rather
				// than first-touch index and inflate cost.
				if err := scanner.DiffHistoryHunksFunc(count); err != nil {
					b.Fatalf("priming scan: %v", err)
				}
				primed := hunks.Load()
				if primed == 0 {
					b.Fatalf("%s: priming scan produced no hunks", name)
				}
				primedLines := lines.Load()

				for b.Loop() {
					hunks.Store(0)
					lines.Store(0)
					if err := scanner.DiffHistoryHunksFunc(count); err != nil {
						b.Fatalf("scan: %v", err)
					}
				}

				// Dedup is documented as deterministic, so a repeat scan of an
				// immutable fixture must emit an identical hunk set. Drift
				// here would invalidate every number above, so it fails the
				// benchmark rather than being averaged into one.
				if got := hunks.Load(); got != primed {
					b.Fatalf("%s: hunk count drifted %d -> %d across iterations", name, primed, got)
				}
				if got := lines.Load(); got != primedLines {
					b.Fatalf("%s: line count drifted %d -> %d across iterations", name, primedLines, got)
				}
				// Guard the regime: dedup-off sees both hunks of every
				// revision; dedup-on suppresses every block copy after the
				// first, so the fixture measures the suppression path.
				want := int64(2 * c.revisions)
				if dedup {
					want = int64(c.revisions + 1)
				}
				if primed != want {
					b.Fatalf("%s: %d hunks reached fn, want %d", name, primed, want)
				}
				b.ReportMetric(float64(primed), "hunks")
				b.ReportMetric(float64(primedLines), "lines")
			})
		}
	}
}
