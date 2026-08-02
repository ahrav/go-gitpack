// hunk_scan_bench_test.go
//
// Measurement substrate for the payload-assembly step of ScanModeHunks
// (HistoryScanner.scanHunks).
//
// scanHunks turns every HunkAddition into an io.Reader before handing it to
// BlobScanner.ScanBlob, and the two hunk shapes cost very different amounts:
//
//   - A binary hunk carries exactly one "line" that is a zero-copy view over
//     the whole new blob (computeAddedHunks -> btostr), so the payload can be
//     handed over as a strings.Reader with no copy at all.
//   - A text hunk carries many lines that must be joined with '\n', which
//     means a bytes.Buffer whose backing array grows to the payload size.
//
// Neither shape was reachable from a benchmark before this file:
//
//   - The pre-existing hunk benchmarks (BenchmarkDiffHistoryHunks,
//     BenchmarkDiffHistoryHunksExternal) drain DiffHistoryHunks directly and
//     never call Scan, so scanHunks never executed under measurement.
//   - No repository under testdata/repos contains a blob with a NUL byte in
//     its first 8 KiB, so isBinary never returned true and no fixture could
//     produce a binary hunk at all.
//
// This file supplies both halves: fixtures that reach each shape, and
// benchmarks that execute scanHunks over them with assertions that fail when
// the shape being measured stops occurring. TestScanHunksBench_* below are the
// reachability proofs; they must pass for the benchmark numbers to mean
// anything.
//
// Consumer shape matters as much as payload shape, because it decides whether
// a string payload can be consumed without being converted back to bytes. Both
// shapes are measured; see the comment on countingSink for why the copy
// destination must not be io.Discard.

package objstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Consumers
// ---------------------------------------------------------------------------

// benchConsumer is the BlobScanner shape these benchmarks need: one that
// drains every payload and reports what it saw, so an iteration that stops
// delivering the payload under measurement fails instead of quietly reporting
// a better number.
type benchConsumer interface {
	BlobScanner

	// reset clears the accumulated totals so a single scan can be checked in
	// isolation.
	reset()

	// totals reports the bytes drained, the number of payloads delivered, and
	// the size of the largest single payload since the last reset.
	totals() (payloadBytes, payloads, maxPayload int64)
}

// payloadTotals holds the bookkeeping shared by both consumer shapes.
//
// scanHunks calls ScanBlob serially from its drain loop, but the BlobScanner
// contract permits concurrent calls, so the mutex also guards each consumer's
// reusable state. It is uncontended on the hunk path, and its cost is
// identical for every implementation being compared.
type payloadTotals struct {
	mu         sync.Mutex
	bytesRead  int64
	payloads   int64
	maxPayload int64
}

// record accounts for one delivered payload of n bytes. The caller must hold
// mu.
func (t *payloadTotals) record(n int64) {
	t.payloads++
	t.bytesRead += n
	if n > t.maxPayload {
		t.maxPayload = n
	}
}

func (t *payloadTotals) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.bytesRead, t.payloads, t.maxPayload = 0, 0, 0
}

func (t *payloadTotals) totals() (int64, int64, int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.bytesRead, t.payloads, t.maxPayload
}

// readLoopScratchSize is the fixed scratch size readLoopConsumer drains into.
//
// A fixed buffer is the point: io.ReadAll would grow a buffer to the payload
// size on every call, adding an N-byte allocation and copy to both sides of
// the comparison and swamping the payload-assembly difference being measured.
const readLoopScratchSize = 32 << 10

// readLoopConsumer drains each payload with repeated Read calls into one
// reused scratch buffer, so the consumer itself allocates nothing per hunk and
// whatever allocation the benchmark reports comes from payload assembly.
type readLoopConsumer struct {
	payloadTotals
	scratch []byte
}

// ScanBlob drains r into the reused scratch buffer.
func (c *readLoopConsumer) ScanBlob(r io.Reader, _ ScanMeta) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.scratch == nil {
		c.scratch = make([]byte, readLoopScratchSize)
	}
	var n int64
	for {
		// Read may return n > 0 together with io.EOF, so count before
		// inspecting the error.
		k, err := r.Read(c.scratch)
		n += int64(k)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	c.record(n)
	return nil
}

// countingSink is an io.Writer that implements Write and nothing else.
//
// That is load-bearing, not laziness. io.Copy(dst, src) prefers
// src.WriteTo(dst) when src implements io.WriterTo, and strings.Reader does.
// strings.Reader.WriteTo forwards to io.WriteString, which branches on the
// destination:
//
//	func WriteString(w Writer, s string) (n int, err error) {
//		if sw, ok := w.(StringWriter); ok {
//			return sw.WriteString(s)
//		}
//		return w.Write([]byte(s))
//	}
//
// io.Discard implements WriteString, so io.Copy(io.Discard, aStringsReader)
// never converts the string and would hide exactly the cost this benchmark
// exists to expose. A destination with only Write forces []byte(s) — one
// payload-sized allocation and copy — which is what a real consumer that
// wants bytes actually pays.
//
// Do not "simplify" this to io.Discard.
// TestScanHunksBench_SinkShapesAreLoadBearing fails if countingSink ever
// grows a WriteString or ReadFrom method, or if the stdlib fast paths this
// reasoning depends on ever change.
type countingSink struct{ n int64 }

func (s *countingSink) Write(p []byte) (int, error) {
	s.n += int64(len(p))
	return len(p), nil
}

// copyConsumer hands each payload to io.Copy with a destination that
// implements only Write, which is the consumer shape that reinserts a
// payload-sized allocation and copy when the payload arrives as a string.
type copyConsumer struct {
	payloadTotals
	sink countingSink
}

// ScanBlob copies r into the counting sink.
func (c *copyConsumer) ScanBlob(r io.Reader, _ ScanMeta) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sink.n = 0
	if _, err := io.Copy(&c.sink, r); err != nil {
		return err
	}
	c.record(c.sink.n)
	return nil
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// hunkBenchFixtures builds the Git repositories that reach each hunk shape and
// memoizes them by name, so a 16 MiB × N-revision repository is written once
// per process instead of once per sub-benchmark.
//
// Every repository lives under root, which callers supply from TempDir() so
// the whole set is removed when the owning test or benchmark returns.
// Sub-benchmarks run sequentially, so the map needs no lock.
type hunkBenchFixtures struct {
	root  string
	built map[string]string
}

func newHunkBenchFixtures(root string) *hunkBenchFixtures {
	return &hunkBenchFixtures{root: root, built: make(map[string]string)}
}

// repo returns the .git directory of the fixture named name, running build to
// create it on first request only.
//
// The repository is repacked with delta search disabled: the fixtures are
// deliberately incompressible, so a delta window would burn seconds looking
// for deltas that do not exist while changing nothing the benchmark measures.
func (f *hunkBenchFixtures) repo(tb testing.TB, name string, build func(tb testing.TB, work string)) string {
	tb.Helper()
	if gitDir, ok := f.built[name]; ok {
		return gitDir
	}
	requireGit(tb)

	work := filepath.Join(f.root, name)
	if err := os.MkdirAll(work, 0o755); err != nil {
		tb.Fatalf("create fixture dir %s: %v", work, err)
	}
	runGit(tb, work, "init", "--quiet")
	build(tb, work)
	runGit(tb, work, "repack", "-a", "-d", "--quiet", "--window=0", "--depth=0")

	gitDir := filepath.Join(work, ".git")
	f.built[name] = gitDir
	return gitDir
}

// binaryChurn builds a repository whose single file is a blobSize-byte binary
// blob rewritten once per revision.
//
// Every new-side blob starts with a NUL byte, so isBinary reports true inside
// its 8 KiB window and computeAddedHunks emits exactly one binary hunk per
// revision whose single line views the whole blob. That is the only shape
// reaching the binary branch of scanHunks, and each revision contributes one
// distinct (oldOID, newOID) pair, so a scan delivers exactly
// revisions × blobSize payload bytes.
func (f *hunkBenchFixtures) binaryChurn(tb testing.TB, blobSize, revisions int) string {
	tb.Helper()
	name := fmt.Sprintf("binary-%dB-%drev", blobSize, revisions)
	return f.repo(tb, name, func(tb testing.TB, work string) {
		const file = "blob.bin"
		path := filepath.Join(work, file)
		for rev := range revisions {
			if err := os.WriteFile(path, pseudoBinary(blobSize, uint64(rev)), 0o644); err != nil {
				tb.Fatalf("write %s: %v", path, err)
			}
			gitCommitTB(tb, work, file, rev)
		}
	})
}

// largeTextSmallDiff builds a repository whose single file is a text file of
// at least textSize bytes, where each revision after the first rewrites
// linesPerEdit lines in place.
//
// The file stays above SmallFileThreshold, so every revision after the first
// takes the large-file text diff and yields only a handful of added lines from
// a multi-megabyte comparison. Two properties matter here: the first revision
// is a pure addition, which produces one text hunk holding the entire file and
// therefore exercises the unchanged line-joining path at multi-megabyte scale;
// and the later revisions produce tiny hunks whose lines are zero-copy views
// into the whole new blob, which is the shape a pairCache retention question
// needs.
func (f *hunkBenchFixtures) largeTextSmallDiff(tb testing.TB, textSize, revisions, linesPerEdit int) string {
	tb.Helper()
	name := fmt.Sprintf("text-%dB-%drev", textSize, revisions)
	return f.repo(tb, name, func(tb testing.TB, work string) {
		const file = "data.txt"
		path := filepath.Join(work, file)
		lines := textLines(textSize)
		if len(lines) <= linesPerEdit {
			tb.Fatalf("textSize %d produced only %d lines, need more than %d", textSize, len(lines), linesPerEdit)
		}
		for rev := range revisions {
			if rev > 0 {
				// 997 is coprime with nothing in particular; it just moves the
				// edit to a different, deterministic place each revision.
				at := (rev * 997) % (len(lines) - linesPerEdit)
				for i := range linesPerEdit {
					lines[at+i] = fmt.Sprintf("rev %d edit %d %s", rev, i, textFiller)
				}
			}
			body := strings.Join(lines, "\n") + "\n"
			if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
				tb.Fatalf("write %s: %v", path, err)
			}
			gitCommitTB(tb, work, file, rev)
		}
	})
}

// textFiller pads generated lines to a realistic width.
const textFiller = "the quick brown fox jumps over the lazy dog 0123456789 abcdef"

// textLines returns deterministic, mutually distinct ASCII lines that total at
// least size bytes once joined with '\n'. No line contains a NUL byte, so
// isBinary reports false and the text path is taken.
func textLines(size int) []string {
	var lines []string
	for total := 0; total < size; {
		line := fmt.Sprintf("line %09d %s", len(lines), textFiller)
		lines = append(lines, line)
		total += len(line) + 1
	}
	return lines
}

// pseudoBinary returns size deterministic bytes whose first byte is NUL.
//
// The body is PCG output rather than a repeated pattern so the packed blob has
// a realistic (essentially incompressible) size and a realistic inflation
// cost. The leading NUL makes isBinary's verdict explicit instead of relying
// on a random byte happening to be zero, and determinism means two commits of
// this repository are benchmarked against byte-identical fixtures.
func pseudoBinary(size int, seed uint64) []byte {
	buf := make([]byte, size)
	r := rand.New(rand.NewPCG(0x9E3779B97F4A7C15, seed+1))
	var word [8]byte
	for i := 0; i < len(buf); i += 8 {
		binary.LittleEndian.PutUint64(word[:], r.Uint64())
		copy(buf[i:], word[:])
	}
	if len(buf) > 0 {
		buf[0] = 0
	}
	return buf
}

// gitCommitTB stages file and commits it with a timestamp derived from rev.
//
// Pinning the timestamps keeps commit OIDs, and therefore pack layout,
// identical across runs and machines.
func gitCommitTB(tb testing.TB, dir, file string, rev int) {
	tb.Helper()
	runGit(tb, dir, "add", "--", file)
	date := fmt.Sprintf("%d +0000", 1700000000+int64(rev)*60)
	runGitEnv(tb, dir,
		append(gitFixtureEnv(), "GIT_AUTHOR_DATE="+date, "GIT_COMMITTER_DATE="+date),
		"commit", "--quiet", "-m", fmt.Sprintf("rev %d", rev))
}

// ---------------------------------------------------------------------------
// Cache-state control
// ---------------------------------------------------------------------------

// warmPairCacheBudget returns a pairCache budget large enough to admit a
// single hunk charged at hunkBytes.
//
// pairCache.add refuses any entry whose charged size exceeds
// budgetPerShard/4, and budgetPerShard is budget/pairCacheShards, so admitting
// a hunk of B bytes needs a budget above 4*pairCacheShards*B. The extra factor
// of two covers the per-hunk and per-line header charge and leaves room for
// several revisions landing in one shard.
//
// The default budget is far below this: defaultPairCacheBudget gives each
// shard 4 MiB and therefore refuses any single hunk above 1 MiB, so a
// multi-megabyte binary hunk is never memoized by a default scanner. The Warm
// variant raises the budget for exactly that reason — see
// TestScanHunksBench_DefaultPairCacheRefusesLargeBinaryHunks.
func warmPairCacheBudget(hunkBytes int) int {
	// int64 math: on a 32-bit build 8*pairCacheShards*(16<<20) is exactly 2^32
	// and would wrap to 0, silently disabling the very cache this warms. Clamp
	// instead. A 32-bit build cannot express the budget a 16 MiB hunk needs
	// (4*pairCacheShards*hunkBytes already exceeds max int), so the warm
	// variant degrades to a refused entry there rather than to no cache at all.
	// No supported target is 32-bit; this keeps the helper honest if one is
	// added or a fixture grows.
	want := 8 * int64(pairCacheShards) * int64(hunkBytes)
	if want > math.MaxInt {
		return math.MaxInt
	}
	return int(want)
}

// pairCacheLen reports how many diff results the memo currently holds.
//
// The benchmarks use it to prove that Cold really is cold and Warm really is
// warm rather than assuming the budget had the intended effect.
func pairCacheLen(c *pairCache) int {
	n := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		n += len(s.m)
		s.mu.Unlock()
	}
	return n
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// hunkScanCase describes one fixture plus the invariants a scan of it must
// satisfy. The invariants are what make the benchmark falsifiable: a change
// that stops delivering the payload shape under measurement fails the
// benchmark instead of reporting a faster number.
type hunkScanCase struct {
	// gitDir is the .git directory to scan.
	gitDir string

	// wantPayloadBytes, when positive, is the exact number of payload bytes a
	// single scan must deliver.
	wantPayloadBytes int64

	// wantPayloads, when positive, is the exact number of payloads a single
	// scan must deliver.
	wantPayloads int64

	// wantMaxPayload, when positive, is the exact size of the largest single
	// payload. For a binary fixture this equals the blob size, which is what
	// proves scanHunks handed over the one-line binary payload whole rather
	// than joining lines.
	wantMaxPayload int64

	// minMaxPayload, when positive, is a lower bound on the largest single
	// payload, for fixtures whose exact payload size is not worth predicting.
	minMaxPayload int64

	// warmBudget is the pairCache budget used by the Warm variant.
	warmBudget int

	// wantWarmPairs, when positive, is the exact number of memoized pairs the
	// Warm variant must hold after its untimed priming scan. Zero means "any
	// non-zero count".
	wantWarmPairs int
}

// BenchmarkScanHunks measures HistoryScanner.Scan in ScanModeHunks, the only
// path that executes scanHunks, across the payload shapes and consumer shapes
// whose costs differ.
//
// The matrix is fixture × cache state × consumer:
//
//	fixture      BinaryChurn_1MiB, BinaryChurn_16MiB  — binary hunks
//	             LargeTextSmallDiff_8MiB, TextRepo    — text hunks (controls)
//	cache state  Cold (no memoization at all)
//	             Warm (diff memo serves every hunk; payload assembly isolated)
//	consumer     ReadLoop               — fixed scratch, no payload-sized copy
//	             CopyToNonStringWriter  — io.Copy into a Write-only sink
//
// The two text fixtures are controls: they must show no delta across a change
// that only touches the binary branch.
func BenchmarkScanHunks(b *testing.B) {
	// Fixtures live under the parent's temp dir, so they are built at most
	// once per process and removed when this function returns. A parent
	// benchmark that calls Run is itself run once with N=1 and is not
	// measured, so fixture construction never lands in a reported number.
	fx := newHunkBenchFixtures(b.TempDir())

	b.Run("BinaryChurn_1MiB", func(b *testing.B) {
		const (
			blobSize  = 1 << 20
			revisions = 8
		)
		runHunkScanBench(b, hunkScanCase{
			gitDir:           fx.binaryChurn(b, blobSize, revisions),
			wantPayloadBytes: int64(blobSize) * revisions,
			wantPayloads:     revisions,
			wantMaxPayload:   blobSize,
			warmBudget:       warmPairCacheBudget(blobSize),
			wantWarmPairs:    revisions,
		})
	})

	// Four revisions rather than eight: the fixture costs one git commit of
	// incompressible data per revision, and 64 MiB is already enough for the
	// per-payload cost to dominate everything else in the scan.
	b.Run("BinaryChurn_16MiB", func(b *testing.B) {
		const (
			blobSize  = 16 << 20
			revisions = 4
		)
		runHunkScanBench(b, hunkScanCase{
			gitDir:           fx.binaryChurn(b, blobSize, revisions),
			wantPayloadBytes: int64(blobSize) * revisions,
			wantPayloads:     revisions,
			wantMaxPayload:   blobSize,
			warmBudget:       warmPairCacheBudget(blobSize),
			wantWarmPairs:    revisions,
		})
	})

	// Text control with a large payload: the first revision is a pure
	// addition, so one hunk carries the whole 8 MiB file through the
	// line-joining path.
	b.Run("LargeTextSmallDiff_8MiB", func(b *testing.B) {
		const (
			textSize     = 8 << 20
			revisions    = 5
			linesPerEdit = 3
		)
		runHunkScanBench(b, hunkScanCase{
			gitDir:        fx.largeTextSmallDiff(b, textSize, revisions, linesPerEdit),
			wantPayloads:  revisions,
			minMaxPayload: textSize / 2,
			// The initial full-file hunk is charged its line bytes plus a
			// 16-byte header per line, so budget for twice the file size.
			warmBudget:    warmPairCacheBudget(2 * textSize),
			wantWarmPairs: revisions,
		})
	})

	// Text control over a checked-in fixture: many small hunks across 100
	// commits, i.e. the shape most existing callers actually see.
	b.Run("TextRepo_LargeRepo", func(b *testing.B) {
		runHunkScanBench(b, hunkScanCase{
			gitDir:     filepath.Join("testdata", "repos", "large-repo"),
			warmBudget: warmPairCacheBudget(1 << 20),
		})
	})
}

// runHunkScanBench runs one fixture through the cache-state × consumer matrix.
func runHunkScanBench(b *testing.B, c hunkScanCase) {
	cacheStates := []struct {
		name string
		// pairBudget and offsetBudget are the two memo budgets; zero disables
		// the corresponding cache.
		pairBudget   int
		offsetBudget int
	}{
		// Cold disables both memos, so every timed iteration re-runs
		// computeAddedHunks and re-inflates every blob. Disabling the memos is
		// preferred over constructing a fresh HistoryScanner per iteration:
		// that would put index mapping inside the timed region, and b.Loop
		// must not be mixed with manual StopTimer/StartTimer. The offset cache
		// has to go too — it holds materialized objects up to 4 MiB, so
		// leaving it on would silently serve the 1 MiB fixture's blobs from
		// memory and make "Cold" cold in name only.
		{name: "Cold", pairBudget: 0, offsetBudget: 0},

		// Warm lets the diff memo serve every hunk, so no blob is loaded and
		// no diff is recomputed inside the timed region. What remains is the
		// commit walk, the tree diffs, payload assembly, and the consumer —
		// which is what this measurement is for.
		{name: "Warm", pairBudget: c.warmBudget, offsetBudget: defaultOffsetCacheBudget},
	}
	consumers := []struct {
		name string
		make func() benchConsumer
	}{
		{name: "ReadLoop", make: func() benchConsumer { return new(readLoopConsumer) }},
		{name: "CopyToNonStringWriter", make: func() benchConsumer { return new(copyConsumer) }},
	}

	for _, cache := range cacheStates {
		b.Run(cache.name, func(b *testing.B) {
			for _, consumer := range consumers {
				b.Run(consumer.name, func(b *testing.B) {
					scanner, err := NewHistoryScanner(c.gitDir,
						WithScanMode(ScanModeHunks),
						WithPairCacheBudget(cache.pairBudget),
						WithOffsetCacheBudget(cache.offsetBudget))
					if err != nil {
						b.Fatalf("NewHistoryScanner(%s): %v", c.gitDir, err)
					}
					defer func() {
						if err := scanner.Close(); err != nil {
							b.Errorf("Close: %v", err)
						}
					}()

					// One untimed priming scan settles the commit, tree, and
					// metadata caches so both cache states measure a steady
					// state, and populates the diff memo when a budget was
					// given. Setup before b.Loop is excluded from timing.
					cons := consumer.make()
					if err := scanner.Scan(nil, cons); err != nil {
						b.Fatalf("priming scan: %v", err)
					}
					wantBytes, wantPayloads, wantMax := cons.totals()
					checkHunkScanTotals(b, c, wantBytes, wantPayloads, wantMax)
					checkPairCacheState(b, c, scanner, cache.pairBudget)

					b.SetBytes(wantBytes)
					b.ReportAllocs()
					for b.Loop() {
						cons.reset()
						if err := scanner.Scan(nil, cons); err != nil {
							b.Fatalf("Scan: %v", err)
						}
						gotBytes, gotPayloads, gotMax := cons.totals()
						if gotBytes != wantBytes || gotPayloads != wantPayloads || gotMax != wantMax {
							b.Fatalf("scan delivered %d bytes in %d payloads (max %d), want %d/%d/%d",
								gotBytes, gotPayloads, gotMax, wantBytes, wantPayloads, wantMax)
						}
					}
				})
			}
		})
	}
}

// checkHunkScanTotals asserts the external oracles a fixture promises, so a
// fixture that silently stops producing the payload shape under measurement
// fails rather than being benchmarked anyway.
func checkHunkScanTotals(b *testing.B, c hunkScanCase, gotBytes, gotPayloads, gotMax int64) {
	b.Helper()
	if gotPayloads == 0 {
		b.Fatalf("scan of %s delivered no payloads", c.gitDir)
	}
	if c.wantPayloadBytes > 0 && gotBytes != c.wantPayloadBytes {
		b.Fatalf("scan delivered %d payload bytes, want %d", gotBytes, c.wantPayloadBytes)
	}
	if c.wantPayloads > 0 && gotPayloads != c.wantPayloads {
		b.Fatalf("scan delivered %d payloads, want %d", gotPayloads, c.wantPayloads)
	}
	if c.wantMaxPayload > 0 && gotMax != c.wantMaxPayload {
		b.Fatalf("largest payload = %d bytes, want exactly %d", gotMax, c.wantMaxPayload)
	}
	if c.minMaxPayload > 0 && gotMax < c.minMaxPayload {
		b.Fatalf("largest payload = %d bytes, want at least %d", gotMax, c.minMaxPayload)
	}
}

// checkPairCacheState asserts that the requested cache state actually took
// effect: Cold must have memoized nothing, Warm must be serving every pair
// from the memo.
func checkPairCacheState(b *testing.B, c hunkScanCase, scanner *HistoryScanner, pairBudget int) {
	b.Helper()
	got := pairCacheLen(scanner.pairs)
	if pairBudget == 0 {
		if got != 0 {
			b.Fatalf("cold run memoized %d diff pairs, want 0", got)
		}
		return
	}
	switch {
	case c.wantWarmPairs > 0 && got != c.wantWarmPairs:
		b.Fatalf("warm run memoized %d diff pairs, want %d (budget %d too small?)",
			got, c.wantWarmPairs, pairBudget)
	case got == 0:
		b.Fatalf("warm run memoized no diff pairs (budget %d too small?)", pairBudget)
	}
}

// ---------------------------------------------------------------------------
// Reachability proofs
// ---------------------------------------------------------------------------

// TestScanHunksBench_BinaryFixtureTakesBinaryBranch proves that the
// binary-churn fixture satisfies the exact condition scanHunks branches on
// (hunk.isBinary && len(hunk.lines) == 1), and that a full Scan over it
// delivers each blob as one whole payload.
//
// Without this, BenchmarkScanHunks/BinaryChurn_* could be measuring the
// line-joining fallback and nobody would notice.
func TestScanHunksBench_BinaryFixtureTakesBinaryBranch(t *testing.T) {
	requireGit(t)

	// 2 MiB is above isBinary's 8 KiB window, above SmallFileThreshold, and
	// above the default pairCache admission limit, so the payload cannot be
	// mistaken for a small-file case.
	const (
		blobSize  = 2 << 20
		revisions = 2
	)
	gitDir := newHunkBenchFixtures(t.TempDir()).binaryChurn(t, blobSize, revisions)

	scanner, err := NewHistoryScanner(gitDir, WithScanMode(ScanModeHunks))
	if err != nil {
		t.Fatalf("NewHistoryScanner(%s): %v", gitDir, err)
	}
	defer scanner.Close()

	hunks, errC := scanner.DiffHistoryHunks()
	binaryHunks := 0
	for hunk := range hunks {
		if !hunk.IsBinary() {
			t.Errorf("hunk %s:%s is not binary; the fixture must only produce binary hunks",
				hunk.Commit(), hunk.Path())
			continue
		}
		if got := len(hunk.Lines()); got != 1 {
			t.Errorf("binary hunk %s:%s has %d lines, want 1; scanHunks only streams at exactly one line",
				hunk.Commit(), hunk.Path(), got)
			continue
		}
		if got := len(hunk.Lines()[0]); got != blobSize {
			t.Errorf("binary hunk %s:%s payload = %d bytes, want %d (the whole blob)",
				hunk.Commit(), hunk.Path(), got, blobSize)
		}
		binaryHunks++
	}
	if err := <-errC; err != nil {
		t.Fatalf("DiffHistoryHunks: %v", err)
	}
	if binaryHunks != revisions {
		t.Fatalf("got %d binary hunks, want %d", binaryHunks, revisions)
	}

	// Same fixture through Scan, which is the path the benchmark drives: each
	// payload must arrive whole, which requires scanHunks to have handed over
	// the single line.
	cons := new(readLoopConsumer)
	if err := scanner.Scan(nil, cons); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	gotBytes, gotPayloads, gotMax := cons.totals()
	if want := int64(blobSize) * revisions; gotBytes != want {
		t.Errorf("Scan delivered %d payload bytes, want %d", gotBytes, want)
	}
	if gotPayloads != revisions {
		t.Errorf("Scan delivered %d payloads, want %d", gotPayloads, revisions)
	}
	if gotMax != blobSize {
		t.Errorf("largest payload = %d bytes, want %d", gotMax, blobSize)
	}
}

// TestScanHunksBench_SinkShapesAreLoadBearing pins the standard-library facts
// that make the CopyToNonStringWriter sub-benchmark meaningful. If any of
// these change, the benchmark silently stops measuring what it claims to.
func TestScanHunksBench_SinkShapesAreLoadBearing(t *testing.T) {
	if _, ok := any(new(countingSink)).(io.StringWriter); ok {
		t.Error("countingSink must not implement io.StringWriter: strings.Reader.WriteTo would call WriteString and skip the []byte conversion this benchmark measures")
	}
	if _, ok := any(new(countingSink)).(io.ReaderFrom); ok {
		t.Error("countingSink must not implement io.ReaderFrom: io.Copy would prefer ReadFrom over strings.Reader.WriteTo")
	}
	if _, ok := io.Discard.(io.StringWriter); !ok {
		t.Error("io.Discard no longer implements io.StringWriter; revisit the comment on countingSink before using it here")
	}
	if _, ok := any(strings.NewReader("x")).(io.WriterTo); !ok {
		t.Error("strings.Reader no longer implements io.WriterTo; the copy consumer no longer distinguishes the payload shapes")
	}
	if _, ok := any(bytes.NewReader(nil)).(io.WriterTo); !ok {
		t.Error("bytes.Reader no longer implements io.WriterTo; the copy consumer no longer distinguishes the payload shapes")
	}
}

// TestScanHunksBench_DefaultPairCacheRefusesLargeBinaryHunks records why the
// Warm variant has to raise the pairCache budget.
//
// pairCache.add refuses any entry charged above budgetPerShard/4, which is
// 1 MiB at the default budget, so a default scanner never memoizes a
// multi-megabyte binary hunk and every repeated scan re-inflates the blob. It
// also shows the flip side: once the budget admits the entry, the memo retains
// the hunk — and because a binary hunk's single line is a zero-copy view over
// the whole blob, retaining it retains the whole blob.
func TestScanHunksBench_DefaultPairCacheRefusesLargeBinaryHunks(t *testing.T) {
	requireGit(t)

	const (
		blobSize  = 2 << 20
		revisions = 2
	)
	gitDir := newHunkBenchFixtures(t.TempDir()).binaryChurn(t, blobSize, revisions)

	withDefaults, err := NewHistoryScanner(gitDir, WithScanMode(ScanModeHunks))
	if err != nil {
		t.Fatalf("NewHistoryScanner(%s): %v", gitDir, err)
	}
	defer withDefaults.Close()
	if err := withDefaults.Scan(nil, new(readLoopConsumer)); err != nil {
		t.Fatalf("Scan with default budgets: %v", err)
	}
	if got := pairCacheLen(withDefaults.pairs); got != 0 {
		t.Errorf("default pairCache memoized %d pairs of %d-byte binary hunks, want 0: "+
			"add refuses entries above budgetPerShard/4 (%d bytes)",
			got, blobSize, defaultPairCacheBudget/pairCacheShards/4)
	}

	warmed, err := NewHistoryScanner(gitDir,
		WithScanMode(ScanModeHunks),
		WithPairCacheBudget(warmPairCacheBudget(blobSize)))
	if err != nil {
		t.Fatalf("NewHistoryScanner(%s): %v", gitDir, err)
	}
	defer warmed.Close()
	if err := warmed.Scan(nil, new(readLoopConsumer)); err != nil {
		t.Fatalf("Scan with warm budget: %v", err)
	}
	if got := pairCacheLen(warmed.pairs); got != revisions {
		t.Errorf("warm pairCache memoized %d pairs, want %d", got, revisions)
	}
}
