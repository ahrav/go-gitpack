// hunk_dedup_test.go verifies the WithHunkLineDedup pipeline (hunk_dedup.go)
// at three levels, per the repo's oracle-based test style:
//
//   - unit tests for lineFingerprintSet (probe behavior, zero-fingerprint
//     remap, deterministic saturation fail-open) and exact-value table tests
//     for dedupHunkEmission's whole-hunk verdicts (intact emission, full
//     suppression, intra-hunk duplicate boundary lines, binary passthrough);
//   - a property test that checks dedupHunkEmission against a trivial
//     map[string]bool "hunk contains at least one first-introduction line"
//     oracle on random line sequences;
//   - repository-level oracle and determinism tests: a dead-simple serial
//     reference implementation (ordered walk + plain map) must produce the
//     same emission multiset as the four-stage parallel pipeline — on both
//     the small fixture and a whale fixture that stresses the reorder
//     buffer and reuses every slot of the dispatch ring — the multiset
//     must be stable across runs, scanners, and GOMAXPROCS (imitating
//     commit_walk_determinism_test.go), every dedup emission must appear
//     verbatim in the streaming scan (hunks are removed whole, never
//     split), the dedup-off path must be unaffected, and the pipeline must
//     abandon cleanly (no goroutine leaks, no shutdown deadlock) when fn
//     errors or when a corrupted pack object fails the tree or hunk stage
//     mid-scan.

package objstore

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- lineFingerprintSet ---

// TestLineFingerprintSet_InsertProbe verifies the basic set contract: the
// first markNew of a fingerprint reports true, repeats report false, and
// fingerprints that collide into the same probe chain stay distinguishable.
func TestLineFingerprintSet_InsertProbe(t *testing.T) {
	set := newLineFingerprintSet(8) // 256 slots

	// 1, 257, and 513 are congruent mod 256, forcing linear-probe chains.
	fps := []uint64{1, 257, 513, 2, 3}
	for _, fp := range fps {
		require.Truef(t, set.markNew(fp), "first insert of %d must be new", fp)
	}
	for _, fp := range fps {
		require.Falsef(t, set.markNew(fp), "repeat of %d must be seen", fp)
	}
	require.True(t, set.markNew(999), "unrelated fingerprint must still be new")
}

// TestLineFingerprintSet_ZeroFingerprintRemap verifies that a raw
// fingerprint of zero (the empty-slot sentinel) is remapped and behaves like
// any other value, and pins the documented aliasing with the substitute
// constant.
func TestLineFingerprintSet_ZeroFingerprintRemap(t *testing.T) {
	set := newLineFingerprintSet(4)
	require.True(t, set.markNew(0), "first zero fingerprint must be new")
	require.False(t, set.markNew(0), "second zero fingerprint must be seen")
	// Zero is stored as dedupZeroFingerprint, so the two alias by design.
	require.False(t, set.markNew(dedupZeroFingerprint),
		"substitute constant aliases the remapped zero")
}

// TestLineFingerprintSet_SaturationFailOpen verifies that once the
// load-factor bound is hit the set fails open: every subsequent fingerprint
// — including previously inserted ones — reports new, so lines are emitted
// rather than dropped.
func TestLineFingerprintSet_SaturationFailOpen(t *testing.T) {
	set := newLineFingerprintSet(4) // 16 slots => saturates after 16*7/10 = 11 inserts
	limit := 16 * 7 / 10
	for i := range limit {
		require.Truef(t, set.markNew(uint64(i+1)), "insert %d below the bound", i)
	}
	require.True(t, set.saturated, "set must saturate at the load-factor bound")

	require.True(t, set.markNew(1), "seen fingerprint reports new after saturation")
	require.True(t, set.markNew(uint64(limit+100)), "unseen fingerprint reports new after saturation")
	require.True(t, set.markNew(1), "verdicts stay fail-open on every call")
}

// TestLineFingerprintSet_DeterministicVerdicts verifies that a fixed insert
// sequence — including repeats, zeros, and saturation on a small table —
// yields an identical verdict sequence on every fresh set.
func TestLineFingerprintSet_DeterministicVerdicts(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	seq := make([]uint64, 4096)
	for i := range seq {
		// A small domain (including 0) forces repeats and drives the
		// 32-slot table through saturation mid-sequence.
		seq[i] = uint64(rng.Intn(64))
	}

	verdicts := func() []bool {
		set := newLineFingerprintSet(5) // 32 slots => saturates after 22 inserts
		out := make([]bool, len(seq))
		for i, fp := range seq {
			out[i] = set.markNew(fp)
		}
		return out
	}

	require.Equal(t, verdicts(), verdicts(), "verdicts must be a pure function of the insert sequence")
}

// TestLineFingerprintSet_HasProbesWithoutInserting verifies has is a pure
// probe — it never inserts — and that it shares markNew's zero-fingerprint
// remap, so the two views of the set always agree on membership.
func TestLineFingerprintSet_HasProbesWithoutInserting(t *testing.T) {
	set := newLineFingerprintSet(8)

	require.False(t, set.has(5), "empty set must not contain 5")
	require.False(t, set.has(5), "a has probe must not insert")
	require.True(t, set.markNew(5), "5 must still be new after has probes")
	require.True(t, set.has(5), "5 must be present after markNew")

	require.False(t, set.has(0), "empty set must not contain the zero fingerprint")
	require.True(t, set.markNew(0))
	require.True(t, set.has(0), "zero fingerprint must be found via the remap")
	require.True(t, set.has(dedupZeroFingerprint),
		"substitute constant aliases the remapped zero, mirroring markNew")
}

// TestLineFingerprintSet_HasFailOpenAfterSaturation verifies the saturated
// set's two views stay consistent in the fail-open direction: has reports
// false even for fingerprints that were inserted before saturation. A
// regression flipping this to true would make dedupHunkEmission suppress
// every hunk forever after saturation — fail-closed data loss, the exact
// disaster the design avoids.
func TestLineFingerprintSet_HasFailOpenAfterSaturation(t *testing.T) {
	set := newLineFingerprintSet(4) // 16 slots => saturates after 11 inserts
	limit := 16 * 7 / 10
	for i := range limit {
		require.True(t, set.markNew(uint64(i+1)))
	}
	require.True(t, set.saturated)

	require.False(t, set.has(1), "inserted fingerprint must probe as absent after saturation")
	require.False(t, set.has(uint64(limit+100)), "unseen fingerprint must probe as absent after saturation")
}

// --- dedupHunkEmission ---

// mkTestHunk builds a text HunkAddition with the package's endLine
// convention (startLine + len(lines) - 1).
func mkTestHunk(start int, lines ...string) HunkAddition {
	var commit Hash
	commit[0] = 0xAB
	return HunkAddition{
		commit:    commit,
		path:      "f.txt",
		startLine: start,
		endLine:   start + len(lines) - 1,
		lines:     lines,
	}
}

// seedLines pre-marks lines as already seen.
func seedLines(set *lineFingerprintSet, lines ...string) {
	for _, l := range lines {
		set.markNew(lineFingerprint(l))
	}
}

// TestDedupHunkEmission_Table pins the whole-hunk verdict rule: a hunk
// survives iff at least one of its lines is unseen at the start of the
// hunk, and is suppressed entirely otherwise. Intact emission is structural
// — the verdict function cannot modify the hunk the caller forwards.
func TestDedupHunkEmission_Table(t *testing.T) {
	// keyringHunk models git.git commit 1e3eefbc's t/lib-gpg/keyring.gpg
	// add-hunk in miniature: two armored key blocks inside ONE hunk sharing
	// their closing line. Any sub-hunk emission granularity decapitates the
	// second block; whole-hunk emission must keep both END markers.
	keyringHunk := mkTestHunk(1,
		"-----BEGIN PGP PRIVATE KEY BLOCK-----",
		"key-one-data",
		"-----END PGP PRIVATE KEY BLOCK-----",
		"-----BEGIN PGP PRIVATE KEY BLOCK-----",
		"key-two-data",
		"-----END PGP PRIVATE KEY BLOCK-----",
	)

	tests := []struct {
		name     string
		seed     []string       // lines marked seen before the hunk is processed
		prior    []HunkAddition // hunks fed through dedup first (marking their lines)
		hunk     HunkAddition
		wantEmit bool
	}{
		{
			name:     "all-new hunk is emitted intact",
			hunk:     mkTestHunk(10, "aa", "bb", "cc"),
			wantEmit: true,
		},
		{
			name:     "all-seen hunk is suppressed entirely",
			seed:     []string{"aa", "bb", "cc"},
			hunk:     mkTestHunk(10, "aa", "bb", "cc"),
			wantEmit: false,
		},
		{
			name:     "mixed hunk is emitted intact, seen lines included",
			seed:     []string{"seen-head", "seen-tail"},
			hunk:     mkTestHunk(7, "seen-head", "only-new", "seen-tail"),
			wantEmit: true,
		},
		{
			name:     "keyring case: intra-hunk duplicate boundary line, hunk emits intact",
			hunk:     keyringHunk,
			wantEmit: true,
		},
		{
			name: "keyring case with pre-seen armor boundaries still emits intact",
			seed: []string{
				"-----BEGIN PGP PRIVATE KEY BLOCK-----",
				"-----END PGP PRIVATE KEY BLOCK-----",
			},
			hunk:     keyringHunk,
			wantEmit: true,
		},
		{
			name:     "only-unseen content is an intra-hunk duplicate: verdict predates marks",
			seed:     []string{"seen-a", "seen-b"},
			hunk:     mkTestHunk(4, "seen-a", "dup-x", "seen-b", "dup-x"),
			wantEmit: true,
		},
		{
			name:     "duplicate hunk in a later commit is suppressed entirely",
			prior:    []HunkAddition{mkTestHunk(1, "p", "q", "r")},
			hunk:     mkTestHunk(50, "p", "q", "r"),
			wantEmit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set := newLineFingerprintSet(12)
			seedLines(set, tt.seed...)
			for _, p := range tt.prior {
				_ = dedupHunkEmission(p, set)
			}

			require.Equal(t, tt.wantEmit, dedupHunkEmission(tt.hunk, set))
		})
	}
}

// TestDedupHunkEmission_BinaryPassthrough verifies binary hunks bypass
// dedup entirely: they are never suppressed, even on repeated observation
// of identical content, because they never mark the set.
func TestDedupHunkEmission_BinaryPassthrough(t *testing.T) {
	set := newLineFingerprintSet(8)
	bin := HunkAddition{
		commit:    Hash{0x01},
		path:      "blob.bin",
		startLine: 1,
		endLine:   1,
		lines:     []string{"\x00binary payload\x00"},
		isBinary:  true,
	}

	for pass := range 2 {
		require.Truef(t, dedupHunkEmission(bin, set),
			"pass %d: binary hunk must never be suppressed", pass)
	}
}

// TestDedupHunkEmission_SaturationFailOpen drives the verdict function
// itself across the saturation boundary. Post-saturation correctness is a
// conjunction — has must report absent AND markNew must report new — and
// the verdict is where the two meet: before saturation an all-seen hunk is
// suppressed; after saturation the identical hunk must survive.
func TestDedupHunkEmission_SaturationFailOpen(t *testing.T) {
	set := newLineFingerprintSet(4) // 16 slots => saturates after 11 inserts

	first := mkTestHunk(1, "s1", "s2", "s3", "s4", "s5", "s6")
	require.True(t, dedupHunkEmission(first, set), "six fresh lines must emit")
	require.False(t, set.saturated, "6 of 11 inserts must not saturate")
	require.False(t, dedupHunkEmission(first, set),
		"below saturation the duplicate hunk is suppressed")

	saturating := mkTestHunk(10, "t1", "t2", "t3", "t4", "t5")
	require.True(t, dedupHunkEmission(saturating, set), "five more fresh lines must emit")
	require.True(t, set.saturated, "11th insert must saturate the set")

	require.True(t, dedupHunkEmission(first, set),
		"after saturation the previously suppressed hunk must fail open and emit")
	require.True(t, dedupHunkEmission(first, set),
		"fail-open verdicts must hold on every subsequent call")
}

// TestDedupHunkEmission_EmptyLines pins the degenerate case: a non-binary
// hunk with no lines has no first introduction to report, so it is
// suppressed (anyNew never becomes true) and leaves the set untouched.
// computeAddedHunks never emits an empty text hunk, so this documents
// defensive behavior rather than a reachable pipeline state.
func TestDedupHunkEmission_EmptyLines(t *testing.T) {
	set := newLineFingerprintSet(8)
	empty := HunkAddition{commit: Hash{0x01}, path: "f.txt", startLine: 1, endLine: 1}

	require.False(t, dedupHunkEmission(empty, set), "an empty hunk has nothing new to emit")
	require.True(t, dedupHunkEmission(mkTestHunk(1, "later"), set),
		"the empty hunk must not have marked or saturated anything")
}

// oracleWholeHunkVerdict is the trivial reference for the whole-hunk dedup
// rule: probe every line against seen (the pre-hunk state), then mark all
// lines, and report whether any line was unseen.
func oracleWholeHunkVerdict(seen map[string]bool, lines []string) bool {
	anyNew := false
	for _, ln := range lines {
		if !seen[ln] {
			anyNew = true
		}
	}
	for _, ln := range lines {
		seen[ln] = true
	}
	return anyNew
}

// TestDedupHunkEmission_PropertyOracle feeds random hunk series through
// dedupHunkEmission and cross-checks every verdict against
// oracleWholeHunkVerdict — which, like the implementation, probes all lines
// against the pre-hunk state before marking any.
func TestDedupHunkEmission_PropertyOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	set := newLineFingerprintSet(16)
	oracle := make(map[string]bool)

	for hunkIdx := range 500 {
		n := 1 + rng.Intn(40)
		lines := make([]string, n)
		for i := range lines {
			// A small domain forces frequent re-introductions and
			// intra-hunk duplicates.
			lines[i] = fmt.Sprintf("line-%d", rng.Intn(120))
		}
		h := mkTestHunk(1+rng.Intn(1000), lines...)

		wantEmit := oracleWholeHunkVerdict(oracle, lines)
		require.Equalf(t, wantEmit, dedupHunkEmission(h, set),
			"hunk %d: verdict diverged from oracle", hunkIdx)
	}
}

// --- repository fixtures ---

// dedupRepoBuilder scripts a git repository with strictly increasing commit
// timestamps, so the parent-first order used by the dedup pipeline follows
// authorship order and tests can predict first-introduction attribution.
type dedupRepoBuilder struct {
	t       *testing.T
	repoDir string
	tick    int64
}

// newDedupRepoBuilder initializes an empty repository (skipping the test if
// git is unavailable, matching the repo's fixture convention).
func newDedupRepoBuilder(t *testing.T) *dedupRepoBuilder {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}
	b := &dedupRepoBuilder{t: t, repoDir: t.TempDir(), tick: 1112911993}
	b.git("init", "-q", "-b", "main")
	b.git("config", "commit.gpgsign", "false")
	return b
}

// git runs one git command in the fixture repo and returns its trimmed
// combined output.
func (b *dedupRepoBuilder) git(args ...string) string {
	b.t.Helper()
	ts := fmt.Sprintf("%d +0000", b.tick)
	cmd := exec.Command("git", append([]string{"-C", b.repoDir}, args...)...)
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@e",
		"GIT_AUTHOR_DATE="+ts,
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@e",
		"GIT_COMMITTER_DATE="+ts,
	)
	out, err := cmd.CombinedOutput()
	require.NoErrorf(b.t, err, "git %s: %s", strings.Join(args, " "), out)
	return strings.TrimSpace(string(out))
}

// write creates or replaces a file in the working tree.
func (b *dedupRepoBuilder) write(name, content string) {
	b.t.Helper()
	require.NoError(b.t, os.WriteFile(filepath.Join(b.repoDir, name), []byte(content), 0o644))
}

// commit stages everything and commits it at the next timestamp tick,
// returning the new commit's OID.
func (b *dedupRepoBuilder) commit(msg string) string {
	b.t.Helper()
	b.tick++
	b.git("add", "-A")
	b.git("commit", "-q", "--allow-empty", "-m", msg)
	return b.git("rev-parse", "HEAD")
}

// mergeNoFF merges branch into the current branch with a real merge commit
// at the next timestamp tick, returning the merge commit's OID.
func (b *dedupRepoBuilder) mergeNoFF(msg, branch string) string {
	b.t.Helper()
	b.tick++
	b.git("merge", "-q", "--no-ff", "-m", msg, branch)
	return b.git("rev-parse", "HEAD")
}

// finish repacks the repository, removes any auto-written commit-graph (so
// the scanner takes the ref-walk path, like buildMergeRepo), and returns the
// .git directory.
func (b *dedupRepoBuilder) finish() string {
	b.t.Helper()
	b.git("repack", "-a", "-d")
	gitDir := filepath.Join(b.repoDir, ".git")
	for _, name := range []string{"commit-graph", "commit-graphs"} {
		_ = os.RemoveAll(filepath.Join(gitDir, "objects", "info", name))
	}
	return gitDir
}

// dedupBinaryContent is the byte-identical payload committed at two paths
// by buildDedupOracleRepo; the NUL bytes make computeAddedHunks classify
// both blobs as binary.
const dedupBinaryContent = "\x00\x01\x02binary-payload\x00\xfftail\n"

// buildDedupOracleRepo creates the fixture used by the oracle and
// determinism tests: a branch + merge, a revert + re-add, a rename with an
// edit, a copied file made entirely of already-seen lines, and a re-added
// identical binary blob. All five shapes force dedup verdicts that differ
// from plain per-commit emission — the binary re-add in the opposite
// direction (never suppressed).
func buildDedupOracleRepo(t *testing.T) string {
	b := newDedupRepoBuilder(t)

	b.write("a.txt", "alpha\nbravo\ncharlie\n")
	b.commit("root")

	b.write("b.txt", "b-one\nb-two\nshared-interior\n")
	b.commit("add b")

	// Feature branch introduces a multi-line block whose interior line is
	// already known — the merge replays it against main's first parent.
	b.git("checkout", "-q", "-b", "feature")
	b.write("secret.txt", "-----BEGIN KEY-----\nkeydata-one\nshared-interior\nkeydata-two\n-----END KEY-----\n")
	b.commit("feature secret")

	b.git("checkout", "-q", "main")
	b.write("a.txt", "alpha\nbravo\ncharlie\ndelta\n")
	b.commit("main delta")
	b.mergeNoFF("merge feature", "feature")

	// Revert + re-add: bravo's second introduction must be suppressed.
	b.write("a.txt", "alpha\ncharlie\ndelta\n")
	b.commit("remove bravo")
	b.write("a.txt", "alpha\nbravo\ncharlie\ndelta\n")
	b.commit("re-add bravo")

	// Rename with an edit in the same commit: only the edited line is new.
	b.git("mv", "b.txt", "c.txt")
	b.write("c.txt", "b-one\nb-two-edited\nshared-interior\n")
	b.commit("rename b to c with edit")

	// Copy: a fresh file made entirely of already-seen lines.
	b.write("a_copy.txt", "alpha\nbravo\ncharlie\ndelta\n")
	b.commit("copy a")

	// Binary re-introduction: identical bytes at a second path. Text this
	// redundant is suppressed; binary must bypass dedup and emit both times.
	b.write("bin.dat", dedupBinaryContent)
	b.commit("add binary")
	b.write("bin_copy.dat", dedupBinaryContent)
	b.commit("copy binary")

	return b.finish()
}

// --- repository-level oracle and determinism tests ---

// canonicalHunkAddition renders one emission in a stable string form for
// sorted-multiset comparison.
func canonicalHunkAddition(h HunkAddition) string {
	return fmt.Sprintf("%s|%s|%d|%d|%s",
		h.Commit(), h.Path(), h.StartLine(), h.EndLine(), strings.Join(h.Lines(), "\\n"))
}

// collectHunkScan runs DiffHistoryHunksFunc on a fresh scanner and returns
// the sorted emission multiset.
func collectHunkScan(t *testing.T, gitDir string, opts ...ScannerOption) []string {
	t.Helper()
	s, err := NewHistoryScanner(gitDir, opts...)
	require.NoError(t, err)
	defer s.Close()
	return collectHunkScanWith(t, s)
}

// collectHunkScanWith runs DiffHistoryHunksFunc on an existing scanner and
// returns the sorted emission multiset. fn is invoked concurrently, so the
// collector serializes appends.
func collectHunkScanWith(t *testing.T, s *HistoryScanner) []string {
	t.Helper()
	var (
		mu  sync.Mutex
		out []string
	)
	require.NoError(t, s.DiffHistoryHunksFunc(func(h HunkAddition) error {
		mu.Lock()
		out = append(out, canonicalHunkAddition(h))
		mu.Unlock()
		return nil
	}))
	sort.Strings(out)
	return out
}

// serialDedupReference computes the expected dedup emissions with a
// dead-simple single-threaded walk: ordered commits, the library's own pair
// and hunk enumeration, and oracleWholeHunkVerdict over a plain
// map[string]bool. It is deliberately independent of lineFingerprintSet
// and dedupHunkEmission, and applies skipPair (matching WithHunkPathFilter
// semantics) itself rather than through the scanner option, so filter
// placement is cross-checked too. skipPair may be nil.
func serialDedupReference(t *testing.T, gitDir string, skipMergeDiffs bool, skipPair func(Hash, string) bool) []string {
	t.Helper()
	s, err := NewHistoryScanner(gitDir)
	require.NoError(t, err)
	defer s.Close()

	commits, err := s.loadAllCommits()
	require.NoError(t, err)
	order := orderCommitsParentFirst(commits)

	seen := make(map[string]bool)
	var out []string
	for _, c := range order {
		if skipMergeDiffs && len(c.ParentOIDs) > 1 {
			continue
		}
		parentTree, err := s.firstParentTree(c)
		require.NoError(t, err)

		var pairs []blobPairWork
		require.NoError(t, s.emitCommitBlobPairs(c, parentTree, func(w blobPairWork) error {
			if skipPair != nil && skipPair(w.commit, filepath.ToSlash(w.path)) {
				return nil
			}
			pairs = append(pairs, w)
			return nil
		}))

		for _, p := range pairs {
			var hunks []HunkAddition
			require.NoError(t, s.streamBlobPairHunks(p, func(h HunkAddition) error {
				hunks = append(hunks, h)
				return nil
			}))
			for _, h := range hunks {
				if h.IsBinary() {
					out = append(out, canonicalHunkAddition(h))
					continue
				}
				if oracleWholeHunkVerdict(seen, h.Lines()) {
					out = append(out, canonicalHunkAddition(h))
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// TestDiffHistoryHunksDedup_SerialOracle asserts the parallel dedup
// pipeline's sorted emission multiset equals the serial reference on the
// merge/revert/rename/copy fixture, with and without skipMergeDiffs.
func TestDiffHistoryHunksDedup_SerialOracle(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)

	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skipMergeDiffs=%v", skip), func(t *testing.T) {
			want := serialDedupReference(t, gitDir, skip, nil)
			require.NotEmpty(t, want)
			got := collectHunkScan(t, gitDir,
				WithHunkLineDedup(true), WithSkipMergeDiffs(skip))
			require.Equal(t, want, got)
		})
	}
}

// TestDiffHistoryHunksDedup_DeterministicMultiset asserts the dedup output
// is a stable multiset across repeated scans on the same scanner and across
// fresh scanners, imitating TestDiffHistoryHunks_DeterministicMultiset.
func TestDiffHistoryHunksDedup_DeterministicMultiset(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)

	want := collectHunkScan(t, gitDir, WithHunkLineDedup(true))
	require.NotEmpty(t, want)

	s, err := NewHistoryScanner(gitDir, WithHunkLineDedup(true))
	require.NoError(t, err)
	defer s.Close()
	for run := range 4 {
		require.Equalf(t, want, collectHunkScanWith(t, s),
			"same-scanner run %d diverged", run)
	}

	for run := range 3 {
		require.Equalf(t, want, collectHunkScan(t, gitDir, WithHunkLineDedup(true)),
			"fresh-scanner run %d diverged", run)
	}
}

// scanWithProcs runs collectHunkScan on a fresh scanner while GOMAXPROCS
// is temporarily set to procs, restoring it before returning.
func scanWithProcs(t *testing.T, procs int, gitDir string, opts ...ScannerOption) []string {
	t.Helper()
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(procs))
	return collectHunkScan(t, gitDir, opts...)
}

// TestDiffHistoryHunksDedup_GOMAXPROCSInvariant asserts a single-threaded
// scan and a parallel scan agree — the strongest cheap probe for output
// that secretly depends on worker scheduling.
func TestDiffHistoryHunksDedup_GOMAXPROCSInvariant(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)

	single := scanWithProcs(t, 1, gitDir, WithHunkLineDedup(true))
	multi := scanWithProcs(t, runtime.NumCPU(), gitDir, WithHunkLineDedup(true))
	require.Equal(t, single, multi, "dedup result depends on parallelism")
}

// TestDiffHistoryHunks_DedupOffUnaffected asserts the dedup-off path is
// untouched: a scanner with WithHunkLineDedup(false) matches one where the
// option was never set, and dedup-on actually changes the fixture's output
// (so the equality above is not vacuous).
func TestDiffHistoryHunks_DedupOffUnaffected(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)

	base := collectHunkScan(t, gitDir)
	explicitOff := collectHunkScan(t, gitDir, WithHunkLineDedup(false))
	require.Equal(t, base, explicitOff, "explicit dedup-off must match the default path")

	on := collectHunkScan(t, gitDir, WithHunkLineDedup(true))
	require.NotEqual(t, base, on, "fixture must exercise dedup suppression")
}

// TestDiffHistoryHunksDedup_BinaryPassthroughRepoLevel drives the binary
// bypass through the real pipeline: the identical binary payload is
// committed at two paths, and dedup must emit a binary hunk for both
// introductions — a text file this redundant is suppressed (a_copy.txt in
// the same fixture proves the contrast). This is the repo-level companion
// to TestDedupHunkEmission_BinaryPassthrough, which checks the verdict
// function in isolation.
func TestDiffHistoryHunksDedup_BinaryPassthroughRepoLevel(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)

	s, err := NewHistoryScanner(gitDir, WithHunkLineDedup(true))
	require.NoError(t, err)
	defer s.Close()

	var (
		mu     sync.Mutex
		binary = map[string]int{} // path -> binary emissions
	)
	require.NoError(t, s.DiffHistoryHunksFunc(func(h HunkAddition) error {
		if h.IsBinary() {
			mu.Lock()
			binary[h.Path()]++
			mu.Unlock()
		}
		return nil
	}))

	require.Equal(t, map[string]int{"bin.dat": 1, "bin_copy.dat": 1}, binary,
		"both introductions of the identical binary blob must survive dedup")
}

// TestScan_HunksModeHonorsHunkLineDedup verifies ScanModeHunks routes
// through the dedup pipeline: with dedup on, the copied/re-added fixture
// content stops producing extra ScanBlob calls.
func TestScan_HunksModeHonorsHunkLineDedup(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)

	count := func(dedup bool) int {
		s, err := NewHistoryScanner(gitDir,
			WithScanMode(ScanModeHunks), WithHunkLineDedup(dedup))
		require.NoError(t, err)
		defer s.Close()
		var sc countingBlobScanner
		require.NoError(t, s.Scan(nil, &sc))
		return int(sc.count.Load())
	}

	off := count(false)
	on := count(true)
	require.Positive(t, on)
	require.Less(t, on, off, "dedup must suppress re-introduced hunks in hunks scan mode")
}

// countingBlobScanner counts ScanBlob invocations; safe for concurrent use.
type countingBlobScanner struct {
	count atomic.Int64
}

func (c *countingBlobScanner) ScanBlob(r io.Reader, _ ScanMeta) error {
	if _, err := io.Copy(io.Discard, r); err != nil {
		return err
	}
	c.count.Add(1)
	return nil
}

// TestDiffHistoryHunksDedup_FirstIntroductionAttribution asserts, on a
// merge fixture scanned with skipMergeDiffs=false, that a line introduced
// on a feature branch and re-arriving via the merge is attributed to the
// branch commit, while genuinely-new content added during the merge itself
// (an evil merge) is attributed to the merge commit.
func TestDiffHistoryHunksDedup_FirstIntroductionAttribution(t *testing.T) {
	b := newDedupRepoBuilder(t)
	b.write("base.txt", "base\n")
	b.commit("root")

	b.git("checkout", "-q", "-b", "feature")
	b.write("feat.txt", "feature-secret-line\n")
	featOID := b.commit("feature adds secret")

	b.git("checkout", "-q", "main")
	b.write("main.txt", "mainline\n")
	b.commit("main moves on")

	// Evil merge: content that exists in neither parent is added while the
	// merge is uncommitted, so it first appears in the merge commit itself.
	b.tick++
	b.git("merge", "-q", "--no-commit", "--no-ff", "feature")
	b.write("evil.txt", "evil-merge-line\n")
	b.git("add", "-A")
	b.git("commit", "-q", "-m", "merge feature with evil content")
	mergeOID := b.git("rev-parse", "HEAD")
	gitDir := b.finish()

	s, err := NewHistoryScanner(gitDir,
		WithHunkLineDedup(true), WithSkipMergeDiffs(false))
	require.NoError(t, err)
	defer s.Close()

	var (
		mu        sync.Mutex
		byContent = map[string][]string{} // line content -> emitting commit OIDs
	)
	require.NoError(t, s.DiffHistoryHunksFunc(func(h HunkAddition) error {
		mu.Lock()
		defer mu.Unlock()
		for _, line := range h.Lines() {
			byContent[line] = append(byContent[line], h.Commit().String())
		}
		return nil
	}))

	require.Equal(t, []string{featOID}, byContent["feature-secret-line"],
		"branch line must be attributed to the branch commit, not the merge")
	require.Equal(t, []string{mergeOID}, byContent["evil-merge-line"],
		"evil-merge line must be attributed to the merge commit")
}

// --- path filtering (WithHunkPathFilter) ---

// TestDiffHistoryHunksDedup_PathFilterRestoresSkippedIntroduction is THE
// regression scenario for WithHunkPathFilter: a line first introduced in a
// path the consumer filters, then re-added in a path it keeps. With the
// filter, dedup emits the line exactly once at its first UNSKIPPED
// introduction; without the filter it is emitted only at the filtered path
// — the composition hazard (consumer-side filtering after dedup marking)
// that the option exists to solve.
func TestDiffHistoryHunksDedup_PathFilterRestoresSkippedIntroduction(t *testing.T) {
	b := newDedupRepoBuilder(t)
	b.write("keep.txt", "base\n")
	b.commit("root")
	b.write("gitleaks.toml", "secret-line-L\nallowlisted-noise\n")
	filteredCommit := b.commit("add allowlisted config")
	b.write("betterleaks.toml", "secret-line-L\n")
	keptCommit := b.commit("add scanned config")
	gitDir := b.finish()

	skipAllowlisted := func(_ Hash, path string) bool { return path == "gitleaks.toml" }

	// occurrences collects every (commit, path) that emits the secret line.
	occurrences := func(opts ...ScannerOption) [][2]string {
		s, err := NewHistoryScanner(gitDir, opts...)
		require.NoError(t, err)
		defer s.Close()
		var (
			mu  sync.Mutex
			out [][2]string
		)
		require.NoError(t, s.DiffHistoryHunksFunc(func(h HunkAddition) error {
			for _, line := range h.Lines() {
				if line == "secret-line-L" {
					mu.Lock()
					out = append(out, [2]string{h.Commit().String(), h.Path()})
					mu.Unlock()
				}
			}
			return nil
		}))
		return out
	}

	withFilter := occurrences(WithHunkLineDedup(true), WithHunkPathFilter(skipAllowlisted))
	require.Equal(t, [][2]string{{keptCommit, "betterleaks.toml"}}, withFilter,
		"filtered pairs must not mark the fingerprint set; the first unskipped introduction wins")

	withoutFilter := occurrences(WithHunkLineDedup(true))
	require.Equal(t, [][2]string{{filteredCommit, "gitleaks.toml"}}, withoutFilter,
		"without the filter the only emission lands in the consumer-filtered path")
}

// TestDiffHistoryHunks_PathFilterNonDedup verifies the filter in the
// streaming (non-dedup) pipeline: filtered paths produce no hunks, and every
// other emission is byte-identical to the unfiltered scan minus that path.
func TestDiffHistoryHunks_PathFilterNonDedup(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)
	skipA := func(_ Hash, path string) bool { return path == "a.txt" }

	unfiltered := collectHunkScan(t, gitDir)
	filtered := collectHunkScan(t, gitDir, WithHunkPathFilter(skipA))

	// Canonical form is "commit|path|start|end|lines", so the path is
	// delimiter-bounded and cannot collide with a_copy.txt.
	var want []string
	for _, e := range unfiltered {
		if strings.Contains(e, "|a.txt|") {
			continue
		}
		want = append(want, e)
	}
	require.NotEqual(t, unfiltered, filtered, "fixture must contain a.txt hunks")
	require.Equal(t, want, filtered,
		"filtered scan must equal the unfiltered multiset minus the filtered path")
}

// TestDiffHistoryHunksDedup_PathFilterDeterminism asserts the dedup+filter
// combination keeps every determinism guarantee — stable multiset across
// runs, GOMAXPROCS invariance — and matches the serial reference applying
// the same predicate independently.
func TestDiffHistoryHunksDedup_PathFilterDeterminism(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)
	// Filtering b.txt keeps its lines out of the fingerprint set entirely,
	// so the reference and the pipeline must agree on both the removed
	// emissions and every downstream verdict.
	skipB := func(_ Hash, path string) bool { return path == "b.txt" }
	opts := []ScannerOption{WithHunkLineDedup(true), WithHunkPathFilter(skipB)}

	want := collectHunkScan(t, gitDir, opts...)
	require.NotEmpty(t, want)
	for run := range 3 {
		require.Equalf(t, want, collectHunkScan(t, gitDir, opts...),
			"dedup+filter run %d diverged", run)
	}

	require.Equal(t,
		scanWithProcs(t, 1, gitDir, opts...),
		scanWithProcs(t, runtime.NumCPU(), gitDir, opts...),
		"dedup+filter result depends on parallelism")

	require.Equal(t, serialDedupReference(t, gitDir, false, skipB), want,
		"parallel dedup+filter must match the serial reference")
}

// TestDiffHistoryHunks_NilPathFilterNoChange asserts WithHunkPathFilter(nil)
// is a no-op in both hunk pipelines: the multiset equals a scan where the
// option was never set.
func TestDiffHistoryHunks_NilPathFilterNoChange(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)

	require.Equal(t,
		collectHunkScan(t, gitDir),
		collectHunkScan(t, gitDir, WithHunkPathFilter(nil)),
		"nil filter must not change the streaming pipeline")

	require.Equal(t,
		collectHunkScan(t, gitDir, WithHunkLineDedup(true)),
		collectHunkScan(t, gitDir, WithHunkLineDedup(true), WithHunkPathFilter(nil)),
		"nil filter must not change the dedup pipeline")
}

// --- stress and abandonment ---

// buildDedupWhaleRepo creates a repository with one whale commit (a
// several-thousand-line file) followed by more small commits than the
// dedup pipeline's look-ahead window, so the reorder buffer, the bounded
// dispatch window, and slot-ring reuse are all exercised under skewed
// pair-completion times.
func buildDedupWhaleRepo(t *testing.T) string {
	b := newDedupRepoBuilder(t)

	b.write("small.txt", "seed\n")
	b.commit("root")

	var whale strings.Builder
	for i := range 5000 {
		fmt.Fprintf(&whale, "whale line %d payload %d\n", i, i*i)
	}
	// The whale sits early so its pair holds the reorder window open while
	// small commits complete behind it.
	b.write("whale.txt", whale.String())
	b.commit("whale")

	var small strings.Builder
	small.WriteString("seed\n")
	// > 2*dedupLookaheadCommits total commits: every one of the 256 ring
	// slots is reused at least once, not just the low indices.
	for i := range 520 {
		fmt.Fprintf(&small, "small line %d\n", i)
		b.write("small.txt", small.String())
		// Order-sensitive echoes: every eighth commit also adds a fresh
		// file whose lines were all first introduced earlier — two whale
		// lines and the small line from eight commits back. In seq order
		// every echo hunk is entirely already-seen and is suppressed; a
		// decision-order slip (echo verdict computed before the whale's
		// or the earlier small commit's) emits it and changes the
		// multiset. Without these, every whale-fixture line is globally
		// unique and the oracle equality is blind to decision reordering.
		if i >= 8 && i%8 == 0 {
			b.write(fmt.Sprintf("echo-%d.txt", i), fmt.Sprintf(
				"whale line %d payload %d\nwhale line %d payload %d\nsmall line %d\n",
				i, i*i, i+1, (i+1)*(i+1), i-8))
		}
		b.commit(fmt.Sprintf("small %d", i))
	}

	return b.finish()
}

// TestDiffHistoryHunksDedup_WhaleStress runs the dedup scan over the whale
// fixture under a hard timeout and checks its output against the serial
// reference. Completion alone would only prove liveness; the equality check
// makes the reorder buffer, seq stamping, and slot-ring reuse — the pieces
// this fixture stresses with skewed pair-completion times — accountable for
// producing the correct multiset. The fixture's echo files carry the
// order-sensitive verdicts: their hunks are entirely already-seen in seq
// order, so a decision-order slip emits them and breaks both assertions.
func TestDiffHistoryHunksDedup_WhaleStress(t *testing.T) {
	gitDir := buildDedupWhaleRepo(t)

	s, err := NewHistoryScanner(gitDir, WithHunkLineDedup(true))
	require.NoError(t, err)
	defer s.Close()

	var (
		mu      sync.Mutex
		got     []string
		scanErr error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		scanErr = s.DiffHistoryHunksFunc(func(h HunkAddition) error {
			mu.Lock()
			got = append(got, canonicalHunkAddition(h))
			mu.Unlock()
			return nil
		})
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Minute):
		t.Fatal("dedup scan did not complete within timeout (possible pipeline deadlock)")
	}
	require.NoError(t, scanErr)
	sort.Strings(got)
	require.NotEmpty(t, got)

	// Vacuity guard: the streaming scan must emit the echo hunks that
	// dedup is expected to suppress, or the NotContains below proves
	// nothing about ordering.
	echoes := 0
	for _, e := range collectHunkScan(t, gitDir) {
		if strings.Contains(e, "|echo-") {
			echoes++
		}
	}
	require.Positive(t, echoes, "fixture must produce echo hunks in the streaming scan")

	for _, e := range got {
		require.NotContainsf(t, e, "|echo-",
			"echo hunks are fully already-seen in seq order and must be suppressed: %s", e)
	}
	require.Equal(t, serialDedupReference(t, gitDir, false, nil), got,
		"parallel pipeline diverged from the serial reference under reorder stress")
}

// TestDiffHistoryHunksDedup_StreamingContainment asserts every dedup
// emission appears verbatim in the streaming (non-dedup) scan's multiset.
// Unlike the serial-oracle test — whose reference shares emitCommitBlobPairs
// and streamBlobPairHunks with the pipeline under test — the streaming scan
// enumerates commits through walkCommitsFromRefs, so this catches commit-set
// drift between the two pipelines' commit sources as well as any hunk that
// dedup split, truncated, or otherwise modified in flight. Dedup may only
// ever remove whole emissions.
func TestDiffHistoryHunksDedup_StreamingContainment(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)

	streaming := collectHunkScan(t, gitDir)
	dedup := collectHunkScan(t, gitDir, WithHunkLineDedup(true))
	require.NotEmpty(t, dedup)

	remaining := make(map[string]int, len(streaming))
	for _, e := range streaming {
		remaining[e]++
	}
	for _, e := range dedup {
		require.Positivef(t, remaining[e],
			"dedup emitted a hunk absent from (or more often than) the streaming scan: %s", e)
		remaining[e]--
	}
}

// requireGoroutinesSettle polls until the process goroutine count returns
// to at most before, failing after a deadline. The dedup pipeline waits for
// all its goroutines before returning, but unrelated runtime goroutines add
// noise, hence the poll.
func requireGoroutinesSettle(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if runtime.NumGoroutine() <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines leaked: before=%d after=%d", before, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDiffHistoryHunksDedup_ErrorStopsPromptlyNoLeak asserts that an fn
// error aborts the scan with that error and that every pipeline goroutine
// exits — the goroutine count must return to its pre-scan level.
func TestDiffHistoryHunksDedup_ErrorStopsPromptlyNoLeak(t *testing.T) {
	gitDir := buildDedupOracleRepo(t)

	s, err := NewHistoryScanner(gitDir, WithHunkLineDedup(true))
	require.NoError(t, err)
	defer s.Close()

	before := runtime.NumGoroutine()

	sentinel := errors.New("stop after a few hunks")
	var calls atomic.Int64
	err = s.DiffHistoryHunksFunc(func(h HunkAddition) error {
		if calls.Add(1) >= 3 {
			return sentinel
		}
		return nil
	})
	require.ErrorIs(t, err, sentinel)
	requireGoroutinesSettle(t, before)
}

// --- mid-scan object failures (tree and hunk stage abort paths) ---

// packObjectSpan locates oid's entry in the fixture's single packfile via
// `git verify-pack -v` and returns the pack path plus the byte span
// [offset, offset+size) that the object's packed representation occupies.
func packObjectSpan(t *testing.T, gitDir, oid string) (packPath string, offset, size int64) {
	t.Helper()
	idxs, err := filepath.Glob(filepath.Join(gitDir, "objects", "pack", "*.idx"))
	require.NoError(t, err)
	require.Len(t, idxs, 1, "fixture must be repacked into exactly one pack")

	out, err := exec.Command("git", "verify-pack", "-v", idxs[0]).CombinedOutput()
	require.NoErrorf(t, err, "git verify-pack -v: %s", out)

	// Object lines are "oid type size size-in-packfile offset-in-packfile"
	// with " depth base-oid" appended for deltified entries.
	for line := range strings.Lines(string(out)) {
		fields := strings.Fields(line)
		if len(fields) < 5 || fields[0] != oid {
			continue
		}
		// Corruption detection relies on the idx CRC check, which the
		// store runs only on the non-delta read path; a deltified target
		// would make the corruption invisible. Tiny fixture objects are
		// never deltified by git's packing heuristics — pin that here so
		// a packing change fails loudly instead of leaving the test
		// asserting an error that no longer occurs.
		require.Lenf(t, fields, 5,
			"corruption target %s must be stored non-deltified (verify-pack line: %q)", oid, line)
		size, err := strconv.ParseInt(fields[3], 10, 64)
		require.NoError(t, err)
		offset, err := strconv.ParseInt(fields[4], 10, 64)
		require.NoError(t, err)
		return strings.TrimSuffix(idxs[0], ".idx") + ".pack", offset, size
	}
	t.Fatalf("object %s not found in %s", oid, idxs[0])
	return "", 0, 0
}

// corruptPackObject flips the last byte of oid's packed representation in
// place. Detection comes from the idx CRC-32 entry covering exactly that
// span — not from zlib, whose adler32 trailer the store deliberately skips —
// so a scanner running with VerifyCRC fails deterministically on the
// object's first read. packObjectSpan pins the non-delta storage this
// relies on.
func corruptPackObject(t *testing.T, gitDir, oid string) {
	t.Helper()
	packPath, offset, size := packObjectSpan(t, gitDir, oid)

	// git writes packfiles read-only; the fixture is throwaway.
	require.NoError(t, os.Chmod(packPath, 0o644))
	f, err := os.OpenFile(packPath, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	buf := make([]byte, 1)
	_, err = f.ReadAt(buf, offset+size-1)
	require.NoError(t, err)
	buf[0] ^= 0xFF
	_, err = f.WriteAt(buf, offset+size-1)
	require.NoError(t, err)
}

// scanDedupCorrupted runs a CRC-verifying dedup scan over a fixture with a
// corrupted pack object and returns the scan error. It fails the test if
// the scan deadlocks instead of returning or if any pipeline goroutine
// outlives the call — the shutdown-chain obligations of the mid-scan error
// paths, which the fn-error test alone cannot reach.
func scanDedupCorrupted(t *testing.T, gitDir string) error {
	t.Helper()
	s, err := NewHistoryScanner(gitDir, WithHunkLineDedup(true))
	require.NoError(t, err)
	defer s.Close()
	s.SetVerifyCRC(true)

	before := runtime.NumGoroutine()
	var scanErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		scanErr = s.DiffHistoryHunksFunc(func(HunkAddition) error { return nil })
	}()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("dedup scan did not return after a mid-scan object failure (possible shutdown deadlock)")
	}
	requireGoroutinesSettle(t, before)
	require.Error(t, scanErr, "scan over a corrupted object must fail")
	return scanErr
}

// TestDiffHistoryHunksDedup_HunkStageErrorAborts corrupts a blob so the
// hunk workers' materialization of it fails mid-scan: the scan must return
// the hunk-stage error, terminate, and leak nothing. Blobs are read only by
// the hunk stage (tree diffing compares OIDs without loading blob bytes),
// so the failing stage is deterministic and the error must carry the hunk
// stage's "failed diffing" wrapper.
func TestDiffHistoryHunksDedup_HunkStageErrorAborts(t *testing.T) {
	b := newDedupRepoBuilder(t)
	b.write("clean.txt", "clean-one\n")
	b.commit("root")
	b.write("payload.txt", "payload-a\npayload-b\n")
	b.commit("add payload")
	b.write("clean.txt", "clean-one\nclean-two\n")
	b.commit("grow clean")
	blobOID := b.git("rev-parse", "HEAD:payload.txt")
	gitDir := b.finish()
	corruptPackObject(t, gitDir, blobOID)

	err := scanDedupCorrupted(t, gitDir)
	require.ErrorContains(t, err, "failed diffing",
		"a corrupted blob must surface as a hunk-stage failure")
}

// TestDiffHistoryHunksDedup_TreeStageErrorAborts corrupts a root tree so
// the tree workers' diff of its commit fails mid-scan: the scan must return
// the tree-stage error, terminate, and leak nothing. The failing worker
// records slot.err and closes stopCh, so the sequencer observes the failure
// through whichever of slot.done or stopCh its select picks — both exits
// converge on the same shutdown chain. Trees are read only by the tree
// stage (collectCommitPairs), so the error must carry its "failed
// processing commit" wrapper.
func TestDiffHistoryHunksDedup_TreeStageErrorAborts(t *testing.T) {
	b := newDedupRepoBuilder(t)
	b.write("a.txt", "one\n")
	b.commit("root")
	b.write("a.txt", "one\ntwo\n")
	b.commit("grow a")
	b.write("b.txt", "bee\n")
	b.commit("add b")
	treeOID := b.git("rev-parse", "HEAD^{tree}")
	gitDir := b.finish()
	corruptPackObject(t, gitDir, treeOID)

	err := scanDedupCorrupted(t, gitDir)
	require.ErrorContains(t, err, "failed processing commit",
		"a corrupted tree must surface as a tree-stage failure")
}
