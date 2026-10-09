// diff_bench_churn_test.go benchmarks the diff pipeline against workloads
// that exercise the lazy-blob-load and common-prefix-skip mechanisms with
// representative inputs.
//
// BenchmarkAddedHunksRealistic drives addedHunksWithPos directly with
// synthetic-but-realistic line distributions: mostly-unique source lines mixed
// with high-frequency duplicates ("", "}", "\treturn nil"), a huge shared
// prefix with tail-only edits, and an adversarial file that cycles a tiny
// line vocabulary to stress duplicate-chain walks in the line index.
//
// BenchmarkDiffHistoryHunksBinaryChurn is a whole-scan macro benchmark over a
// generated fixture repository whose history contains binary blob rewrites,
// a pure deletion, a re-add, a mode-only chmod commit, and tail-edits to a
// large text file with a stable header. These are exactly the diff shapes
// where lazy blob loading can skip reading one or both sides entirely.
//
// This file is deliberately self-contained (its own repo builder and scanner
// helper, no symbols beyond the stable public/internal API shared across
// commits) so that the identical file can be compiled at older baseline
// commits for A/B measurement.

package objstore

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// benchChurnSink is a package-level sink preventing the compiler from
// eliminating benchmarked calls as dead code.
var benchChurnSink int

// churnBenchInputs holds the lazily-built (old, new) byte-slice pairs for the
// BenchmarkAddedHunksRealistic sub-benchmarks. Construction is deterministic
// (no RNG) and happens once via churnBenchOnce, outside any timed region.
type churnBenchInputs struct {
	realisticOld, realisticNew []byte
	prefixOld, prefixNew       []byte
	adversOld, adversNew       []byte
}

var (
	churnBenchOnce sync.Once
	churnBench     churnBenchInputs
)

// churnFillerLines is the small vocabulary of high-frequency duplicate lines
// interspersed into the synthetic source files, mimicking the blank lines and
// closing braces that dominate real code.
var churnFillerLines = [...]string{"", "}", "\treturn nil", "\t}"}

// makeRealisticSourceLines returns n synthetic source lines: roughly 80%
// unique statements and 20% high-frequency duplicates at deterministic
// positions.
func makeRealisticSourceLines(n int) []string {
	lines := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if i%5 == 4 {
			// Every fifth line is a duplicate drawn from the tiny vocabulary.
			lines = append(lines, churnFillerLines[(i/5)%len(churnFillerLines)])
			continue
		}
		lines = append(lines, fmt.Sprintf("\tresult_%d := compute(input_%d, %d)", i, i*7, i%13))
	}
	return lines
}

// joinLines materializes a line slice into the []byte form addedHunksWithPos
// consumes.
func joinLines(lines []string) []byte {
	return []byte(strings.Join(lines, "\n"))
}

// buildChurnBenchInputs constructs all three (old, new) input pairs. All edits
// use fixed strides/offsets so repeated runs produce byte-identical inputs.
func buildChurnBenchInputs() {
	// realistic_edit: ~4,000-line file, ~2% of interior lines replaced at
	// scattered positions, plus ~40 lines inserted in the middle third.
	{
		const n = 4000
		oldLines := makeRealisticSourceLines(n)

		newLines := make([]string, 0, n+40)
		newLines = append(newLines, oldLines...)
		// Replace ~2% of lines (every 53rd line, skipping the file edges).
		for i := 101; i < n-100; i += 53 {
			newLines[i] = fmt.Sprintf("\tresult_%d := computeV2(input_%d, %d) // edited", i, i*11, i%17)
		}
		// Insert ~40 lines in the middle third.
		insertAt := n / 2
		inserted := make([]string, 0, 40)
		for i := 0; i < 40; i++ {
			inserted = append(inserted, fmt.Sprintf("\tinjected_%d := helper(%d)", i, i*3))
		}
		withInsert := make([]string, 0, len(newLines)+len(inserted))
		withInsert = append(withInsert, newLines[:insertAt]...)
		withInsert = append(withInsert, inserted...)
		withInsert = append(withInsert, newLines[insertAt:]...)

		churnBench.realisticOld = joinLines(oldLines)
		churnBench.realisticNew = joinLines(withInsert)
	}

	// large_common_prefix: ~20,000 lines, identical except the last ~100
	// lines are replaced and ~20 lines appended.
	{
		const n = 20000
		oldLines := makeRealisticSourceLines(n)

		newLines := make([]string, n, n+20)
		copy(newLines, oldLines)
		for i := n - 100; i < n; i++ {
			newLines[i] = fmt.Sprintf("\ttail_%d := rewritten(%d)", i, i*5)
		}
		for i := 0; i < 20; i++ {
			newLines = append(newLines, fmt.Sprintf("\tappended_%d := extend(%d)", i, i))
		}

		churnBench.prefixOld = joinLines(oldLines)
		churnBench.prefixNew = joinLines(newLines)
	}

	// repeated_lines_adversarial: ~120,000 lines cycling a 4-line vocabulary
	// (~0.5 MiB). The new side prepends one unique line (defeating any
	// prefix skip) and inserts ~50 unique lines at scattered positions, so
	// every duplicate line's occurrence chain must be walked.
	{
		const n = 120000
		vocab := [...]string{"", "}", "\t}", "end"}
		oldLines := make([]string, 0, n)
		for i := 0; i < n; i++ {
			oldLines = append(oldLines, vocab[i%len(vocab)])
		}

		newLines := make([]string, 0, n+51)
		newLines = append(newLines, "// unique prefix defeating common-prefix skip")
		stride := n / 50
		for i := 0; i < n; i++ {
			if i > 0 && i%stride == 0 {
				newLines = append(newLines, fmt.Sprintf("unique_inserted_line_%d", i))
			}
			newLines = append(newLines, oldLines[i])
		}

		churnBench.adversOld = joinLines(oldLines)
		churnBench.adversNew = joinLines(newLines)
	}
}

// BenchmarkAddedHunksRealistic measures addedHunksWithPos on line
// distributions shaped like real source files rather than fully-unique or
// fully-identical lines. The sub-benchmarks respectively exercise the line
// index with realistic duplicate chains, the common-prefix skip with a lazy
// index build, and the duplicate-chain walk worst case.
func BenchmarkAddedHunksRealistic(b *testing.B) {
	churnBenchOnce.Do(buildChurnBenchInputs)

	cases := []struct {
		name     string
		old, new []byte
		// wantHunks reports whether the diff must produce at least one hunk.
		wantHunks bool
	}{
		{"realistic_edit", churnBench.realisticOld, churnBench.realisticNew, true},
		{"large_common_prefix", churnBench.prefixOld, churnBench.prefixNew, true},
		{"repeated_lines_adversarial", churnBench.adversOld, churnBench.adversNew, true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			// Sanity-check the result once, outside the timed loop, so the
			// benchmarked call cannot be considered dead code.
			warm := addedHunksWithPos(tc.old, tc.new)
			if tc.wantHunks && len(warm) == 0 {
				b.Fatalf("expected at least one added hunk, got %d", len(warm))
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(tc.old) + len(tc.new)))
			for b.Loop() {
				hunks := addedHunksWithPos(tc.old, tc.new)
				benchChurnSink += len(hunks)
			}
		})
	}
}

// churnGit runs one git command in dir with a hermetic, deterministic
// configuration (fixed identity, no GPG signing, fixed timestamps) and fails
// the benchmark on error.
func churnGit(b *testing.B, dir string, args ...string) {
	b.Helper()
	if out, err := churnGitOutput(dir, args...); err != nil {
		b.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// churnGitErr is like churnGit but returns the error instead of failing,
// letting callers fall back when a flag is unsupported by the local git.
func churnGitErr(dir string, args ...string) error {
	_, err := churnGitOutput(dir, args...)
	return err
}

// churnGitOutput executes the git command and returns its combined output.
func churnGitOutput(dir string, args ...string) ([]byte, error) {
	base := []string{
		"-c", "user.name=bench",
		"-c", "user.email=bench@example.com",
		"-c", "commit.gpgsign=false",
		"-c", "core.autocrlf=false",
	}
	cmd := exec.Command("git", append(base, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_DATE=2021-01-01T00:00:00+00:00",
		"GIT_COMMITTER_DATE=2021-01-01T00:00:00+00:00",
	)
	return cmd.CombinedOutput()
}

// churnWriteFile writes a fixture file, creating parent directories as
// needed.
func churnWriteFile(b *testing.B, path string, data []byte) {
	b.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		b.Fatal(err)
	}
}

// churnBinaryBlob returns a 2 MiB deterministic binary payload for the given
// generation. Each generation yields a distinct byte pattern, and every
// 256-byte block contains a NUL byte so binary detection (NUL within the
// first 8 KiB) always triggers.
func churnBinaryBlob(generation int) []byte {
	const size = 2 << 20
	block := make([]byte, 256)
	for i := range block {
		block[i] = byte((i*7 + generation*31) % 251)
	}
	block[0] = 0x00 // Guarantee a NUL in the first 256 bytes.
	buf := make([]byte, 0, size)
	for len(buf) < size {
		buf = append(buf, block...)
	}
	return buf[:size]
}

// churnMainText returns the ~200 KiB src/main.txt payload for the given
// revision. The first ~150 KiB header is identical across revisions; only
// lines near the tail change, so tree-level diffs of consecutive revisions
// share a large common prefix.
func churnMainText(rev int) []byte {
	var sb strings.Builder
	sb.Grow(210 << 10)
	// Stable ~150 KiB header: ~3,000 lines of ~50 bytes.
	for i := 0; i < 3000; i++ {
		fmt.Fprintf(&sb, "header line %06d: stable content padding %08d\n", i, i*13)
	}
	// Volatile tail: ~1,000 lines, a handful rewritten per revision plus a
	// few appended lines per revision.
	for i := 0; i < 1000; i++ {
		if i%97 == rev%97 {
			fmt.Fprintf(&sb, "tail line %06d: edited in revision %03d\n", i, rev)
			continue
		}
		fmt.Fprintf(&sb, "tail line %06d: original content %08d\n", i, i*7)
	}
	for i := 0; i < rev; i++ {
		fmt.Fprintf(&sb, "appended by revision %03d entry %03d\n", i, i*3)
	}
	return []byte(sb.String())
}

// buildChurnRepo generates the fixture repository under dir and returns the
// path to its .git directory. The history (~28 commits) contains:
//
//   - an initial 2 MiB binary blob with NUL bytes in the first 100 bytes,
//   - five commits rewriting that blob with distinct 2 MiB patterns
//     (binary-to-binary transitions),
//   - one commit deleting the blob (pure deletion),
//   - one commit re-adding it,
//   - one commit that only flips the executable bit on scripts/tool.sh
//     (mode-only change, identical content),
//   - fifteen commits editing only the tail of a ~200 KiB text file with a
//     stable ~150 KiB header,
//   - three small text files churning normally.
//
// All objects are repacked into a single packfile and a commit-graph is
// written, matching the bare-fixture layout the scanner expects.
func buildChurnRepo(b *testing.B, dir string) string {
	b.Helper()

	churnGit(b, dir, "init", "-q", "-b", "main")

	commit := func(msg string) {
		churnGit(b, dir, "add", "-A")
		churnGit(b, dir, "commit", "-q", "-m", msg)
	}

	blobPath := filepath.Join(dir, "assets", "blob.bin")
	mainPath := filepath.Join(dir, "src", "main.txt")
	toolPath := filepath.Join(dir, "scripts", "tool.sh")

	// (a) Initial commit: binary blob + script + text files.
	churnWriteFile(b, blobPath, churnBinaryBlob(0))
	churnWriteFile(b, toolPath, []byte("#!/bin/sh\necho tool\n"))
	churnWriteFile(b, mainPath, churnMainText(0))
	churnWriteFile(b, filepath.Join(dir, "notes.txt"), []byte("notes rev 0\n"))
	churnWriteFile(b, filepath.Join(dir, "config.txt"), []byte("config rev 0\n"))
	commit("initial import")

	// (b) Five binary-to-binary rewrites of blob.bin.
	for gen := 1; gen <= 5; gen++ {
		churnWriteFile(b, blobPath, churnBinaryBlob(gen))
		commit(fmt.Sprintf("rewrite blob.bin generation %d", gen))
	}

	// (c) Pure deletion of blob.bin.
	churnGit(b, dir, "rm", "-q", "assets/blob.bin")
	commit("delete blob.bin")

	// (d) Re-add blob.bin.
	churnWriteFile(b, blobPath, churnBinaryBlob(6))
	commit("re-add blob.bin")

	// (e) Mode-only change: chmod +x with identical content. The mode is
	// flipped both on disk and in the index (update-index --chmod is
	// filesystem-independent), and the commit is made without re-adding the
	// worktree so the resulting diff is exactly 100644 -> 100755 on the same
	// blob OID.
	if err := os.Chmod(toolPath, 0o755); err != nil {
		b.Fatal(err)
	}
	churnGit(b, dir, "update-index", "--chmod=+x", "scripts/tool.sh")
	churnGit(b, dir, "commit", "-q", "-m", "make tool.sh executable")

	// (f) Fifteen tail-only edits of src/main.txt.
	for rev := 1; rev <= 15; rev++ {
		churnWriteFile(b, mainPath, churnMainText(rev))
		if rev%5 == 0 {
			// (g) Small text files churn alongside for realism.
			churnWriteFile(b, filepath.Join(dir, "notes.txt"), []byte(fmt.Sprintf("notes rev %d\nmore notes\n", rev)))
			churnWriteFile(b, filepath.Join(dir, "config.txt"), []byte(fmt.Sprintf("config rev %d\nkey = value%d\n", rev, rev)))
		}
		commit(fmt.Sprintf("edit main.txt tail revision %d", rev))
	}

	// The scanner reads packfiles only, so everything must be packed.
	// Older gits lack repack -q; fall back to the non-quiet form.
	if churnGitErr(dir, "repack", "-adq") != nil {
		churnGit(b, dir, "repack", "-ad")
	}
	churnGit(b, dir, "commit-graph", "write", "--reachable")

	return filepath.Join(dir, ".git")
}

// BenchmarkDiffHistoryHunksBinaryChurn measures a full DiffHistoryHunks scan
// over the generated churn repository. The history's binary rewrites, pure
// deletion, mode-only change, and stable-header text edits are the diff
// shapes where lazy blob loading skips reading one or both blob sides, so
// this benchmark makes those skips visible end to end.
func BenchmarkDiffHistoryHunksBinaryChurn(b *testing.B) {
	if _, err := exec.LookPath("git"); err != nil {
		b.Skip("git binary not available; skipping fixture-repo benchmark")
	}

	// Repository construction and scanner setup happen before b.Loop's first
	// call and are therefore excluded from the timed region.
	gitDir := buildChurnRepo(b, b.TempDir())

	scanner, err := NewHistoryScanner(gitDir)
	if err != nil {
		b.Fatalf("open scanner: %v", err)
	}
	defer scanner.Close()

	b.ReportAllocs()

	count := 0
	for b.Loop() {
		count = 0
		hunks, errC := scanner.DiffHistoryHunks()
		for range hunks {
			count++
		}
		if err := <-errC; err != nil {
			b.Fatalf("DiffHistoryHunks: %v", err)
		}
	}

	if count == 0 {
		b.Fatal("expected at least one hunk from the churn repository scan")
	}
	benchChurnSink += count
}
