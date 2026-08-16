// tree_worker_bench_test.go measures the DiffHistoryHunks pipeline on a
// history shaped to bind on stage 1 (tree diffing + parent-header inflation):
// thousands of commits that each change one small file in a moderately wide
// tree. Stage-2 hunk work per commit is trivial, so pipeline throughput
// tracks the tree-worker stage — the stage capped by maxTreeDiffWorkers.
//
// This is the harness behind the measured choice of maxTreeDiffWorkers; see
// the constant's comment in history_scanner.go.
package objstore

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// buildManySmallCommitsRepo creates a bare repository with numCommits commits
// via one git fast-import stream: commit 1 adds numFiles small files, every
// later commit rewrites a single file. Returns the bare repo directory.
func buildManySmallCommitsRepo(tb testing.TB, dir string, numCommits, numFiles int) string {
	tb.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		tb.Skip("git executable not found in PATH")
	}

	repoDir := filepath.Join(dir, fmt.Sprintf("small-commits-%d-%d.git", numCommits, numFiles))
	if _, err := os.Stat(repoDir); err == nil {
		return repoDir // Fixture already built by an earlier sub-benchmark.
	}
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		tb.Fatalf("create fixture dir: %v", err)
	}
	runGit(tb, repoDir, "init", "--bare", "--quiet")

	var stream strings.Builder
	mark := 0
	blob := func(content string) int {
		mark++
		fmt.Fprintf(&stream, "blob\nmark :%d\ndata %d\n%s\n", mark, len(content), content)
		return mark
	}
	commit := func(n int, fileMarks map[int]int) {
		mark++
		fmt.Fprintf(&stream, "commit refs/heads/main\nmark :%d\n", mark)
		fmt.Fprintf(&stream, "author t <t@e> %d +0000\n", 1112911993+n)
		fmt.Fprintf(&stream, "committer t <t@e> %d +0000\n", 1112911993+n)
		msg := fmt.Sprintf("commit %d", n)
		fmt.Fprintf(&stream, "data %d\n%s\n", len(msg), msg)
		for f, m := range fileMarks {
			fmt.Fprintf(&stream, "M 100644 :%d dir-%d/file-%d.txt\n", m, f%8, f)
		}
		stream.WriteByte('\n')
	}

	root := make(map[int]int, numFiles)
	for f := 0; f < numFiles; f++ {
		root[f] = blob(fmt.Sprintf("file %d line one\nfile %d line two\n", f, f))
	}
	commit(0, root)
	for n := 1; n < numCommits; n++ {
		f := n % numFiles
		m := blob(fmt.Sprintf("file %d rewritten at commit %d\nsecond line %d\n", f, n, n))
		commit(n, map[int]int{f: m})
	}

	cmd := gitTestCommand(repoDir, "fast-import", "--quiet")
	cmd.Env = gitFixtureEnv()
	cmd.Stdin = strings.NewReader(stream.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("git fast-import: %v: %s", err, out)
	}
	// Force the parallel ref-walk fallback (the code under test) even if a
	// git config writes commit graphs.
	for _, name := range []string{"commit-graph", "commit-graphs"} {
		_ = os.RemoveAll(filepath.Join(repoDir, "objects", "info", name))
	}
	return repoDir
}

// BenchmarkDiffHistoryHunksManySmallCommits drains the full pipeline over a
// stage-1-bound history. Hunk output is tiny by construction, so this number
// moves with tree-worker throughput rather than blob-diff throughput.
func BenchmarkDiffHistoryHunksManySmallCommits(b *testing.B) {
	gitDir := buildManySmallCommitsRepo(b, b.TempDir(), 3000, 200)
	b.ReportAllocs()
	for b.Loop() {
		scanner, err := NewHistoryScanner(gitDir)
		if err != nil {
			b.Fatal(err)
		}
		hunks, errC := scanner.DiffHistoryHunks()
		count := 0
		for range hunks {
			count++
		}
		if err := <-errC; err != nil {
			b.Fatal(err)
		}
		if count == 0 {
			b.Fatal("expected hunks from fixture")
		}
		if err := scanner.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
