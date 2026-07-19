// commit_walk_determinism_test.go is the correctness oracle for the parallel
// commit walk introduced in 6516fc3. walkCommitsFromRefs visits commits in
// nondeterministic order across a pool of worker goroutines; loadFromRefs then
// re-imposes a deterministic parent-first ordering. The invariant that matters
// is that the *observable* result — the ordered commit slice — is independent
// of worker scheduling.
//
// These tests build a branching/merging DAG with no commit-graph (forcing the
// ref-walk fallback) and assert:
//
//   - repeated walks on the same and fresh scanners produce byte-identical
//     ordered results (determinism under whatever scheduling occurs);
//   - a single-threaded walk (GOMAXPROCS=1) and a multi-threaded walk agree
//     (parallelism does not change the result);
//   - the completeness/shape invariants (all parents resolvable, one root)
//     hold; and
//   - DiffHistoryHunks emits a deterministic multiset of hunks across runs.

package objstore

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildMergeRepo creates a repository with a branching, merging commit DAG and
// NO commit-graph, so the scanner is forced onto the parallel ref-walk
// fallback. It returns the repo's .git directory.
func buildMergeRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repoDir := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		cmd := gitTestCommand(repoDir, args...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@e",
			"GIT_AUTHOR_DATE=2005-04-07T22:13:13 +0000",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@e",
			// A fixed committer date makes timestamps collide, which forces the
			// ordering's OID tie-breaker to do real work — exactly the code
			// most sensitive to nondeterministic visit order.
			"GIT_COMMITTER_DATE=2005-04-07T22:13:13 +0000",
		)
		out, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "git %s: %s", strings.Join(args, " "), out)
	}
	commit := func(msg string) {
		git("commit", "-q", "--allow-empty", "-m", msg)
	}

	git("init", "-q", "-b", "main")
	git("config", "commit.gpgsign", "false")
	commit("root")

	// Interleave linear commits with side branches that merge back, producing
	// multi-parent commits and a wide frontier for the parallel walk.
	for i := 0; i < 40; i++ {
		writeFile := filepath.Join(repoDir, "f.txt")
		require.NoError(t, os.WriteFile(writeFile, []byte(fmt.Sprintf("v%d\n", i)), 0o644))
		git("add", "-A")
		commit(fmt.Sprintf("main %d", i))

		if i%5 == 4 {
			branch := fmt.Sprintf("side%d", i)
			git("checkout", "-q", "-b", branch)
			require.NoError(t, os.WriteFile(
				filepath.Join(repoDir, "s.txt"), []byte(fmt.Sprintf("side %d\n", i)), 0o644))
			git("add", "-A")
			commit(fmt.Sprintf("side %d", i))
			git("checkout", "-q", "main")
			git("merge", "-q", "--no-ff", "-m", fmt.Sprintf("merge %d", i), branch)
		}
	}

	// Repack so commit-header reads exercise the pack path. We then remove any
	// commit-graph so both loadFromRefs (called directly below) and
	// DiffHistoryHunks take the parallel ref-walk fallback — the code under
	// test. Some git configurations auto-write a commit-graph, so delete it
	// defensively rather than assuming its absence.
	git("repack", "-a", "-d")

	gitDir := filepath.Join(repoDir, ".git")
	for _, name := range []string{"commit-graph", "commit-graphs"} {
		_ = os.RemoveAll(filepath.Join(gitDir, "objects", "info", name))
	}
	return gitDir
}

// canonicalCommits renders an ordered commit slice into a stable string form
// for equality comparison, capturing OID, tree, parents, and timestamp.
func canonicalCommits(commits []commitInfo) []string {
	out := make([]string, len(commits))
	for i, c := range commits {
		parents := make([]string, len(c.ParentOIDs))
		for j, p := range c.ParentOIDs {
			parents[j] = p.String()
		}
		out[i] = fmt.Sprintf("%s tree=%s ts=%d parents=[%s]",
			c.OID, c.TreeOID, c.Timestamp, strings.Join(parents, ","))
	}
	return out
}

// TestParallelCommitWalk_Deterministic asserts that the parallel ref-walk
// yields an identical ordered result across repeated runs and across worker
// scheduling, and that the DAG shape invariants hold.
func TestParallelCommitWalk_Deterministic(t *testing.T) {
	gitDir := buildMergeRepo(t)

	newScanner := func() *HistoryScanner {
		s, err := NewHistoryScanner(gitDir)
		require.NoError(t, err)
		return s
	}

	// Baseline: one walk on a fresh scanner.
	base := newScanner()
	defer base.Close()
	want, err := base.loadFromRefs()
	require.NoError(t, err)
	require.NotEmpty(t, want)
	wantCanon := canonicalCommits(want)

	// Shape invariants: exactly one root, every parent present, has merges.
	verifyDAGShape(t, want)

	// Repeated walks on the SAME scanner must be identical.
	for run := 0; run < 8; run++ {
		got, err := base.loadFromRefs()
		require.NoError(t, err)
		require.Equalf(t, wantCanon, canonicalCommits(got),
			"same-scanner run %d diverged", run)
	}

	// Fresh scanners must also be identical.
	for run := 0; run < 4; run++ {
		s := newScanner()
		got, err := s.loadFromRefs()
		s.Close()
		require.NoError(t, err)
		require.Equalf(t, wantCanon, canonicalCommits(got),
			"fresh-scanner run %d diverged", run)
	}
}

// TestParallelCommitWalk_GOMAXPROCSInvariant asserts the ordered result is the
// same whether the walk runs on a single OS thread or many. Serializing the
// scheduler is the strongest cheap probe for a result that secretly depends on
// visit order.
func TestParallelCommitWalk_GOMAXPROCSInvariant(t *testing.T) {
	gitDir := buildMergeRepo(t)

	walk := func(procs int) []string {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(procs))
		s, err := NewHistoryScanner(gitDir)
		require.NoError(t, err)
		defer s.Close()
		commits, err := s.loadFromRefs()
		require.NoError(t, err)
		return canonicalCommits(commits)
	}

	single := walk(1)
	multi := walk(runtime.NumCPU())
	require.Equal(t, single, multi, "walk result depends on parallelism")
}

// TestDiffHistoryHunks_DeterministicMultiset asserts the streaming hunk output
// is a stable multiset across runs, independent of the concurrent pipeline's
// scheduling.
func TestDiffHistoryHunks_DeterministicMultiset(t *testing.T) {
	gitDir := buildMergeRepo(t)

	collect := func() []string {
		s, err := NewHistoryScanner(gitDir)
		require.NoError(t, err)
		defer s.Close()

		hunks, errC := s.DiffHistoryHunks()
		var lines []string
		for h := range hunks {
			lines = append(lines, fmt.Sprintf("%s|%s|%d-%d|%d",
				h.commit.String(), h.Path(), h.StartLine(), h.EndLine(), len(h.Lines())))
		}
		require.NoError(t, <-errC)
		// The emission ORDER is intentionally nondeterministic; compare as a
		// sorted multiset so we test set-equality, not order.
		sort.Strings(lines)
		return lines
	}

	want := collect()
	require.NotEmpty(t, want)
	for run := 0; run < 4; run++ {
		require.Equalf(t, want, collect(), "hunk multiset diverged on run %d", run)
	}
}

// verifyDAGShape checks structural invariants that must hold regardless of walk
// order: a single root commit, all parents resolvable within the set, and at
// least one merge (multi-parent) commit so the test actually exercises the
// branching case.
func verifyDAGShape(t *testing.T, commits []commitInfo) {
	t.Helper()
	set := make(map[Hash]bool, len(commits))
	for _, c := range commits {
		set[c.OID] = true
	}
	roots, merges := 0, 0
	for _, c := range commits {
		switch len(c.ParentOIDs) {
		case 0:
			roots++
		default:
			if len(c.ParentOIDs) > 1 {
				merges++
			}
			for _, p := range c.ParentOIDs {
				require.Truef(t, set[p], "parent %s of %s missing from walk", p, c.OID)
			}
		}
	}
	require.Equal(t, 1, roots, "expected exactly one root commit")
	require.Positive(t, merges, "expected at least one merge commit to exercise branching")
}
