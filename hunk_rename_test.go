package objstore

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiffHistoryHunks_ExactOIDRenameDoesNotEmitAddition(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old.txt"), []byte("same bytes\n"), 0o644))
	runGit(t, repo, "add", "old.txt")
	runGit(t, repo, "commit", "-m", "add", "--quiet")
	runGit(t, repo, "mv", "old.txt", "new.txt")
	runGit(t, repo, "commit", "-m", "rename", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	hunks, errC := scanner.DiffHistoryHunks()
	paths := make(map[string]int)
	for h := range hunks {
		paths[h.Path()]++
	}
	require.NoError(t, <-errC)
	assert.Equal(t, 1, paths["old.txt"], "root add should still be reported")
	assert.Zero(t, paths["new.txt"], "exact-OID rename should not become a full-file addition")
}

func TestDiffHistoryHunks_DirectoryRenameEditPairsAgainstOldPath(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.Mkdir(filepath.Join(repo, "old"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "edited.txt"), []byte("stable\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "same-a.txt"), []byte("same a\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "same-b.txt"), []byte("same b\n"), 0o644))
	runGit(t, repo, "add", "old")
	runGit(t, repo, "commit", "-m", "add", "--quiet")

	require.NoError(t, os.Rename(filepath.Join(repo, "old"), filepath.Join(repo, "new")))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "new", "edited.txt"), []byte("stable\nsecret\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "rename edit", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	hunks, errC := scanner.DiffHistoryHunks()
	linesByPath := make(map[string][]string)
	for h := range hunks {
		linesByPath[h.Path()] = append(linesByPath[h.Path()], h.Lines()...)
	}
	require.NoError(t, <-errC)

	assert.Equal(t, []string{"secret"}, linesByPath["new/edited.txt"])
	assert.Empty(t, linesByPath["new/same-a.txt"])
	assert.Empty(t, linesByPath["new/same-b.txt"])
}

// TestInferDirectoryRenames_DeterministicOrderOnTies pins the candidate
// ordering to a total order. Two directory pairs can tie on both evidence
// count and len(newDir) — for example when two source dirs collapse into one
// target dir — and matchDirectoryRename is first-match-wins over this slice,
// so any tie left to map iteration order makes the chosen delete (and the
// emitted hunks) differ between runs, violating the determinism DiffHistoryHunks
// documents.
func TestInferDirectoryRenames_DeterministicOrderOnTies(t *testing.T) {
	evidence := []exactRenameEvidence{
		{oldPath: "a/f1.txt", newPath: "x/f1.txt"},
		{oldPath: "a/f2.txt", newPath: "x/f2.txt"},
		{oldPath: "b/g1.txt", newPath: "x/g1.txt"},
		{oldPath: "b/g2.txt", newPath: "x/g2.txt"},
	}
	want := []directoryRenameCandidate{
		{oldDir: "a", newDir: "x", count: 2},
		{oldDir: "b", newDir: "x", count: 2},
	}
	// Map iteration order is randomized per call, so repeat enough times that
	// an order-dependent comparator cannot pass by chance.
	for run := 0; run < 64; run++ {
		require.Equalf(t, want, inferDirectoryRenames(evidence).ordered,
			"candidate order diverged on run %d", run)
	}
}

func TestInferDirectoryRenames_CandidateLimitFallsBack(t *testing.T) {
	evidence := make([]exactRenameEvidence, 0, 2*(maxDirectoryRenameCandidates+1))
	for dir := 0; dir <= maxDirectoryRenameCandidates; dir++ {
		for file := 0; file < minDirectoryRenameEvidence; file++ {
			name := fmt.Sprintf("anchor-%d.txt", file)
			evidence = append(evidence, exactRenameEvidence{
				oldPath: fmt.Sprintf("old-%d/%s", dir, name),
				newPath: fmt.Sprintf("new-%d/%s", dir, name),
			})
		}
	}

	assert.Empty(t, inferDirectoryRenames(evidence).ordered)
}

// TestDiffHistoryHunks_DirectoryRenameDoesNotPairUnrelatedContent asserts that
// directory-rename inference never pairs an add with a deleted file whose
// content is unrelated. Git only pairs renames at >= 50% similarity; pairing
// below that silently drops any coincidentally-shared lines from the
// added-hunk stream, under-reporting genuinely new content.
func TestDiffHistoryHunks_DirectoryRenameDoesNotPairUnrelatedContent(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.Mkdir(filepath.Join(repo, "old"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "notes.txt"),
		[]byte("alpha\nshared-line\nbeta\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "same-a.txt"), []byte("same a\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "same-b.txt"), []byte("same b\n"), 0o644))
	runGit(t, repo, "add", "old")
	runGit(t, repo, "commit", "-m", "add", "--quiet")

	// Rename old/ -> new/ (two exact-OID renames establish the directory
	// rename), but replace notes.txt with unrelated content that happens to
	// share one line with the deleted file.
	require.NoError(t, os.Rename(filepath.Join(repo, "old"), filepath.Join(repo, "new")))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "new", "notes.txt"),
		[]byte("one\ntwo\nshared-line\nthree\nfour\nfive\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "rename dir, replace notes", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	hunks, errC := scanner.DiffHistoryHunks()
	linesByPath := make(map[string][]string)
	for h := range hunks {
		linesByPath[h.Path()] = append(linesByPath[h.Path()], h.Lines()...)
	}
	require.NoError(t, <-errC)

	// The replacement file is < 50% similar to the deleted one, so every one
	// of its lines — including the coincidentally-shared line — is new.
	assert.ElementsMatch(t,
		[]string{"one", "two", "shared-line", "three", "four", "five"},
		linesByPath["new/notes.txt"],
		"unrelated replacement must be reported as a whole-file addition")
	assert.Empty(t, linesByPath["new/same-a.txt"])
	assert.Empty(t, linesByPath["new/same-b.txt"])
}
