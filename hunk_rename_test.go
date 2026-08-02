package objstore

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

// buildInferredRenameRepo creates a repo whose second commit renames old/ ->
// new/ with two exact-OID renames — enough evidence for directory-rename
// inference — while replacing old/data with newData at new/data. The add at
// new/data is therefore paired against the deleted old/data blob purely from
// path structure, which is the guess gateInferredRenameHunks must validate.
func buildInferredRenameRepo(t *testing.T, oldData, newData []byte) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.Mkdir(filepath.Join(repo, "old"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "data"), oldData, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "same-a.txt"), []byte("same a\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "same-b.txt"), []byte("same b\n"), 0o644))
	runGit(t, repo, "add", "old")
	runGit(t, repo, "commit", "-m", "add", "--quiet")

	require.NoError(t, os.Rename(filepath.Join(repo, "old"), filepath.Join(repo, "new")))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "new", "data"), newData, 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "rename dir, replace data", "--quiet")

	return filepath.Join(repo, ".git")
}

// scanHunksByPath drains DiffHistoryHunks into per-path lines and per-path
// binary flags.
func scanHunksByPath(t *testing.T, gitDir string) (map[string][]string, map[string]bool) {
	t.Helper()
	scanner, err := NewHistoryScanner(gitDir)
	require.NoError(t, err)
	defer scanner.Close()

	hunks, errC := scanner.DiffHistoryHunks()
	linesByPath := make(map[string][]string)
	binaryByPath := make(map[string]bool)
	for h := range hunks {
		linesByPath[h.Path()] = append(linesByPath[h.Path()], h.Lines()...)
		if h.IsBinary() {
			binaryByPath[h.Path()] = true
		}
	}
	require.NoError(t, <-errC)
	return linesByPath, binaryByPath
}

// TestDiffHistoryHunks_InferredRenameFromBinaryOldBlobReportsTextLines covers
// the binary escape hatch in gateInferredRenameHunks. When the guessed old
// blob is binary, computeAddedHunks reports the whole new file as one binary
// hunk — a shape decided entirely by the old side. Accepting that on an
// inferred pairing loses the new text file's line structure, and any consumer
// that skips binary hunks loses the file's content outright.
func TestDiffHistoryHunks_InferredRenameFromBinaryOldBlobReportsTextLines(t *testing.T) {
	// NUL bytes make isBinary report true for the deleted blob.
	oldData := []byte("\x00\x01\x02binary payload\x00")
	gitDir := buildInferredRenameRepo(t, oldData, []byte("one\ntwo\nthree\n"))

	linesByPath, binaryByPath := scanHunksByPath(t, gitDir)

	assert.False(t, binaryByPath["new/data"],
		"a text file must not be reported as binary because its guessed predecessor was binary")
	assert.ElementsMatch(t, []string{"one", "two", "three"}, linesByPath["new/data"],
		"new text file must be reported as a whole-file addition")
}

// TestDiffHistoryHunks_InferredRenameFromOversizedOldBlobReportsTextLines
// covers the oversized escape hatch in gateInferredRenameHunks. When only the
// guessed old blob exceeds the diff limit, computeAddedHunks returns its
// "[File too large to diff]" placeholder. That placeholder carries one line,
// which satisfies the >= 50%-common similarity test for any new file with two
// or more lines, so the pairing is kept and the placeholder becomes the file's
// only output — the new content never reaches the stream.
func TestDiffHistoryHunks_InferredRenameFromOversizedOldBlobReportsTextLines(t *testing.T) {
	// Shrink the limit rather than materialize a gigabyte. Safe because this
	// test is serial; see the maxDiffSize doc comment.
	restore := maxDiffSize
	maxDiffSize = 64
	t.Cleanup(func() { maxDiffSize = restore })

	oldData := []byte(strings.Repeat("filler line\n", 20)) // 240 bytes > 64
	newData := []byte("one\ntwo\nthree\n")                 // 14 bytes <= 64
	gitDir := buildInferredRenameRepo(t, oldData, newData)

	linesByPath, _ := scanHunksByPath(t, gitDir)

	assert.ElementsMatch(t, []string{"one", "two", "three"}, linesByPath["new/data"],
		"new content must be reported even when the guessed predecessor is too large to diff")
	for _, line := range linesByPath["new/data"] {
		assert.NotContains(t, line, "File too large to diff",
			"an inferred pairing must not substitute a placeholder for the new file's lines")
	}
}
