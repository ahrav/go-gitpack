package objstore

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"unsafe"

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

func TestDiffHistoryHunks_DirectoryRenameDoesNotPairUnrelatedReplacement(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.Mkdir(filepath.Join(repo, "old"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "replaced.txt"), []byte("shared\nold only\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "same-a.txt"), []byte("same a\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "same-b.txt"), []byte("same b\n"), 0o644))
	runGit(t, repo, "add", "old")
	runGit(t, repo, "commit", "-m", "add", "--quiet")

	require.NoError(t, os.Rename(filepath.Join(repo, "old"), filepath.Join(repo, "new")))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "new", "replaced.txt"), []byte("shared\nnew one\nnew two\nnew three\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "rename with replacement", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	hunks, errC := scanner.DiffHistoryHunks()
	var occurrences int
	for h := range hunks {
		if h.Path() != "new/replaced.txt" {
			continue
		}
		for _, line := range h.Lines() {
			if line == "shared" {
				occurrences++
			}
		}
	}
	require.NoError(t, <-errC)
	assert.Equal(t, 1, occurrences, "unrelated replacement must remain a full-file addition")
}

// buildDirectoryRenameEditRepo commits old/ with two unchanged files and
// edited.txt holding base, then renames old/ to new/ and appends tail to
// edited.txt. The unchanged files are exact-OID renames, which is the
// evidence that makes new/edited.txt a directory-rename candidate.
func buildDirectoryRenameEditRepo(t *testing.T, base, tail string) (repo string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}
	repo = t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.Mkdir(filepath.Join(repo, "old"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "edited.txt"), []byte(base), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "same-a.txt"), []byte("same a\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "same-b.txt"), []byte("same b\n"), 0o644))
	runGit(t, repo, "add", "old")
	runGit(t, repo, "commit", "-m", "add", "--quiet")

	require.NoError(t, os.Rename(filepath.Join(repo, "old"), filepath.Join(repo, "new")))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "new", "edited.txt"), []byte(base+tail), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "rename edit", "--quiet")
	return repo
}

func addedLinesByPath(t *testing.T, gitDir string, opts ...ScannerOption) map[string][]string {
	t.Helper()
	scanner, err := NewHistoryScanner(gitDir, opts...)
	require.NoError(t, err)
	defer scanner.Close()

	var mu sync.Mutex
	linesByPath := make(map[string][]string)
	require.NoError(t, scanner.DiffHistoryHunksFunc(func(h HunkAddition) error {
		mu.Lock()
		linesByPath[h.Path()] = append(linesByPath[h.Path()], h.Lines()...)
		mu.Unlock()
		return nil
	}))
	return linesByPath
}

// A directory-rename candidate larger than SmallFileThreshold still pairs
// against its old path when the content is similar, so a one-line edit
// emits one line.
func TestDiffHistoryHunks_DirectoryRenameEditLargeFilePairsAgainstOldPath(t *testing.T) {
	var base strings.Builder
	for i := 0; base.Len() <= SmallFileThreshold; i++ {
		fmt.Fprintf(&base, "stable line %06d of a large generated file\n", i)
	}
	repo := buildDirectoryRenameEditRepo(t, base.String(), "secret\n")

	for _, dedup := range []bool{false, true} {
		linesByPath := addedLinesByPath(t, filepath.Join(repo, ".git"), WithHunkLineDedup(dedup))
		assert.Equalf(t, []string{"secret"}, linesByPath["new/edited.txt"], "dedup=%v", dedup)
	}
}

// The path filter drops a directory-rename candidate before either blob is
// read, so a filtered candidate whose old blob is unreadable leaves the
// scan unaffected.
func TestDiffHistoryHunks_PathFilteredRenameCandidateSkipsBlobLoads(t *testing.T) {
	repo := buildDirectoryRenameEditRepo(t, "stable\n", "secret\n")
	out, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD~1:old/edited.txt").Output()
	require.NoError(t, err)
	oldBlob := strings.TrimSpace(string(out))
	gitDir := filepath.Join(repo, ".git")
	require.NoError(t, os.Remove(filepath.Join(gitDir, "objects", oldBlob[:2], oldBlob[2:])))

	skipEdited := func(_ Hash, path string) bool { return strings.HasSuffix(path, "/edited.txt") }
	for _, dedup := range []bool{false, true} {
		linesByPath := addedLinesByPath(t, gitDir, WithHunkLineDedup(dedup), WithHunkPathFilter(skipEdited))
		assert.Emptyf(t, linesByPath["new/edited.txt"], "dedup=%v", dedup)
		assert.Equalf(t, []string{"same a"}, linesByPath["old/same-a.txt"], "dedup=%v", dedup)
	}
}

func TestRenameLinesSimilar(t *testing.T) {
	cases := []struct {
		name     string
		old, new string
		want     bool
	}{
		{"identical", "a\nb\n", "a\nb\n", true},
		{"quarter of the longer side shared", "a\nb\n", "a\nc\nd\ne\n", false},
		{"exactly half shared", "a\nb\n", "a\nb\nc\nd\n", true},
		{"duplicates count once each", "a\n", "a\na\na\n", false},
		{"binary old side", "a\x00\nb\n", "a\nb\n", false},
		{"empty old side", "", "a\n", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, renameLinesSimilar([]byte(c.old), []byte(c.new)))
		})
	}
}

func TestBlobPairWorkSize(t *testing.T) {
	assert.LessOrEqual(t, unsafe.Sizeof(blobPairWork{}), uintptr(80))
}

func TestInferDirectoryRenames_TiedCandidatesDeterministic(t *testing.T) {
	evidence := []exactRenameEvidence{
		{oldPath: "old-a/x.go", newPath: "merged/x.go"},
		{oldPath: "old-a/y.go", newPath: "merged/y.go"},
		{oldPath: "old-b/p.go", newPath: "merged/p.go"},
		{oldPath: "old-b/q.go", newPath: "merged/q.go"},
	}
	first := inferDirectoryRenames(evidence)
	require.Len(t, first, 2)
	for run := range 64 {
		require.Equalf(t, first, inferDirectoryRenames(evidence), "run %d", run)
	}
}
