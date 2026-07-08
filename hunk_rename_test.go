package objstore

import (
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
