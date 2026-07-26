package objstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiffHistoryHunks_ExactOIDRenameDoesNotEmitAddition(t *testing.T) {
	requireGit(t)

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

	root, rename := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	require.Len(t, got, 1)
	requireHunkLines(t, got[hunkAttribution{commit: root, path: "old.txt"}], "same bytes")
	require.Empty(t, got[hunkAttribution{commit: rename, path: "new.txt"}],
		"exact-OID rename should not become a full-file addition")
}

func TestDiffHistoryHunks_ExactOIDDeletionSuppressesOnlyOneAddition(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	content := []byte("same bytes\n")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "source.txt"), content, 0o644))
	runGit(t, repo, "add", "source.txt")
	runGit(t, repo, "commit", "-m", "add source", "--quiet")

	require.NoError(t, os.Remove(filepath.Join(repo, "source.txt")))
	for _, path := range []string{"copy-a.txt", "copy-b.txt"} {
		require.NoError(t, os.WriteFile(filepath.Join(repo, path), content, 0o644))
	}
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "rename and copy", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, renameAndCopy := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "source.txt"}], "same bytes")
	require.Empty(t, got[hunkAttribution{commit: renameAndCopy, path: "source.txt"}])

	survivors := 0
	for _, path := range []string{"copy-a.txt", "copy-b.txt"} {
		hunks := got[hunkAttribution{commit: renameAndCopy, path: path}]
		if len(hunks) == 0 {
			continue
		}
		requireHunkLines(t, hunks, "same bytes")
		survivors++
	}
	require.Equal(t, 1, survivors, "one deletion must suppress exactly one same-OID addition")
	require.Len(t, got, 2, "expected the root addition and one surviving copy addition")
}

func TestDiffHistoryHunks_DeletionOnlyDoesNotEmitHunk(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	path := filepath.Join(repo, "gone.txt")
	require.NoError(t, os.WriteFile(path, []byte("removed later\n"), 0o644))
	runGit(t, repo, "add", "gone.txt")
	runGit(t, repo, "commit", "-m", "add", "--quiet")
	require.NoError(t, os.Remove(path))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "delete", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, deletion := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	require.Len(t, got, 1)
	requireHunkLines(t, got[hunkAttribution{commit: root, path: "gone.txt"}], "removed later")
	require.Empty(t, got[hunkAttribution{commit: deletion, path: "gone.txt"}],
		"a deletion-only commit has no added hunk")
}

func TestDiffHistoryHunks_ModificationStillEmitsAddedLine(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	path := filepath.Join(repo, "modified.txt")
	require.NoError(t, os.WriteFile(path, []byte("existing line\n"), 0o644))
	runGit(t, repo, "add", "modified.txt")
	runGit(t, repo, "commit", "-m", "add", "--quiet")
	require.NoError(t, os.WriteFile(path, []byte("existing line\nadded line\n"), 0o644))
	runGit(t, repo, "add", "modified.txt")
	runGit(t, repo, "commit", "-m", "modify", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, modification := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	require.Len(t, got, 2)
	requireHunkLines(t, got[hunkAttribution{commit: root, path: "modified.txt"}], "existing line")
	requireHunkLines(t, got[hunkAttribution{commit: modification, path: "modified.txt"}], "added line")
}

// A move onto an already-tracked path is reported by walkDiff as a modification
// of the destination, not as an addition: the destination's old OID is its prior
// content and its new OID is the moved blob. Suppression must still fire, because
// every line the destination gains is a line of the deleted blob.
func TestDiffHistoryHunks_ExactOIDMoveOntoTrackedPathIsSuppressed(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old.txt"), []byte("moved bytes\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "new.txt"), []byte("original dest\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add source and destination", "--quiet")

	runGit(t, repo, "mv", "-f", "old.txt", "new.txt")
	runGit(t, repo, "commit", "-m", "overwriting move", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, move := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "old.txt"}], "moved bytes")
	requireHunkLines(t, got[hunkAttribution{commit: root, path: "new.txt"}], "original dest")
	require.Empty(t, got[hunkAttribution{commit: move, path: "new.txt"}],
		"a move onto a tracked path re-adds bytes the history already carries")
	require.Len(t, got, 2, "expected only the two root additions")
}

// A regular file whose bytes are exactly a path string and a symlink pointing at
// that path share one blob OID. Suppression keys on OID plus entry type, so the
// deleted file must not silence the symlink that replaces it.
func TestDiffHistoryHunks_SameOIDAcrossEntryTypesIsNotSuppressed(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	// No trailing newline: the blob is exactly the symlink target string, so
	// both entries hash to the same OID.
	const target = "target/path"
	require.NoError(t, os.WriteFile(filepath.Join(repo, "payload.txt"), []byte(target), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add regular file", "--quiet")

	require.NoError(t, os.Remove(filepath.Join(repo, "payload.txt")))
	require.NoError(t, os.Symlink(target, filepath.Join(repo, "link")))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "replace the file with a symlink", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, swap := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "payload.txt"}], target)
	requireHunkLines(t, got[hunkAttribution{commit: swap, path: "link"}], target)
	require.Len(t, got, 2, "a blob-to-symlink type change is not an exact-OID move")
}

// Permission bits are not blob content, so flipping the exec bit across an
// otherwise byte-identical move must not defeat suppression.
func TestDiffHistoryHunks_ExactOIDMoveSuppressedAcrossPermissionChange(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old.sh"), []byte("moved bytes\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add", "--quiet")

	runGit(t, repo, "mv", "old.sh", "new.sh")
	require.NoError(t, os.Chmod(filepath.Join(repo, "new.sh"), 0o755))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "move and make executable", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, move := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "old.sh"}], "moved bytes")
	require.Empty(t, got[hunkAttribution{commit: move, path: "new.sh"}],
		"an exec-bit change does not alter the blob's bytes")
	require.Len(t, got, 1)
}

type hunkAttribution struct {
	commit Hash
	path   string
}

func collectAttributedHunks(t *testing.T, scanner *HistoryScanner) map[hunkAttribution][]HunkAddition {
	t.Helper()
	hunks, errC := scanner.DiffHistoryHunks()
	got := make(map[hunkAttribution][]HunkAddition)
	for hunk := range hunks {
		key := hunkAttribution{commit: hunk.Commit(), path: hunk.Path()}
		got[key] = append(got[key], hunk)
	}
	require.NoError(t, <-errC)
	return got
}

func twoCommitHistoryOIDs(t *testing.T, scanner *HistoryScanner) (root, child Hash) {
	t.Helper()
	commits, err := scanner.loadAllCommits()
	require.NoError(t, err)
	require.Len(t, commits, 2)

	var childParent Hash
	for _, commit := range commits {
		switch len(commit.ParentOIDs) {
		case 0:
			require.True(t, root.IsZero(), "history has multiple root commits")
			root = commit.OID
		case 1:
			require.True(t, child.IsZero(), "history has multiple non-root commits")
			child = commit.OID
			childParent = commit.ParentOIDs[0]
		default:
			t.Fatalf("commit %s has %d parents, want at most one", commit.OID, len(commit.ParentOIDs))
		}
	}
	require.False(t, root.IsZero(), "root commit not found")
	require.False(t, child.IsZero(), "child commit not found")
	require.Equal(t, root, childParent, "non-root commit does not descend from root")
	return root, child
}

func requireHunkLines(t *testing.T, hunks []HunkAddition, want ...string) {
	t.Helper()
	require.Len(t, hunks, 1)
	require.Equal(t, want, hunks[0].Lines())
}
