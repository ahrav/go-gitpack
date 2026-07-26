package objstore

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Probe 1: the bot's claim. A move whose source path is simultaneously
// reoccupied by a symlink. The destination should be suppressed (exact-OID
// move) but the old blob's departure is never credited.
func TestProbe_MoveWithSymlinkReoccupation(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "src.txt"), []byte("payload\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add src", "--quiet")

	// Move src.txt -> dst.txt, then put a symlink at src.txt.
	runGit(t, repo, "mv", "src.txt", "dst.txt")
	require.NoError(t, os.Symlink("dst.txt", filepath.Join(repo, "src.txt")))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "move and symlink", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, child := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)
	for k, v := range got {
		lines := [][]string{}
		for _, h := range v {
			lines = append(lines, h.Lines())
		}
		tag := "root"
		if k.commit == child {
			tag = "child"
		}
		t.Logf("PROBE1 %s %-10s -> %v", tag, k.path, lines)
	}
	_ = root
}

// Probe 2: a regular file replaced in place by a directory of the same name.
// Does walkDiff ever report the blobs newly added under that directory?
func TestProbe_FileReplacedByDirectory(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "foo"), []byte("old file\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add foo as file", "--quiet")

	require.NoError(t, os.Remove(filepath.Join(repo, "foo")))
	require.NoError(t, os.Mkdir(filepath.Join(repo, "foo"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "foo", "bar"), []byte("SECRET\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "replace foo with a directory", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	// Raw walkDiff observation.
	commits, err := scanner.loadAllCommits()
	require.NoError(t, err)
	for _, c := range commits {
		if len(c.ParentOIDs) != 1 {
			continue
		}
		parentTree, err := scanner.firstParentTree(c)
		require.NoError(t, err)
		require.NoError(t, walkDiff(scanner.store, parentTree, c.TreeOID, "",
			func(path string, old, newH Hash, mode uint32) error {
				t.Logf("PROBE2 walkDiff: path=%-10q old=%s new=%s mode=%o blob=%v",
					path, shortHash(old), shortHash(newH), mode, isBlobMode(mode))
				return nil
			}))
	}

	got := collectAttributedHunks(t, scanner)
	for k, v := range got {
		lines := [][]string{}
		for _, h := range v {
			lines = append(lines, h.Lines())
		}
		t.Logf("PROBE2 hunk: %-10s -> %v", k.path, lines)
	}
}

// Probe 3: a directory replaced in place by a regular file of the same name.
// walkDiff hands the old TREE oid to a blob-diff consumer.
func TestProbe_DirectoryReplacedByFile(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "foo"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "foo", "bar"), []byte("inside dir\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add foo/bar", "--quiet")

	require.NoError(t, os.RemoveAll(filepath.Join(repo, "foo")))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "foo"), []byte("now a file\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "replace dir with file", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	commits, err := scanner.loadAllCommits()
	require.NoError(t, err)
	for _, c := range commits {
		if len(c.ParentOIDs) != 1 {
			continue
		}
		parentTree, err := scanner.firstParentTree(c)
		require.NoError(t, err)
		require.NoError(t, walkDiff(scanner.store, parentTree, c.TreeOID, "",
			func(path string, old, newH Hash, mode uint32) error {
				kind := "?"
				if _, ot, err := scanner.store.get(old); err == nil {
					kind = fmt.Sprint(ot)
				}
				t.Logf("PROBE3 walkDiff: path=%-10q old=%s(type=%s) new=%s mode=%o",
					path, shortHash(old), kind, shortHash(newH), mode)
				return nil
			}))
	}

	hunks, errC := scanner.DiffHistoryHunks()
	n := 0
	for hunk := range hunks {
		n++
		t.Logf("PROBE3 hunk: path=%s lines=%v", hunk.Path(), hunk.Lines())
	}
	t.Logf("PROBE3 scan err = %v (hunks=%d)", <-errC, n)
}

func shortHash(h Hash) string {
	if h.IsZero() {
		return "zero"
	}
	return h.String()[:8]
}

// Probe 4: does ScanModeBlob (the recommended mode) see a blob that only ever
// exists under a directory which replaced a same-named file in place?
func TestProbe_BlobModeMissesDirReplacingFile(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "foo"), []byte("old file\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add foo as file", "--quiet")

	require.NoError(t, os.Remove(filepath.Join(repo, "foo")))
	require.NoError(t, os.Mkdir(filepath.Join(repo, "foo"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "foo", "bar"), []byte("SECRET\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "replace foo with a directory", "--quiet")

	// Ground truth from git itself.
	out, err := exec.Command("git", "-C", repo, "log", "--reverse", "--raw", "--format=commit %h").CombinedOutput()
	require.NoError(t, err)
	t.Logf("PROBE4 git log --raw:\n%s", out)

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	sink := &capturingBlobScanner{}
	require.NoError(t, scanner.Scan(nil, sink))
	for _, it := range sink.items {
		t.Logf("PROBE4 blob-mode saw: path=%-10s data=%q", it.meta.Path, string(it.data))
	}
	found := false
	for _, it := range sink.items {
		if string(it.data) == "SECRET\n" {
			found = true
		}
	}
	t.Logf("PROBE4 SECRET reachable in ScanModeBlob = %v", found)
}

// Probe 5: same-type in-place modification of the source path while its bytes
// move elsewhere. No type change is involved, so no old mode is needed.
func TestProbe_MoveWithSameTypeReoccupation(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "src.txt"), []byte("payload\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add src", "--quiet")

	require.NoError(t, os.WriteFile(filepath.Join(repo, "dst.txt"), []byte("payload\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "src.txt"), []byte("brand new\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "move bytes, reoccupy source", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	_, child := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)
	for k, v := range got {
		lines := [][]string{}
		for _, h := range v {
			lines = append(lines, h.Lines())
		}
		tag := "root"
		if k.commit == child {
			tag = "child"
		}
		t.Logf("PROBE5 %s %-10s -> %v", tag, k.path, lines)
	}
}

// Probe 6: the second documented gap. A regular file whose bytes are exactly
// "target" replaced in place by a symlink to "target" -- one OID, two types.
func TestProbe_SameOIDTypeChangeInPlace(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "link"), []byte("target"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "target"), []byte("real\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add file holding \"target\"", "--quiet")

	require.NoError(t, os.Remove(filepath.Join(repo, "link")))
	require.NoError(t, os.Symlink("target", filepath.Join(repo, "link")))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "file becomes symlink, same OID", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	_, child := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)
	for k, v := range got {
		lines := [][]string{}
		for _, h := range v {
			lines = append(lines, h.Lines())
		}
		tag := "root"
		if k.commit == child {
			tag = "child"
		}
		t.Logf("PROBE6 %s %-10s -> %v", tag, k.path, lines)
	}
}

// Probe 7: permission-only change must stay excluded.
func TestProbe_PermissionOnlyChange(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	p := filepath.Join(repo, "s.sh")
	require.NoError(t, os.WriteFile(p, []byte("echo hi\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add", "--quiet")
	require.NoError(t, os.Chmod(p, 0o755))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "chmod +x", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	_, child := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)
	for k, v := range got {
		tag := "root"
		if k.commit == child {
			tag = "child"
		}
		t.Logf("PROBE7 %s %-10s -> %d hunk(s)", tag, k.path, len(v))
	}
}
