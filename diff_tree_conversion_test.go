// diff_tree_conversion_test.go covers the end-to-end consequences of an
// entry-type transition, using real repositories built with the git CLI.
//
// walkDiff reports a path whose entry type changes as a deletion of the old
// entry followed by an addition of the new one, and both consumers of the walk
// depend on that shape:
//
//   - Hunk mode mints a suppression credit from the deletion and diffs the
//     addition against nothing, so no tree OID or submodule commit OID ever
//     reaches the blob differ.
//   - Blob mode filters on the entry mode, so blobs that arrive only under a
//     directory replacing a file or a gitlink are visited at all.
//
// The tests below pin one behavior each, keyed by (commit, path), plus the one
// residual documented on ScanModeHunks: an in-place overwrite mints no credit.

package objstore

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// absentSubmoduleCommit is a commit OID that exists in no repository. A gitlink
// records the commit of a submodule whose objects live in another store, so
// this is what the superproject legitimately looks like: any code path that
// tries to read the OID as a local object fails with ErrObjectNotFound.
const absentSubmoduleCommit = "1111111111111111111111111111111111111111"

// gitOutput runs git in repo with the fixture-isolated environment and returns
// its trimmed combined output. runGit discards output, which these fixtures
// need in order to name the object IDs git assigned.
func gitOutput(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	cmd.Env = gitFixtureEnv()
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "git %v failed: %s", args, out)
	return strings.TrimSpace(string(out))
}

// A move suppresses its destination through the deletion of its source. When
// the source path is reoccupied by an entry of a different type, that
// reoccupation is still a deletion of the old blob, so the credit exists and
// the destination re-adds nothing. The symlink taking the path over is a
// separate identity and reports its own target.
func TestDiffHistoryHunks_MoveWhoseSourceBecomesASymlinkIsSuppressed(t *testing.T) {
	requireGit(t)

	const (
		moved      = "moved bytes\n"
		linkTarget = "elsewhere/target"
	)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "src.txt"), []byte(moved), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add source", "--quiet")

	require.NoError(t, os.Remove(filepath.Join(repo, "src.txt")))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "dst.txt"), []byte(moved), 0o644))
	require.NoError(t, os.Symlink(linkTarget, filepath.Join(repo, "src.txt")))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "move the bytes and symlink the source path", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, move := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "src.txt"}], "moved bytes")
	require.Empty(t, got[hunkAttribution{commit: move, path: "dst.txt"}],
		"the vacated source credits the moved bytes, so the destination re-adds nothing")
	requireHunkLines(t, got[hunkAttribution{commit: move, path: "src.txt"}], linkTarget)
	require.Len(t, got, 2, "expected the root addition and the symlink's arrival")
}

// A regular file whose bytes are exactly a path string and a symlink to that
// path share one blob OID. Replacing one with the other at a single path is two
// entries with two identities, so the arriving symlink reports its target
// rather than being dropped as unchanged content.
func TestDiffHistoryHunks_InPlaceTypeChangeKeepingOneOIDEmitsArrival(t *testing.T) {
	requireGit(t)

	// No trailing newline: the blob is exactly the symlink target string.
	const target = "target/path"

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "payload.txt"), []byte(target), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add regular file", "--quiet")

	require.NoError(t, os.Remove(filepath.Join(repo, "payload.txt")))
	require.NoError(t, os.Symlink(target, filepath.Join(repo, "payload.txt")))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "replace the file with a symlink in place", "--quiet")

	// The shared OID is the whole point of the fixture: with two OIDs the
	// arrival would emit under any rule.
	require.Equal(t,
		gitOutput(t, repo, "rev-parse", "HEAD^:payload.txt"),
		gitOutput(t, repo, "rev-parse", "HEAD:payload.txt"),
		"fixture must keep one blob OID across the type change")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, swap := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "payload.txt"}], target)
	requireHunkLines(t, got[hunkAttribution{commit: swap, path: "payload.txt"}], target)
	require.Len(t, got, 2, "the symlink's arrival is its own event")
}

// A permission change keeps the entry type, so it stays a single event carrying
// both OIDs and is filtered as unchanged content. Splitting it would report the
// whole file as added lines on every chmod.
func TestDiffHistoryHunks_PermissionOnlyChangeEmitsNoHunk(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	script := filepath.Join(repo, "script.sh")
	require.NoError(t, os.WriteFile(script, []byte("echo hello\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add script", "--quiet")

	require.NoError(t, os.Chmod(script, 0o755))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "make it executable", "--quiet")

	require.Contains(t, gitOutput(t, repo, "ls-tree", "HEAD", "script.sh"), "100755",
		"fixture must record the exec bit, otherwise there is no mode change to test")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, chmod := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "script.sh"}], "echo hello")
	require.Empty(t, got[hunkAttribution{commit: chmod, path: "script.sh"}],
		"permission bits are not blob content")
	require.Len(t, got, 1)
}

// The residual documented on ScanModeHunks: only a deletion mints a
// suppression credit, and an in-place overwrite is not a deletion. A move whose
// source is reoccupied by an entry of the SAME type therefore still reports its
// destination in full. Widening credits to the old side of a same-type
// modification would make a commit that swaps two files' contents emit nothing
// at all, so this addition is required to survive.
func TestDiffHistoryHunks_SameTypeReoccupationStillEmitsFullAddition(t *testing.T) {
	requireGit(t)

	const (
		shared    = "shared bytes\n"
		different = "different bytes\n"
	)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	src := filepath.Join(repo, "src.txt")
	require.NoError(t, os.WriteFile(src, []byte(shared), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add source", "--quiet")

	require.NoError(t, os.WriteFile(filepath.Join(repo, "dst.txt"), []byte(shared), 0o644))
	require.NoError(t, os.WriteFile(src, []byte(different), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "copy the bytes and overwrite the source", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, reoccupy := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "src.txt"}], "shared bytes")
	requireHunkLines(t, got[hunkAttribution{commit: reoccupy, path: "src.txt"}], "different bytes")
	requireHunkLines(t, got[hunkAttribution{commit: reoccupy, path: "dst.txt"}], "shared bytes")
	require.Len(t, got, 3)
}

// A directory replaced by a file must diff the arriving blob against nothing.
// Forwarding the old tree's OID as the previous blob would feed tree bytes to
// the differ, whose NUL bytes trip the binary heuristic and mislabel a text
// file as binary.
func TestDiffHistoryHunks_DirectoryToFileEmitsTextHunk(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "dir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "dir", "a.txt"), []byte("inside the directory\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add directory", "--quiet")

	require.NoError(t, os.RemoveAll(filepath.Join(repo, "dir")))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "dir"), []byte("line one\nline two\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "replace the directory with a file", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, convert := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "dir/a.txt"}], "inside the directory")

	hunks := got[hunkAttribution{commit: convert, path: "dir"}]
	requireHunkLines(t, hunks, "line one", "line two")
	require.False(t, hunks[0].IsBinary(), "the arriving entry is a text blob")
	require.Len(t, got, 2)
}

// Recursing into the replaced directory is what mints deletion credits for the
// blobs that left it, so a blob moved out of a directory that a file replaces
// in the same commit is still suppressed.
func TestDiffHistoryHunks_DirectoryToFileMintsDeletionCredits(t *testing.T) {
	requireGit(t)

	const moved = "moved bytes\n"

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "dir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "dir", "moved.txt"), []byte(moved), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add directory", "--quiet")

	require.NoError(t, os.RemoveAll(filepath.Join(repo, "dir")))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "dir"), []byte("now a file\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "moved.txt"), []byte(moved), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "replace the directory and move its file out", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	root, convert := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "dir/moved.txt"}], "moved bytes")
	require.Empty(t, got[hunkAttribution{commit: convert, path: "moved.txt"}],
		"the deleted subtree credits the moved bytes")
	requireHunkLines(t, got[hunkAttribution{commit: convert, path: "dir"}], "now a file")
	require.Len(t, got, 2)
}

// A gitlink replaced by a file must diff the arriving blob against nothing. The
// submodule's commit OID is not an object in this repository, so handing it to
// the blob differ ends the whole scan with ErrObjectNotFound.
func TestDiffHistoryHunks_GitlinkToFileCompletesScan(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	// Staged through plumbing so no real submodule repository is needed, which
	// is what keeps the referenced commit genuinely absent from the store.
	runGit(t, repo, "update-index", "--add", "--cacheinfo", "160000,"+absentSubmoduleCommit+",sub")
	runGit(t, repo, "commit", "-m", "gitlink root", "--quiet")

	runGit(t, repo, "update-index", "--force-remove", "sub")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "sub"), []byte("regular content\n"), 0o644))
	runGit(t, repo, "add", "sub")
	runGit(t, repo, "commit", "-m", "replace the gitlink with a file", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	_, convert := twoCommitHistoryOIDs(t, scanner)
	// collectAttributedHunks fails the test on a non-nil scan error, which is
	// the primary assertion here.
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: convert, path: "sub"}], "regular content")
	require.Len(t, got, 1, "the gitlink root contributes no scannable content")
}

// TestScanModeBlob_ConversionIntroducedBlobIsScanned pins blob mode against a
// blob that exists nowhere else in history and arrives only under a directory
// that replaces a non-tree entry.
//
// Every blob-mode consumer filters on isBlobMode, so a single event carrying
// the new directory's tree mode would drop the whole subtree and the blob would
// never be visited -- silent data loss in the mode documented as visiting every
// unique blob.
//
// Both consumers of the candidate walk are exercised: scanBlobsStreaming, which
// Scan dispatches to, and planScanJobs, which is reached only directly. Neither
// covers the other.
func TestScanModeBlob_ConversionIntroducedBlobIsScanned(t *testing.T) {
	requireGit(t)

	// Unique in the fixture, so nothing else in history can schedule it.
	const unique = "conversion-only content\n"

	cases := []struct {
		name  string
		build func(t *testing.T, repo string)
		path  string
	}{
		{
			name: "FileToDirectory",
			build: func(t *testing.T, repo string) {
				require.NoError(t, os.WriteFile(filepath.Join(repo, "foo"), []byte("plain file\n"), 0o644))
				runGit(t, repo, "add", "-A")
				runGit(t, repo, "commit", "-m", "add file", "--quiet")

				require.NoError(t, os.Remove(filepath.Join(repo, "foo")))
				require.NoError(t, os.MkdirAll(filepath.Join(repo, "foo"), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(repo, "foo", "bar"), []byte(unique), 0o644))
				runGit(t, repo, "add", "-A")
				runGit(t, repo, "commit", "-m", "replace the file with a directory", "--quiet")
			},
			path: "foo/bar",
		},
		{
			name: "GitlinkToDirectory",
			build: func(t *testing.T, repo string) {
				runGit(t, repo, "update-index", "--add", "--cacheinfo", "160000,"+absentSubmoduleCommit+",sub")
				runGit(t, repo, "commit", "-m", "gitlink root", "--quiet")

				runGit(t, repo, "update-index", "--force-remove", "sub")
				require.NoError(t, os.MkdirAll(filepath.Join(repo, "sub"), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(repo, "sub", "inner.txt"), []byte(unique), 0o644))
				runGit(t, repo, "add", "sub")
				runGit(t, repo, "commit", "-m", "replace the gitlink with a directory", "--quiet")
			},
			path: "sub/inner.txt",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			repo := t.TempDir()
			runGit(t, repo, "init", "--quiet")
			tt.build(t, repo)
			// planScanJobs only schedules packed objects, so both consumers
			// need the fixture packed to see the same candidate set.
			runGit(t, repo, "repack", "-a", "-d", "--quiet")

			want, err := ParseHash(gitOutput(t, repo, "rev-parse", "HEAD:"+tt.path))
			require.NoError(t, err)

			scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
			require.NoError(t, err)
			defer scanner.Close()

			t.Run("Scan", func(t *testing.T) {
				rec := &capturingBlobScanner{}
				require.NoError(t, scanner.Scan(nil, rec))

				var found bool
				for _, item := range rec.items {
					if item.meta.Blob != want {
						continue
					}
					found = true
					require.Equal(t, tt.path, item.meta.Path)
					require.Equal(t, unique, string(item.data))
				}
				require.Truef(t, found, "blob %s at %s was never scanned", want, tt.path)
			})

			t.Run("planScanJobs", func(t *testing.T) {
				jobsByPack, err := scanner.planScanJobs(nil)
				require.NoError(t, err)

				var found bool
				for _, job := range flattenJobs(jobsByPack) {
					if job.Blob != want {
						continue
					}
					found = true
					require.Equal(t, tt.path, job.Path)
				}
				require.Truef(t, found, "blob %s at %s was never scheduled", want, tt.path)
			})
		})
	}
}
