// diff_tree_sort_order_test.go covers walkDiff against Git's own tree sort
// order, using repositories built with the git CLI.
//
// Git compares a tree entry's name as if a tree's name ended in '/', so a
// sibling blob whose name continues with a byte below '/' -- '.', '-', ' ',
// '!' -- is stored before the tree it shadows, while one continuing with a byte
// above '/' is stored after it. A merge-join that compares the two sides' names
// any other way steps its cursors out of order the moment such a sibling exists
// on one side only, and every same-name pair after it is missed: the shadowed
// subtree is then enumerated whole, as deletions on the old side and additions
// on the new one.
//
// The tests here pin that from three angles:
//
//   - The change set walkDiff reports for every parent->child pair of a
//     window-heavy history equals what `git diff-tree -r` reports. This is the
//     oracle: a hand-written expectation and a wrong comparator can agree, git
//     cannot be talked into agreeing with either.
//   - Blob mode attributes a blob to the commit that introduced it, not to a
//     later commit that only added a sibling next to its directory.
//   - Hunk mode reports the lines a commit added inside a shadowed subtree, not
//     the whole file.
package objstore

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// treeChange is the part of a diff that `git diff-tree -r` and walkDiff must
// agree on: a path plus the object ID on each side, with a zero ID where the
// entry does not exist. Modes are excluded because the two spellings differ by
// construction -- walkDiff carries one mode per event, the side that event
// describes.
type treeChange struct {
	path           string
	oldOID, newOID Hash
}

// String renders one event compactly, because the two sides are compared as
// sorted strings: a mismatch then prints as a readable line diff instead of two
// maps of 20-byte arrays.
func (c treeChange) String() string {
	return fmt.Sprintf("%s %s->%s", c.path, shortOID(c.oldOID), shortOID(c.newOID))
}

// shortOID abbreviates an object ID, naming the zero hash for the side of an
// addition or deletion where no object exists.
func shortOID(h Hash) string {
	if h.IsZero() {
		return "absent"
	}
	return h.String()[:8]
}

// sortedChanges renders a change set for comparison. Sorting drops emission
// order -- tree order decides that, and the unit tests pin it -- while keeping
// duplicates, so a doubled event still fails.
func sortedChanges(changes []treeChange) []string {
	lines := make([]string, 0, len(changes))
	for _, c := range changes {
		lines = append(lines, c.String())
	}
	slices.Sort(lines)
	return lines
}

// TestWalkDiff_MatchesGitDiffTree diffs every parent->child pair of a history
// built around directory/file name windows and requires walkDiff to report the
// same change set as git.
func TestWalkDiff_MatchesGitDiffTree(t *testing.T) {
	requireGit(t)

	repo := buildNameWindowRepo(t)

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	// %H %s gives the walk order and a readable subtest name in one call; the
	// fixture's subjects are single words.
	log := strings.Split(gitOutput(t, repo, "log", "--reverse", "--format=%H %s"), "\n")
	require.Greater(t, len(log), 1, "fixture must have several commits")

	var parent string
	for _, line := range log {
		commit, subject, ok := strings.Cut(line, " ")
		require.True(t, ok, "unexpected git log line %q", line)

		t.Run(subject, func(t *testing.T) {
			want := sortedChanges(gitDiffTreeChanges(t, repo, parent, commit))
			got := sortedChanges(walkDiffChanges(t, scanner, repo, parent, commit))
			require.Equal(t, want, got, "walkDiff disagrees with git diff-tree -r")
		})

		parent = commit
	}
}

// TestScanModeBlob_ShadowedSubtreeKeepsIntroducingCommit pins blob-mode
// attribution across a name window.
//
// Blob mode dedupes by OID over a newest-first walk, so whichever commit is
// credited first owns the blob. A commit that only adds "pkg.go" must not also
// claim the blobs under "pkg/": they were introduced by an earlier commit, and
// "which commit introduced this blob" is the answer callers act on.
//
// Both consumers of the candidate walk are exercised, because neither covers
// the other: scanBlobsStreaming, which Scan dispatches to, and planScanJobs.
func TestScanModeBlob_ShadowedSubtreeKeepsIntroducingCommit(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "pkg"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "pkg", "a.txt"), []byte("a content\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "pkg", "b.txt"), []byte("b content\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "root", "--quiet")

	require.NoError(t, os.WriteFile(filepath.Join(repo, "pkg.go"), []byte("package pkg\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "sibling", "--quiet")

	// planScanJobs only schedules packed objects, so both consumers need the
	// fixture packed to see the same candidate set.
	runGit(t, repo, "repack", "-a", "-d", "--quiet")

	requireWindowOrder(t, repo, "HEAD", "pkg.go", "pkg")

	// Commits are compared by subject so a wrong attribution reads as a name
	// rather than as two byte arrays.
	subject := map[Hash]string{
		parseObjectID(t, gitOutput(t, repo, "rev-parse", "HEAD^")): "root",
		parseObjectID(t, gitOutput(t, repo, "rev-parse", "HEAD")):  "sibling",
	}
	wantCommit := map[string]string{
		"pkg/a.txt": "root",
		"pkg/b.txt": "root",
		"pkg.go":    "sibling",
	}
	wantPath := make(map[Hash]string, len(wantCommit))
	for path := range wantCommit {
		wantPath[parseObjectID(t, gitOutput(t, repo, "rev-parse", "HEAD:"+path))] = path
	}

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	// requireAttribution keys on the blob OID because that is what the dedupe
	// keys on: the path and commit are exactly what a misaligned walk gets
	// wrong.
	requireAttribution := func(t *testing.T, blob Hash, path string, commit Hash) {
		t.Helper()
		want, ok := wantPath[blob]
		require.Truef(t, ok, "unexpected blob %s at %s", blob, path)
		require.Equal(t, want, path, "blob %s reported under the wrong path", blob)

		got, ok := subject[commit]
		if !ok {
			got = commit.String()
		}
		require.Equal(t, wantCommit[want], got, "blob at %s attributed to the wrong commit", want)
	}

	t.Run("Scan", func(t *testing.T) {
		rec := &capturingBlobScanner{}
		require.NoError(t, scanner.Scan(nil, rec))
		require.Len(t, rec.items, len(wantPath), "every blob is scanned exactly once")
		for _, item := range rec.items {
			requireAttribution(t, item.meta.Blob, item.meta.Path, item.meta.Commit)
		}
	})

	t.Run("planScanJobs", func(t *testing.T) {
		jobsByPack, err := scanner.planScanJobs(nil)
		require.NoError(t, err)
		jobs := flattenJobs(jobsByPack)
		require.Len(t, jobs, len(wantPath), "every blob is scheduled exactly once")
		for _, job := range jobs {
			requireAttribution(t, job.Blob, job.Path, job.Commit)
		}
	})
}

// TestDiffHistoryHunks_ShadowedSubtreeEditReportsOnlyAddedLines pins hunk mode
// across a name window.
//
// The commit appends one line to "pkg/a.txt" and adds "pkg.go" next to "pkg/".
// Only the appended line is new content. Suppression cannot rescue a walk that
// enumerates the subtree instead of pairing it, because the deleted and added
// entries carry different blob OIDs, so the whole file would be reported as
// added lines.
func TestDiffHistoryHunks_ShadowedSubtreeEditReportsOnlyAddedLines(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "pkg"), 0o755))
	target := filepath.Join(repo, "pkg", "a.txt")
	require.NoError(t, os.WriteFile(target, []byte("line1\nline2\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add the directory", "--quiet")

	require.NoError(t, os.WriteFile(target, []byte("line1\nline2\nline3\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "pkg.go"), []byte("package pkg\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "append a line and add the sibling", "--quiet")

	requireWindowOrder(t, repo, "HEAD", "pkg.go", "pkg")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"),
		WithScanMode(ScanModeHunks))
	require.NoError(t, err)
	defer scanner.Close()

	root, edit := twoCommitHistoryOIDs(t, scanner)
	got := collectAttributedHunks(t, scanner)

	requireHunkLines(t, got[hunkAttribution{commit: root, path: "pkg/a.txt"}], "line1", "line2")
	requireHunkLines(t, got[hunkAttribution{commit: edit, path: "pkg/a.txt"}], "line3")
	requireHunkLines(t, got[hunkAttribution{commit: edit, path: "pkg.go"}], "package pkg")
	require.Len(t, got, 3)
}

// buildNameWindowRepo builds a repository whose churn happens around names that
// straddle the '/' a tree sorts with, and returns its work tree.
//
// Every commit is one step of that churn: opening a window over a directory,
// editing and deleting inside the shadowed subtree, adding siblings on both
// sides of '/', closing the window again, and converting a directory to a file
// and a file to a directory while a window sibling is present. Names use plain
// ASCII with no tabs, which keeps `git diff-tree -r` output unquoted and
// parseable on the tab before the path.
func buildNameWindowRepo(t *testing.T) string {
	t.Helper()

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")

	write := func(name, content string) {
		t.Helper()
		path := filepath.Join(repo, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
	commit := func(subject string) {
		t.Helper()
		runGit(t, repo, "add", "-A")
		runGit(t, repo, "commit", "-m", subject, "--quiet")
	}

	write("pkg/a.txt", "line1\n")
	write("pkg/b.txt", "b\n")
	write("pkg/c.txt", "c\n")
	write("mod/inner.txt", "inner\n")
	write("top.txt", "top\n")
	commit("root")

	// '.' (0x2E) is below '/', so "pkg.go" is stored before the tree "pkg".
	write("pkg.go", "package pkg\n")
	commit("open-the-window")
	requireWindowOrder(t, repo, "HEAD", "pkg.go", "pkg")

	write("pkg/a.txt", "line1\nline2\n")
	commit("edit-inside-the-shadowed-subtree")

	runGit(t, repo, "rm", "--quiet", "pkg/b.txt")
	commit("delete-inside-the-shadowed-subtree")

	// One sibling on each side of '/': '-' (0x2D) sorts before the tree, '0'
	// (0x30) after it.
	write("pkg-old", "old\n")
	write("pkg0", "zero\n")
	commit("siblings-on-both-sides")
	requireWindowOrder(t, repo, "HEAD", "pkg", "pkg0")

	runGit(t, repo, "rm", "--quiet", "pkg.go")
	commit("close-the-window")

	write("mod-old.txt", "old\n")
	write("top-note.txt", "note\n")
	commit("open-windows-over-the-converted-names")
	requireWindowOrder(t, repo, "HEAD", "mod-old.txt", "mod")

	runGit(t, repo, "rm", "--quiet", "-r", "mod")
	write("mod", "now a file\n")
	commit("directory-becomes-a-file")
	// The window flips with the conversion: the blob "mod" sorts before its
	// sibling, the tree "mod" sorted after it.
	requireWindowOrder(t, repo, "HEAD", "mod", "mod-old.txt")

	runGit(t, repo, "rm", "--quiet", "top.txt")
	write("top/x.txt", "inside\n")
	commit("file-becomes-a-directory")
	requireWindowOrder(t, repo, "HEAD", "top-note.txt", "top")

	return repo
}

// requireWindowOrder asserts that rev's root tree really stores before and after
// adjacently in that order, so a fixture that means to open a name window
// cannot silently stop doing so.
func requireWindowOrder(t *testing.T, repo, rev, before, after string) {
	t.Helper()

	var names []string
	for _, line := range strings.Split(gitOutput(t, repo, "ls-tree", rev), "\n") {
		_, name, ok := strings.Cut(line, "\t")
		require.Truef(t, ok, "unexpected git ls-tree line %q", line)
		names = append(names, name)
	}

	for i, name := range names {
		if name != before {
			continue
		}
		require.Lessf(t, i+1, len(names), "%q is the last entry of %s, so %q cannot follow it", before, rev, after)
		require.Equal(t, after, names[i+1], "git does not store %q immediately before %q", before, after)
		return
	}
	t.Fatalf("%q is absent from %s, entries are %q", before, rev, names)
}

// gitDiffTreeChanges returns the changes `git diff-tree -r` reports between two
// commits, or between the empty tree and commit when parent is empty.
//
// Rename and copy detection is off so the output is one entry per changed path,
// which is the shape walkDiff produces.
func gitDiffTreeChanges(t *testing.T, repo, parent, commit string) []treeChange {
	t.Helper()

	args := []string{"diff-tree", "-r", "--no-renames", "--root"}
	if parent != "" {
		args = append(args, parent)
	}
	args = append(args, commit)

	var changes []treeChange
	for _, line := range strings.Split(gitOutput(t, repo, args...), "\n") {
		// A raw entry is ":<oldmode> <newmode> <oldsha> <newsha> <status>\t<path>".
		// The commit line git prints for a single-argument invocation carries no
		// leading colon.
		if !strings.HasPrefix(line, ":") {
			continue
		}
		meta, path, ok := strings.Cut(line[1:], "\t")
		require.Truef(t, ok, "no path in git diff-tree entry %q", line)

		fields := strings.Fields(meta)
		require.Lenf(t, fields, 5, "unexpected git diff-tree entry %q", line)
		require.NotEqualf(t, "T", fields[4],
			"fixture holds an in-place type change between two non-tree entries at %q; "+
				"git spells that as one entry carrying both OIDs and walkDiff as two events, "+
				"so this oracle cannot compare it", path)

		changes = append(changes, treeChange{
			path:   path,
			oldOID: parseObjectID(t, fields[2]),
			newOID: parseObjectID(t, fields[3]),
		})
	}
	return changes
}

// walkDiffChanges runs walkDiff over the two commits' trees and collects the
// events it emits. An empty parent walks from the zero hash, which is how the
// scanners diff a root commit.
func walkDiffChanges(t *testing.T, scanner *HistoryScanner, repo, parent, commit string) []treeChange {
	t.Helper()

	var parentTree Hash
	if parent != "" {
		parentTree = parseObjectID(t, gitOutput(t, repo, "rev-parse", parent+"^{tree}"))
	}
	commitTree := parseObjectID(t, gitOutput(t, repo, "rev-parse", commit+"^{tree}"))

	var changes []treeChange
	err := walkDiff(scanner.store, parentTree, commitTree, "",
		func(path string, oldOID, newOID Hash, _ uint32) error {
			changes = append(changes, treeChange{path: path, oldOID: oldOID, newOID: newOID})
			return nil
		})
	require.NoError(t, err)
	return changes
}

// parseObjectID converts a 40-character hex object ID. Git's all-zero ID parses
// to the zero Hash, which is what walkDiff reports for an absent side.
func parseObjectID(t *testing.T, s string) Hash {
	t.Helper()
	h, err := ParseHash(s)
	require.NoErrorf(t, err, "parse object ID %q", s)
	return h
}
