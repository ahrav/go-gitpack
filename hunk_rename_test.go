package objstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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

// The root/shallow fast path emits during the walk, so its only channel send is
// inside emit. An entry the mode filter rejects never reaches emit, which is why
// the cancellation check has to sit ahead of that filter: a tree of nothing but
// gitlinks would otherwise traverse to completion after another worker had
// already failed. stopCh is closed before the call so this is deterministic
// rather than a race.
func TestEmitCommitBlobPairs_RootWalkAbortsOnNonBlobEntries(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	// A gitlink is the one entry kind that reaches the callback and is then
	// rejected by isBlobMode; walkDiff recurses into trees rather than
	// reporting them. Staged via plumbing so no real submodule is needed.
	runGit(t, repo, "update-index", "--add", "--cacheinfo",
		"160000,1111111111111111111111111111111111111111,sub")
	runGit(t, repo, "commit", "-m", "gitlink-only root", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer scanner.Close()

	commits, err := scanner.loadAllCommits()
	require.NoError(t, err)
	require.Len(t, commits, 1)
	root := commits[0]
	require.Empty(t, root.ParentOIDs, "fixture must be a root commit to take the zero-parent path")

	stopCh := make(chan struct{})
	close(stopCh)

	// Buffered so a hypothetical emit could not block; the walk must abort
	// before producing anything regardless.
	blobs := make(chan blobPairWork, 8)
	err = scanner.emitCommitBlobPairs(root, Hash{}, blobs, stopCh)

	require.ErrorIs(t, err, errScanAborted,
		"a cancelled root walk must abort even when every entry is filtered out")
	require.Empty(t, blobs, "no work should be emitted after cancellation")
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

func TestDiffHistoryHunks_DirectoryRenameEditPairsAgainstOldPath(t *testing.T) {
	requireGit(t)

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

	linesByPath, _ := scanHunksByPath(t, filepath.Join(repo, ".git"))

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
	requireGit(t)

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

	linesByPath, _ := scanHunksByPath(t, filepath.Join(repo, ".git"))

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
	requireGit(t)

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

// TestDiffHistoryHunks_InferredRenameOntoMuchSmallerFileReportsTextLines
// covers the shrinking-pairing case in gateInferredRenameHunks' similarity
// test. An unrelated new file that is much smaller than the deleted file it
// was paired with can share every one of its lines with that file, so the pair
// diff has ZERO added lines. Scoring similarity against the new file alone
// would read that as "identical content, trustworthy pairing" and emit no hunk
// at all, dropping the new file's content from the stream; scoring against the
// larger side rejects the pairing the way Git's max(src,dst) denominator does.
func TestDiffHistoryHunks_InferredRenameOntoMuchSmallerFileReportsTextLines(t *testing.T) {
	var big strings.Builder
	for i := range 100 {
		fmt.Fprintf(&big, "line-%d\n", i)
	}
	// The new file's only line occurs in the old file, so the pair diff adds
	// nothing: 1 common line against a 100-line old side is 2% similar.
	gitDir := buildInferredRenameRepo(t, []byte(big.String()), []byte("line-7\n"))

	linesByPath, binaryByPath := scanHunksByPath(t, gitDir)

	assert.Equal(t, []string{"line-7"}, linesByPath["new/data"],
		"a pairing that only shrinks must fall back to the whole-file addition")
	assert.False(t, binaryByPath["new/data"])
}

// TestDiffHistoryHunks_DroppedDeletePathsStillSuppressExactMoves pins the
// degradation contract of maxRetainedDeletePathBytes. Past that bound a commit
// stops retaining deleted paths, which costs directory-rename inference — it
// has no source path left to build evidence from — but must not cost exact-OID
// move suppression, which needs only a per-identity credit.
func TestDiffHistoryHunks_DroppedDeletePathsStillSuppressExactMoves(t *testing.T) {
	requireGit(t)

	// Small enough that the fixture's handful of deleted paths overruns it.
	// maxRetainedDeletePathBytes is package-global, so this test must stay
	// serial (no t.Parallel()); see its doc comment.
	restore := maxRetainedDeletePathBytes
	maxRetainedDeletePathBytes = 8
	t.Cleanup(func() { maxRetainedDeletePathBytes = restore })

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.Mkdir(filepath.Join(repo, "old"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "a.txt"), []byte("anchor a\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "b.txt"), []byte("anchor b\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "edited.txt"),
		[]byte("keep one\nkeep two\nkeep three\nkeep four\n"), 0o644))
	runGit(t, repo, "add", "old")
	runGit(t, repo, "commit", "-m", "add", "--quiet")

	// Move old/ -> new/. a.txt and b.txt move byte-identical (exact-OID moves);
	// edited.txt gains a line, so inference is what would have paired it.
	require.NoError(t, os.Rename(filepath.Join(repo, "old"), filepath.Join(repo, "new")))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "new", "edited.txt"),
		[]byte("keep one\nkeep two\nkeep three\nkeep four\nbrand new\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "move dir", "--quiet")

	linesByPath, _ := scanHunksByPath(t, filepath.Join(repo, ".git"))

	// Suppression is unaffected: both exact moves stay silent.
	assert.Empty(t, linesByPath["new/a.txt"], "an exact-OID move must stay suppressed without delete paths")
	assert.Empty(t, linesByPath["new/b.txt"], "an exact-OID move must stay suppressed without delete paths")

	// Inference is given up, so the edited file is a whole-file addition
	// rather than just its one added line. Losing precision is the accepted
	// cost; losing the content would not be.
	assert.ElementsMatch(t,
		[]string{"keep one", "keep two", "keep three", "keep four", "brand new"},
		linesByPath["new/edited.txt"],
		"without delete paths the edited file must still report every line")
}

// TestDiffHistoryHunks_InferredRenameAcrossEntryTypesReportsSymlink covers the
// entry-type half of directory-rename inference. A regular file whose bytes are
// exactly a path string and a symlink to that path share a blob OID, so pairing
// them from path structure alone yields a pair diff whose old side equals its
// new side: no hunk at all, and the symlink's target silently leaves the
// stream. Exact-OID suppression already refuses that conflation by keying on
// blob identity rather than OID; inference must refuse it too.
func TestDiffHistoryHunks_InferredRenameAcrossEntryTypesReportsSymlink(t *testing.T) {
	requireGit(t)

	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.Mkdir(filepath.Join(repo, "old"), 0o755))
	// No trailing newline: the blob is exactly the symlink target string, so
	// the regular file and the symlink hash identically.
	const target = "target/path"
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "payload"), []byte(target), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "a.txt"), []byte("anchor a\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "old", "b.txt"), []byte("anchor b\n"), 0o644))
	runGit(t, repo, "add", "old")
	runGit(t, repo, "commit", "-m", "add", "--quiet")

	// Move old/ -> new/. a.txt and b.txt move byte-identical, which is the two
	// anchors inference needs; payload arrives at the corresponding new path as
	// a SYMLINK rather than a regular file.
	require.NoError(t, os.Mkdir(filepath.Join(repo, "new"), 0o755))
	for _, name := range []string{"a.txt", "b.txt"} {
		require.NoError(t, os.Rename(
			filepath.Join(repo, "old", name), filepath.Join(repo, "new", name)))
	}
	require.NoError(t, os.Remove(filepath.Join(repo, "old", "payload")))
	require.NoError(t, os.Remove(filepath.Join(repo, "old")))
	require.NoError(t, os.Symlink(target, filepath.Join(repo, "new", "payload")))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "move dir, file becomes symlink", "--quiet")

	linesByPath, _ := scanHunksByPath(t, filepath.Join(repo, ".git"))

	assert.Equal(t, []string{target}, linesByPath["new/payload"],
		"a blob-to-symlink type change must not be silenced by an inferred rename")
	// The anchors are genuine exact-OID moves and stay suppressed.
	assert.Empty(t, linesByPath["new/a.txt"])
	assert.Empty(t, linesByPath["new/b.txt"])
}

// TestDiffHistoryHunks_InferredRenameOfLargeFileReportsTextLines covers the
// lossy-algorithm escape hatch in gateInferredRenameHunks. Past
// SmallFileThreshold computeAddedHunks switches to set-membership diffing, where
// one occurrence of a line in the old blob marks every occurrence in the new
// blob as not added. A one-line old file paired with a megabyte of that same
// line therefore produces zero added lines, and a gate that trusted that count
// would suppress the entire new file.
func TestDiffHistoryHunks_InferredRenameOfLargeFileReportsTextLines(t *testing.T) {
	const lines = 600_000 // ~1.2 MB, past SmallFileThreshold
	gitDir := buildInferredRenameRepo(t,
		[]byte("x\n"), []byte(strings.Repeat("x\n", lines)))

	linesByPath, binaryByPath := scanHunksByPath(t, gitDir)

	assert.Len(t, linesByPath["new/data"], lines,
		"a pairing the gate cannot measure must fall back to the whole-file addition")
	assert.False(t, binaryByPath["new/data"])
}
