// commit_payload_test.go verifies store.readCommitPayload against the
// authoritative Git implementation: for every commit in a repository built to
// contain all three storage shapes — plain packed (non-delta), delta-chained,
// and loose — the payload must byte-equal `git cat-file commit` output.
//
// The oracle is deliberately `git cat-file commit` (raw object bytes, no
// "commit <size>\0" prefix) and NOT `git log --format=%B`, which re-encodes
// and NUL-truncates as presentation behavior.

package objstore

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zlib"
	"github.com/stretchr/testify/require"
)

// buildCommitShapesRepo initializes a Git repository whose commit objects
// cover the three storage shapes readCommitPayload must handle:
//
//   - delta-chained packed commits: long, mostly-identical messages make the
//     commit objects themselves profitable delta bases under an aggressive
//     repack (verified by requireCommitShapes, not assumed);
//   - plain packed commits: short unique messages that stay non-delta;
//   - one loose commit: created after the repack.
//
// It returns the repository root and pack directory. Skipped without git.
func buildCommitShapesRepo(t *testing.T) (repoDir, packDir string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repoDir = t.TempDir()
	git := func(args ...string) {
		t.Helper()
		cmd := gitTestCommand(repoDir, args...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@e",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@e",
		)
		out, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "git %s: %s", strings.Join(args, " "), out)
	}

	git("init", "-q")
	git("config", "commit.gpgsign", "false")

	writeFile := func(name, data string) {
		require.NoError(t, os.WriteFile(filepath.Join(repoDir, name), []byte(data), 0o644))
	}

	// A large shared body makes consecutive commit objects near-identical,
	// which the repack delta-compresses; multiline content also exercises
	// the header/message split downstream.
	var shared strings.Builder
	for i := range 120 {
		fmt.Fprintf(&shared, "shared line %d of the long template commit message body\n", i)
	}

	const commits = 30
	for c := range commits {
		writeFile("f.txt", fmt.Sprintf("content %d\n", c))
		git("add", "f.txt")
		git("commit", "-q", "-m", fmt.Sprintf("commit %d preamble\n%s", c, shared.String()))
	}
	// Short unique messages: cheap to store whole, so the repack keeps some
	// commits non-delta even at maximum window/depth.
	for c := range 5 {
		writeFile("g.txt", fmt.Sprintf("g %d\n", c))
		git("add", "g.txt")
		git("commit", "-q", "-m", fmt.Sprintf("short %d", c))
	}

	git("repack", "-adf", "--window=250", "--depth=50")

	// One commit after the repack stays loose.
	writeFile("h.txt", "loose\n")
	git("add", "h.txt")
	git("commit", "-q", "-m", "loose commit with a message\n\nsecond paragraph")

	packDir = filepath.Join(repoDir, ".git", "objects", "pack")
	return repoDir, packDir
}

// commitShapes classifies every commit OID in the repository by storage
// shape using `git verify-pack -v` (packed commits, with delta depth) and
// `git rev-list --all` (any commit absent from the pack listing is loose).
type commitShapes struct {
	plain []Hash // packed, non-delta
	delta []Hash // packed, delta-chained
	loose []Hash
}

func classifyCommitShapes(t *testing.T, repoDir, packDir string) commitShapes {
	t.Helper()

	idxs, err := filepath.Glob(filepath.Join(packDir, "*.idx"))
	require.NoError(t, err)
	require.Len(t, idxs, 1, "expected exactly one pack after repack")

	out, err := exec.Command("git", "verify-pack", "-v", idxs[0]).Output()
	require.NoError(t, err)

	packed := make(map[Hash]bool) // oid -> isDelta
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		// Object lines: "<oid> <type> <size> <packed-size> <offset> [<depth> <base>]".
		if len(fields) < 5 || fields[1] != "commit" {
			continue
		}
		oid, err := ParseHash(fields[0])
		if err != nil {
			continue
		}
		packed[oid] = len(fields) >= 7
	}
	require.NoError(t, sc.Err())

	revs, err := exec.Command("git", "-C", repoDir, "rev-list", "--all").Output()
	require.NoError(t, err)

	var shapes commitShapes
	for line := range strings.Lines(string(revs)) {
		oid, err := ParseHash(strings.TrimSpace(line))
		require.NoError(t, err)
		isDelta, inPack := packed[oid]
		switch {
		case !inPack:
			shapes.loose = append(shapes.loose, oid)
		case isDelta:
			shapes.delta = append(shapes.delta, oid)
		default:
			shapes.plain = append(shapes.plain, oid)
		}
	}
	return shapes
}

// TestReadCommitPayload_DifferentialAgainstGit proves the payload read path:
// every commit, in every storage shape, must round-trip byte-for-byte against
// `git cat-file commit`. A second read guards against payload corruption from
// cache aliasing (the delta path copies out of shared cache buffers).
func TestReadCommitPayload_DifferentialAgainstGit(t *testing.T) {
	repoDir, packDir := buildCommitShapesRepo(t)
	shapes := classifyCommitShapes(t, repoDir, packDir)

	// The test is vacuous unless all three shapes are present.
	require.NotEmpty(t, shapes.plain, "repo must contain plain packed commits")
	require.NotEmpty(t, shapes.delta, "repo must contain delta-chained commits")
	require.NotEmpty(t, shapes.loose, "repo must contain loose commits")
	t.Logf("commit shapes: %d plain, %d delta, %d loose",
		len(shapes.plain), len(shapes.delta), len(shapes.loose))

	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()

	check := func(shape string, oids []Hash) {
		for _, oid := range oids {
			want := gitCatFile(t, repoDir, "commit", oid)

			got, err := st.readCommitPayload(oid)
			require.NoErrorf(t, err, "readCommitPayload %s (%s)", oid, shape)
			require.Equalf(t, want, got, "payload mismatch for %s (%s)", oid, shape)

			// Second read: cache-warm delta paths must still return a
			// private, uncorrupted copy.
			again, err := st.readCommitPayload(oid)
			require.NoError(t, err)
			require.Equalf(t, want, again, "warm payload mismatch for %s (%s)", oid, shape)
		}
	}
	check("plain", shapes.plain)
	check("delta", shapes.delta)
	check("loose", shapes.loose)
}

// TestReadCommitPayload_NotACommit pins the error contract: asking for a
// non-commit object (a blob) fails with ErrObjectNotCommit rather than
// returning payload bytes.
func TestReadCommitPayload_NotACommit(t *testing.T) {
	objectsDir := t.TempDir()
	body := []byte("not a commit")
	blobOID := calculateHash(ObjBlob, body)
	path := looseObjectPath(objectsDir, blobOID)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))

	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	_, err := fmt.Fprintf(zw, "blob %d\x00", len(body))
	require.NoError(t, err)
	_, err = zw.Write(body)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	require.NoError(t, os.WriteFile(path, compressed.Bytes(), 0o644))

	st := &store{objectsDir: objectsDir}
	_, err = st.readCommitPayload(blobOID)
	require.ErrorIs(t, err, ErrObjectNotCommit)
}

// TestCommitPayload_HeaderLargerThanMaxHdr pins an improvement the payload
// path buys over the old header read: a commit whose header exceeds MaxHdr
// (4096 bytes) — here a 100-parent octopus merge — used to fail attribution
// because readCommitHeaderFromStream gives up at MaxHdr without finding the
// committer line. The payload path has no header cap, so attribution now
// succeeds, and the message still byte-matches the git oracle.
func TestCommitPayload_HeaderLargerThanMaxHdr(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repoDir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := gitTestCommand(repoDir, args...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@e",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@e",
		)
		out, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "git %s: %s", strings.Join(args, " "), out)
		return strings.TrimSpace(string(out))
	}

	git("init", "-q")
	git("config", "commit.gpgsign", "false")

	// 100 distinct root commits to merge (distinct messages, otherwise
	// identical commit-tree calls produce one deduplicated object); each
	// "parent <sha1>\n" line is 48 bytes, so the octopus header comfortably
	// exceeds MaxHdr.
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, "f.txt"), []byte("x\n"), 0o644))
	git("add", "f.txt")
	tree := git("write-tree")
	parents := make([]string, 0, 100)
	commitTreeArgs := []string{"commit-tree", tree, "-m", "octopus of unusual size"}
	for i := range 100 {
		parents = append(parents, git("commit-tree", tree, "-m", fmt.Sprintf("root %d", i)))
	}
	for _, p := range parents {
		commitTreeArgs = append(commitTreeArgs, "-p", p)
	}
	octopus := git(commitTreeArgs...)
	git("update-ref", "refs/heads/main", octopus)
	git("repack", "-adq")

	oid, err := ParseHash(octopus)
	require.NoError(t, err)

	packDir := filepath.Join(repoDir, ".git", "objects", "pack")
	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()

	// The old header path fails on this commit — pinned so this test starts
	// failing (and gets updated) if readCommitHeader ever learns to cope.
	_, err = st.readCommitHeader(oid)
	require.Error(t, err, "expected the MaxHdr-capped header read to fail on a >4 KiB header")

	// The payload path must succeed and match the oracle.
	want := gitCatFile(t, repoDir, "commit", oid)
	got, err := st.readCommitPayload(oid)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// Attribution end-to-end through the metaCache.
	mc := newMetaCache(nil, st)
	meta, err := mc.get(oid)
	require.NoError(t, err)
	require.Equal(t, "t", meta.Author.Name)
	require.Equal(t, "t@e", meta.Author.Email)
	require.Equal(t, "octopus of unusual size\n", meta.Message)
}
