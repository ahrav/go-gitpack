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
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

func TestReadLooseObjectLimitedRejectsDeclaredSizeBeforeBody(t *testing.T) {
	objectsDir := t.TempDir()
	oid := Hash{0x12, 0x34}
	path := looseObjectPath(objectsDir, oid)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))

	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	_, err := zw.Write([]byte("commit 5\x00hello"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	require.NoError(t, os.WriteFile(path, compressed.Bytes(), 0o644))

	st := &store{objectsDir: objectsDir}
	_, _, err = st.readLooseObjectLimited(oid, 4)
	require.ErrorContains(t, err, "exceeds 4 byte limit")
}

// A loose object whose decompressed stream puts the header's NUL terminator
// far past the longest valid header ("<type> <size>\0") is rejected once
// the header limit is reached, before the body limit applies.
func TestReadLooseObjectLimitedBoundsHeader(t *testing.T) {
	objectsDir := t.TempDir()
	oid := Hash{0x56, 0x78}
	path := looseObjectPath(objectsDir, oid)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))

	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	_, err := zw.Write([]byte("commit " + strings.Repeat("9", 1<<20) + "\x00hello"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	require.NoError(t, os.WriteFile(path, compressed.Bytes(), 0o644))

	st := &store{objectsDir: objectsDir}
	_, _, err = st.readLooseObjectLimited(oid, 4)
	require.ErrorContains(t, err, "header exceeds")
}

// corruptIdxCRCTable flips every byte of the pack index's CRC-32 table, so
// any read that consults the index CRC fails verification.
func corruptIdxCRCTable(t *testing.T, packDir string) {
	t.Helper()
	idxs, err := filepath.Glob(filepath.Join(packDir, "*.idx"))
	require.NoError(t, err)
	require.Len(t, idxs, 1)
	data, err := os.ReadFile(idxs[0])
	require.NoError(t, err)
	const headerAndFanout = 8 + 256*4
	objCount := int(binary.BigEndian.Uint32(data[headerAndFanout-4:]))
	crcBase := headerAndFanout + objCount*20
	for i := range objCount * 4 {
		data[crcBase+i] ^= 0xFF
	}
	// Keep the index self-consistent: its trailing SHA-1 covers everything
	// before it, and open rejects an index whose trailer mismatches.
	sum := sha1.Sum(data[:len(data)-sha1.Size])
	copy(data[len(data)-sha1.Size:], sum[:])
	// git writes pack files read-only.
	require.NoError(t, os.Chmod(idxs[0], 0o644))
	require.NoError(t, os.WriteFile(idxs[0], data, 0o644))
}

// With SetVerifyCRC enabled every packed read is checked against the index
// CRC, including the plain packed commit fast path of readCommitPayload.
func TestReadCommitPayload_VerifiesCRCOnPlainPackedCommits(t *testing.T) {
	repoDir, packDir := buildCommitShapesRepo(t)
	shapes := classifyCommitShapes(t, repoDir, packDir)
	require.NotEmpty(t, shapes.plain)
	corruptIdxCRCTable(t, packDir)

	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()
	st.VerifyCRC = true

	oid := shapes.plain[0]
	_, _, err = st.getMaterialized(oid)
	require.ErrorContains(t, err, "crc mismatch", "the generic materializing read must reject the corrupt index")

	_, err = st.readCommitPayload(oid)
	require.ErrorContains(t, err, "crc mismatch", "the commit payload fast path must honor VerifyCRC")
}

// buildBigDeltaCommitRepo returns a repository whose second commit carries a
// 1 MiB message nearly identical to the first commit's, so an aggressive
// repack stores it as a delta against the first. It returns the pack dir
// and the delta commit's OID, and skips when git stored the commit whole.
func buildBigDeltaCommitRepo(t *testing.T) (packDir string, delta Hash, payload []byte) {
	t.Helper()
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
	body := strings.Repeat("the same very long commit message line\n", (1<<20)/40)
	msgFile := filepath.Join(t.TempDir(), "msg")
	for c := range 2 {
		require.NoError(t, os.WriteFile(filepath.Join(repoDir, "f.txt"), []byte(fmt.Sprintf("content %d\n", c)), 0o644))
		require.NoError(t, os.WriteFile(msgFile, []byte(fmt.Sprintf("commit %d\n%s", c, body)), 0o644))
		git("add", "f.txt")
		git("commit", "-q", "-F", msgFile)
	}
	git("repack", "-adf", "--window=250", "--depth=50")
	packDir = filepath.Join(repoDir, ".git", "objects", "pack")
	shapes := classifyCommitShapes(t, repoDir, packDir)
	if len(shapes.delta) == 0 {
		t.Skip("git stored both big commits whole; no delta commit to test")
	}
	delta = shapes.delta[0]
	return packDir, delta, gitCatFile(t, repoDir, "commit", delta)
}

// A delta-chained commit larger than the payload cap is rejected from its
// delta header. The rejected read inflates the chain's root base (a 1 MiB
// commit here) and nothing for the over-cap target itself, so it allocates
// well under two payloads; a read that reconstructs the target first
// allocates at least the base plus the target.
func TestReadCommitPayload_DeltaCommitCapPrecedesMaterialization(t *testing.T) {
	packDir, oid, payload := buildBigDeltaCommitRepo(t)

	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()

	saved := maxCommitPayload
	t.Cleanup(func() { maxCommitPayload = saved })
	maxCommitPayload = len(payload) / 4

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = st.readCommitPayload(oid)
	runtime.ReadMemStats(&after)
	require.ErrorContains(t, err, "exceeds")
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("commit payload %d bytes, cap %d, rejected read allocated %d bytes", len(payload), maxCommitPayload, allocated)
	require.Lessf(t, allocated, uint64(len(payload))*3/2,
		"rejecting an over-cap delta commit must not allocate its reconstructed payload")
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
