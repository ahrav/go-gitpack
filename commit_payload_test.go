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
			// private, uncorrupted copy. Scribbling over the first result
			// makes a shared buffer visible in the second.
			for i := range got {
				got[i] = ^got[i]
			}
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
// returning payload bytes, on both the loose and the packed branch.
func TestReadCommitPayload_NotACommit(t *testing.T) {
	t.Run("loose", func(t *testing.T) {
		objectsDir := t.TempDir()
		blobOID := writeLooseObject(t, objectsDir, "blob", []byte("not a commit"))

		st := &store{objectsDir: objectsDir}
		_, err := st.readCommitPayload(blobOID)
		require.ErrorIs(t, err, ErrObjectNotCommit)
	})

	t.Run("packed", func(t *testing.T) {
		repoDir, packDir := buildCommitShapesRepo(t)
		out, err := gitTestCommand(repoDir, "rev-parse", "HEAD~1:f.txt").Output()
		require.NoError(t, err)
		blobOID, err := ParseHash(strings.TrimSpace(string(out)))
		require.NoError(t, err)

		st, err := OpenForTesting(packDir)
		require.NoError(t, err)
		defer st.Close()
		_, _, inPack := st.findPackedObject(blobOID)
		require.True(t, inPack, "fixture blob must be packed")

		_, err = st.readCommitPayload(blobOID)
		require.ErrorIs(t, err, ErrObjectNotCommit)
	})
}

// writeLooseObject stores body as a loose object of the given type under
// objectsDir and returns its OID.
func writeLooseObject(t *testing.T, objectsDir, typ string, body []byte) Hash {
	t.Helper()
	return writeLooseObjectDeclaring(t, objectsDir, typ, body, len(body))
}

// writeLooseObjectDeclaring is writeLooseObject with an explicit header size,
// so tests can produce objects whose declared size disagrees with the body.
func writeLooseObjectDeclaring(t *testing.T, objectsDir, typ string, body []byte, declared int) Hash {
	t.Helper()
	objType, ok := parseLooseObjectType(typ)
	require.True(t, ok)
	oid := calculateHash(objType, body)
	path := looseObjectPath(objectsDir, oid)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))

	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	_, err := fmt.Fprintf(zw, "%s %d\x00", typ, declared)
	require.NoError(t, err)
	_, err = zw.Write(body)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	require.NoError(t, os.WriteFile(path, compressed.Bytes(), 0o644))
	return oid
}

// TestReadCommitPayload_HonorsVerifyCRC pins the store's CRC policy on the
// attribution path: with VerifyCRC set, a plain packed commit whose index
// CRC disagrees with the pack bytes is rejected instead of being returned
// (and cached) unverified. The index entries are corrupted in memory after
// open so the pack bytes stay valid and inflation itself succeeds.
func TestReadCommitPayload_HonorsVerifyCRC(t *testing.T) {
	repoDir, packDir := buildCommitShapesRepo(t)
	shapes := classifyCommitShapes(t, repoDir, packDir)
	require.NotEmpty(t, shapes.plain)
	require.NotEmpty(t, shapes.delta)

	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()

	plain, delta := shapes.plain[0], shapes.delta[0]
	for _, pf := range st.packs {
		for i := range pf.entries {
			pf.entries[i].crc ^= 0xFFFFFFFF
		}
	}

	// Verification off: corrupt index CRCs are irrelevant.
	_, err = st.readCommitPayload(plain)
	require.NoError(t, err)

	st.VerifyCRC = true
	_, err = st.readCommitPayload(plain)
	require.Error(t, err, "plain packed commit must fail CRC verification")
	require.Contains(t, err.Error(), "crc mismatch")

	_, err = st.readCommitPayload(delta)
	require.Error(t, err, "delta commit must fail CRC verification")
	require.Contains(t, err.Error(), "crc mismatch")
}

// looseCommitWithMessage writes a well-formed loose commit whose message is
// msgLen bytes of compressible content and returns the store plus its OID.
func looseCommitWithMessage(t *testing.T, msgLen int) (*store, Hash) {
	t.Helper()
	var body bytes.Buffer
	body.WriteString("tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n")
	body.WriteString("author t <t@e> 1700000000 +0000\n")
	body.WriteString("committer t <t@e> 1700000000 +0000\n\n")
	body.Write(bytes.Repeat([]byte("m"), msgLen))
	objectsDir := t.TempDir()
	oid := writeLooseObject(t, objectsDir, "commit", body.Bytes())
	return &store{objectsDir: objectsDir}, oid
}

// allocatedBytes returns the heap bytes allocated while fn runs.
func allocatedBytes(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestReadCommitPayload_LooseCapAppliesBeforeInflate pins that the payload
// cap gates a loose commit on its declared size, before the body is
// inflated: an over-cap loose commit is rejected without allocating its
// body.
func TestReadCommitPayload_LooseCapAppliesBeforeInflate(t *testing.T) {
	const msgLen = 16 << 20
	st, oid := looseCommitWithMessage(t, msgLen)

	saved := maxCommitPayload
	maxCommitPayload = 1 << 20
	t.Cleanup(func() { maxCommitPayload = saved })

	// Warm the reader pools so their first-use allocations stay out of the
	// measured call.
	_, _ = st.readCommitPayload(oid)

	var err error
	n := allocatedBytes(func() { _, err = st.readCommitPayload(oid) })
	require.ErrorIs(t, err, errCommitPayloadTooLarge)
	require.Lessf(t, n, uint64(msgLen/4),
		"over-cap loose commit allocated %d bytes; the body must not be inflated", n)
}

// TestReadCommitHeader_LooseStreamsHeader pins that the header read of a
// loose commit stops at the committer line instead of inflating the whole
// object. This is the fallback path for over-cap commits, so it bounds the
// attribution memory for loose commits the same way the cap does.
func TestReadCommitHeader_LooseStreamsHeader(t *testing.T) {
	const msgLen = 16 << 20
	st, oid := looseCommitWithMessage(t, msgLen)

	_, _ = st.readCommitHeader(oid) // warm pools

	var hdr []byte
	var err error
	n := allocatedBytes(func() { hdr, err = st.readCommitHeader(oid) })
	require.NoError(t, err)
	require.True(t, bytes.HasSuffix(hdr, []byte("committer t <t@e> 1700000000 +0000\n")), "header %q", hdr)
	require.Lessf(t, n, uint64(msgLen/4),
		"loose header read allocated %d bytes; the message must not be inflated", n)

	// A declared size shorter than the header marks the object corrupt in
	// Git's terms; the streaming read must stay within it rather than
	// accept bytes past the declared body as metadata.
	objectsDir := t.TempDir()
	body := []byte("tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\nauthor t <t@e> 1 +0000\ncommitter t <t@e> 1 +0000\n\nm\n")
	short := writeLooseObjectDeclaring(t, objectsDir, "commit", body, 10)
	_, err = (&store{objectsDir: objectsDir}).readCommitHeader(short)
	require.Error(t, err, "header read must fail when the declared size ends before the committer line")

	// A declared size above the cap with a body that ends after the committer
	// line is a corrupt object, not an oversized commit: the fallback must
	// validate the body length it skips, as readLooseObject does.
	saved := maxCommitPayload
	maxCommitPayload = 1 << 20
	t.Cleanup(func() { maxCommitPayload = saved })
	objectsDir = t.TempDir()
	bloated := writeLooseObjectDeclaring(t, objectsDir, "commit", body, 2<<20)
	bst := &store{objectsDir: objectsDir}
	_, err = bst.readCommitHeader(bloated)
	require.Error(t, err, "header read must reject a body shorter than its over-cap declared size")
	_, err = newMetaCache(nil, bst).get(bloated)
	require.Error(t, err, "attribution must reject a corrupt over-cap loose commit")

	// The over-cap attribution path ends here: author from the header,
	// empty message, no body allocation.
	mc := newMetaCache(nil, st)
	var meta CommitMetadata
	n = allocatedBytes(func() { meta, err = mc.get(oid) })
	require.NoError(t, err)
	require.Equal(t, "t", meta.Author.Name)
	require.Empty(t, meta.Message)
	require.Lessf(t, n, uint64(msgLen/4), "over-cap attribution allocated %d bytes", n)
}

// TestCommitPayload_HeaderLargerThanMaxHdr pins that a commit whose header
// exceeds MaxHdr (4096 bytes), here a 100-parent octopus merge, is attributed
// through the payload path, which has no header cap, and that its message
// byte-matches the git oracle.
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

func TestCommitPayload_OverCapFallsBackToHeader(t *testing.T) {
	repoDir, packDir := buildCommitShapesRepo(t)
	shapes := classifyCommitShapes(t, repoDir, packDir)
	require.NotEmpty(t, shapes.plain)
	require.NotEmpty(t, shapes.delta)
	require.NotEmpty(t, shapes.loose)

	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()

	saved := maxCommitPayload
	maxCommitPayload = 64 // below every fixture commit
	t.Cleanup(func() { maxCommitPayload = saved })

	mc := newMetaCache(nil, st)
	for shape, oids := range map[string][]Hash{"plain": shapes.plain, "delta": shapes.delta, "loose": shapes.loose} {
		for _, oid := range oids {
			_, err := st.readCommitPayload(oid)
			require.ErrorIsf(t, err, errCommitPayloadTooLarge, "%s commit %s", shape, oid)

			meta, err := mc.get(oid)
			require.NoErrorf(t, err, "%s commit %s", shape, oid)
			require.Equalf(t, "t", meta.Author.Name, "%s commit %s", shape, oid)
			require.Equalf(t, "t@e", meta.Author.Email, "%s commit %s", shape, oid)
			require.Emptyf(t, meta.Message, "%s commit %s", shape, oid)
		}
	}
}
