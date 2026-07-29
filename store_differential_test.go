// store_differential_test.go cross-checks the object-materialization paths —
// iterative delta-chain resolution and zero-copy inflation out of the mmap'd
// pack — against the authoritative Git implementation.
//
// The oracle is `git cat-file`: for every object in a repository whose pack has
// been repacked into deep delta chains, the bytes produced by the store must be
// byte-identical to the bytes Git produces. This exercises, on real Git-authored
// data (real copy+insert delta commands, multi-hop ofs-delta chains, the
// ping-pong arena, and the offset-cache short-circuit):
//
//   - inflateDeltaChainStreaming   (via getMaterialized, cacheResult=true)
//   - inflateDeltaChainBorrowed    (via getPackedObjectNoCache, cacheResult=false)
//   - readRawObject / inflateExact (non-delta objects, both zlib backends)
//
// The two delta entry points are compared against Git *and* against each other,
// which pins the invariant that the borrowed (consume-and-discard) path returns
// exactly the same bytes as the streaming (cached) path.

package objstore

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildDeltaHeavyRepo initializes a Git repository whose objects are repacked
// into deep delta chains, so the store's delta-resolution paths are exercised
// on realistic Git-authored data rather than synthetic fixtures.
//
// It returns the repository root and its pack directory. The test is skipped if
// the git executable is unavailable.
func buildDeltaHeavyRepo(t *testing.T) (repoDir, packDir string) {
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
			"GIT_AUTHOR_DATE=2000-01-01T00:00:00Z",
			"GIT_COMMITTER_DATE=2000-01-01T00:00:00Z",
		)
		out, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "git %s: %s", strings.Join(args, " "), out)
	}

	git("init", "-q")
	git("config", "commit.gpgsign", "false")

	// Content shapes chosen to exercise distinct materialization paths:
	//
	//   a.txt  strict-append growth. Each version is a superset of the prior
	//          one, which is exactly what Git's delta selector chains — this
	//          produces the multi-hop chains (requireHasDeltas enforces a
	//          deepest chain of at least 2 hops) that drive the ping-pong arena
	//          and the offset-cache short-circuit. The fixture blobs stay a few
	//          KiB, far below the 16 MiB arena half, so the arena grow path is
	//          not covered here.
	//   b.txt  wholesale-rewritten line file. Copy+insert delta commands.
	//   c.bin  wholesale-rewritten binary blob. Non-delta / insert-heavy.
	//
	// The large repack window/depth lets the selector build the deepest chains
	// the content allows.
	const commits = 120
	writeFile := func(name string, data []byte) {
		require.NoError(t, os.WriteFile(filepath.Join(repoDir, name), data, 0o644))
	}

	var aGrow bytes.Buffer
	for c := 0; c < commits; c++ {
		fmt.Fprintf(&aGrow, "block %d: ", c)
		aGrow.Write(bytes.Repeat([]byte{byte(c)}, 64))
		aGrow.WriteByte('\n')
		writeFile("a.txt", append([]byte(nil), aGrow.Bytes()...))

		bLines := make([]string, 100)
		for i := range bLines {
			bLines[i] = fmt.Sprintf("b line %d rev %d: mutated payload %d", i, c, (c*31+i)%97)
		}
		writeFile("b.txt", []byte(strings.Join(bLines, "\n")+"\n"))

		writeFile("c.bin", bytes.Repeat([]byte{byte(c), 0x00, 0xff, byte(c * 7)}, 300+c))

		git("add", "-A")
		git("commit", "-q", "-m", fmt.Sprintf("commit %d", c))
	}

	// Force everything into a single pack with deep delta chains. -f discards
	// existing deltas and recomputes them; the large window/depth maximizes
	// chain length so the walk-up, ping-pong, and offset-cache paths all run.
	git("repack", "-a", "-d", "-f", "--window=250", "--depth=50", "--threads=1")

	packDir = filepath.Join(repoDir, ".git", "objects", "pack")
	return repoDir, packDir
}

// gitObject is a single entry from `git cat-file --batch-all-objects`.
type gitObject struct {
	oid Hash
	typ string
}

// listGitObjects enumerates every object in the repository together with its
// type using `git cat-file --batch-check`.
func listGitObjects(t *testing.T, repoDir string) []gitObject {
	t.Helper()
	cmd := exec.Command("git", "-C", repoDir, "cat-file",
		"--batch-all-objects", "--batch-check=%(objectname) %(objecttype)")
	out, err := cmd.Output()
	require.NoError(t, err)

	var objs []gitObject
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 2 {
			continue
		}
		h, err := ParseHash(fields[0])
		require.NoError(t, err)
		objs = append(objs, gitObject{oid: h, typ: fields[1]})
	}
	require.NoError(t, sc.Err())
	return objs
}

// gitCatFileBatch returns the raw, authoritative bytes of every object's
// content (no "<type> <size>\0" header), matching what the store materializes.
//
// One `git cat-file --batch` process serves the whole object list. Spawning a
// process per object instead would cost one fork+exec per object, and these
// tests walk every object in a 120-commit repository twice over — enough
// subprocess churn to dominate a default `go test` run.
//
// The reported type is checked against the enumerated one as a side effect: the
// two come from different Git invocations (cat-file here, rev-list --objects in
// listGitObjects), so a disagreement means the fixture shifted under the test.
func gitCatFileBatch(t *testing.T, repoDir string, objs []gitObject) map[Hash][]byte {
	t.Helper()

	var stdin bytes.Buffer
	for _, o := range objs {
		fmt.Fprintln(&stdin, o.oid.String())
	}

	cmd := gitTestCommand(repoDir, "cat-file", "--batch")
	cmd.Stdin = &stdin
	out, err := cmd.Output()
	require.NoError(t, err, "git cat-file --batch")

	want := make(map[Hash]string, len(objs))
	for _, o := range objs {
		want[o.oid] = o.typ
	}

	// Each record is "<oid> SP <type> SP <size> LF", then exactly <size> content
	// bytes, then a trailing LF. Content is binary, so it must be read by length
	// rather than scanned for a delimiter.
	br := bufio.NewReader(bytes.NewReader(out))
	contents := make(map[Hash][]byte, len(objs))
	for range objs {
		header, err := br.ReadString('\n')
		require.NoError(t, err, "reading cat-file record header")

		fields := strings.Fields(strings.TrimSuffix(header, "\n"))
		require.Lenf(t, fields, 3, "unexpected cat-file record header %q", header)

		oid, err := ParseHash(fields[0])
		require.NoError(t, err)
		require.Equalf(t, want[oid], fields[1], "cat-file type disagrees for %s", oid)

		size, err := strconv.Atoi(fields[2])
		require.NoError(t, err)

		body := make([]byte, size)
		_, err = io.ReadFull(br, body)
		require.NoErrorf(t, err, "reading %d content bytes for %s", size, oid)

		delim, err := br.ReadByte()
		require.NoError(t, err)
		require.Equalf(t, byte('\n'), delim, "missing record terminator for %s", oid)

		contents[oid] = body
	}

	require.Len(t, contents, len(objs), "every requested object must be returned")
	return contents
}

// TestStore_DifferentialAgainstGit is the primary regression guard for the
// delta-materialization and inflate optimizations: it proves that every object
// in a delta-heavy repository round-trips byte-for-byte through the store, via
// both the streaming (cached) and borrowed (consume-and-discard) delta paths.
func TestStore_DifferentialAgainstGit(t *testing.T) {
	repoDir, packDir := buildDeltaHeavyRepo(t)

	// Sanity: the pack must actually contain delta objects, otherwise the test
	// would silently exercise none of the code it is meant to protect.
	requireHasDeltas(t, packDir)

	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()

	objs := listGitObjects(t, repoDir)
	require.NotEmpty(t, objs)
	want := gitCatFileBatch(t, repoDir, objs)

	var checkedPacked, checkedLoose int
	for _, o := range objs {
		want := want[o.oid]

		// Streaming path (getMaterialized): full body, cached, commit
		// fast-path disabled.
		gotStream, streamType, err := st.getMaterialized(o.oid)
		require.NoErrorf(t, err, "getMaterialized %s (%s)", o.oid, o.typ)
		require.Equalf(t, want, gotStream,
			"streaming mismatch for %s (%s)", o.oid, o.typ)
		// The type travels with the bytes: callers branch on it, so returning
		// the right body under the wrong type is a regression the byte
		// comparison alone cannot see.
		require.Equalf(t, o.typ, streamType.String(),
			"streaming type mismatch for %s", o.oid)

		// Determinism: a second read (now cache-warm) must be identical.
		gotWarm, warmType, err := st.getMaterialized(o.oid)
		require.NoError(t, err)
		require.Equalf(t, want, gotWarm, "warm-cache mismatch for %s (%s)", o.oid, o.typ)
		require.Equalf(t, o.typ, warmType.String(),
			"warm-cache type mismatch for %s", o.oid)

		// Borrowed path (getPackedObjectNoCache): only meaningful for packed
		// objects, since it takes an explicit (pack, offset).
		//
		// This path is what guarantees the multi-hop ping-pong arena is
		// actually reached. inflateDeltaChainBorrowed passes a nil offset
		// cache, so its walk never short-circuits on a hop some earlier
		// iteration materialized; an object at chain depth d therefore always
		// builds a d-entry stack here. requireHasDeltas asserts a deepest
		// chain of at least 2, so at least one object in this loop resolves
		// through the arena rather than the single-hop fast path. The
		// streaming reads above cannot carry that guarantee: they share one
		// store, so ascending enumeration order leaves each object's base
		// already published under its pack offset.
		if p, off, ok := st.findPackedObject(o.oid); ok {
			gotBorrowed, borrowedType, err := st.getPackedObjectNoCache(p, off, o.oid)
			require.NoErrorf(t, err, "getPackedObjectNoCache %s (%s)", o.oid, o.typ)
			require.Equalf(t, want, gotBorrowed,
				"borrowed mismatch for %s (%s)", o.oid, o.typ)
			require.Equalf(t, o.typ, borrowedType.String(),
				"borrowed type mismatch for %s", o.oid)
			checkedPacked++
		} else {
			checkedLoose++
		}
	}

	t.Logf("verified %d objects (%d packed via both paths, %d loose)",
		len(objs), checkedPacked, checkedLoose)
	require.Positive(t, checkedPacked, "expected packed objects to compare")
}

// requireHasDeltas fails the test unless the pack contains delta objects and at
// least one multi-hop chain, so a repack that produced no deltas — or only
// single-hop ones, which never reach the ping-pong arena — cannot give a false
// green. It parses the chain-length histogram that `git verify-pack -v` prints.
func requireHasDeltas(t *testing.T, packDir string) {
	t.Helper()
	idxs, err := filepath.Glob(filepath.Join(packDir, "*.idx"))
	require.NoError(t, err)
	require.NotEmpty(t, idxs, "no pack index in %s", packDir)

	out, err := exec.Command("git", "verify-pack", "-v", idxs[0]).Output()
	require.NoError(t, err)

	totalDeltas, maxChain := parseVerifyPackDeltaStats(out)
	require.Positive(t, totalDeltas, "repacked pack contains no delta objects")
	require.GreaterOrEqual(t, maxChain, 2,
		"repack produced no multi-hop chains; this test no longer covers the ping-pong path")
	t.Logf("pack contains %d delta objects, deepest chain = %d hops", totalDeltas, maxChain)
}

// parseVerifyPackDeltaStats extracts Git's chain-length histogram. Git uses
// "object" for a count of one and "objects" otherwise.
func parseVerifyPackDeltaStats(out []byte) (totalDeltas, maxChain int) {
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "chain length = ") {
			continue
		}
		rest := strings.TrimPrefix(line, "chain length = ")
		numStr, cntStr, ok := strings.Cut(rest, ":")
		if !ok {
			continue
		}
		countFields := strings.Fields(cntStr)
		if len(countFields) != 2 ||
			(countFields[1] != "object" && countFields[1] != "objects") {
			continue
		}
		length, err1 := strconv.Atoi(strings.TrimSpace(numStr))
		count, err2 := strconv.Atoi(countFields[0])
		if err1 != nil || err2 != nil {
			continue
		}
		totalDeltas += count
		if length > maxChain {
			maxChain = length
		}
	}
	return totalDeltas, maxChain
}

func TestParseVerifyPackDeltaStats(t *testing.T) {
	out := []byte(strings.Join([]string{
		"non delta: 4 objects",
		"chain length = 1: 1 object",
		"chain length = 2: 3 objects",
		"pack-example.pack: ok",
	}, "\n"))

	totalDeltas, maxChain := parseVerifyPackDeltaStats(out)
	require.Equal(t, 4, totalDeltas)
	require.Equal(t, 2, maxChain)
}

// TestStore_DifferentialUnderGOMAXPROCS1 repeats the differential check with a
// single OS thread. The materialization paths use pooled arenas and a shared
// offset cache; running serialized rules out scheduling-dependent divergence in
// those shared structures independent of the -race detector.
func TestStore_DifferentialUnderGOMAXPROCS1(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	repoDir, packDir := buildDeltaHeavyRepo(t)
	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()

	objs := listGitObjects(t, repoDir)
	want := gitCatFileBatch(t, repoDir, objs)
	for _, o := range objs {
		got, typ, err := st.getMaterialized(o.oid)
		require.NoError(t, err)
		require.Equalf(t, want[o.oid], got, "mismatch for %s (%s)", o.oid, o.typ)
		require.Equalf(t, o.typ, typ.String(), "type mismatch for %s", o.oid)
	}
}
