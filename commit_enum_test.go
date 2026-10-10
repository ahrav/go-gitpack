package objstore

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireSameCommitSet(t *testing.T, want, got []commitInfo) {
	t.Helper()
	require.Len(t, got, len(want))
	wm := make(map[Hash]commitInfo, len(want))
	for _, c := range want {
		wm[c.OID] = c
	}
	for _, c := range got {
		w, ok := wm[c.OID]
		require.Truef(t, ok, "commit %s not in the reference set", c.OID)
		require.Equal(t, w.TreeOID, c.TreeOID, c.OID)
		require.Equal(t, w.ParentOIDs, c.ParentOIDs, c.OID)
		require.Equal(t, w.Timestamp, c.Timestamp, c.OID)
	}
}

func refWalkCommits(t *testing.T, s *HistoryScanner) []commitInfo {
	t.Helper()
	var out []commitInfo
	require.NoError(t, s.walkCommitsFromRefsOrdered(func(c commitInfo) error {
		out = append(out, c)
		return nil
	}))
	return out
}

// TestLoadFromRefs_MatchesRefWalk pins the pack-enumeration load to the
// reachable DAG walk on every packed fixture: the same commits with the
// same headers, in parent-first order.
func TestLoadFromRefs_MatchesRefWalk(t *testing.T) {
	for _, repo := range []string{"simple-linear", "with-merges", "no-commit-graph", "large-repo", "very-large-repo-1k", "super-large-repo-10k"} {
		s := createScannerForRepo(t, repo)
		want := refWalkCommits(t, s)
		got, _, _, err := s.loadFromRefs()
		require.NoError(t, err, repo)
		requireSameCommitSet(t, want, got)
		ordered := orderCommitsParentFirst(append([]commitInfo(nil), want...))
		for i := range ordered {
			require.Equal(t, ordered[i].OID, got[i].OID, "%s: position %d", repo, i)
		}
		s.Close()
	}
}

// TestEnumeratePackedCommits_CoversWholeCommits pins the enumeration to the
// set of commits stored whole in the packs, with headers equal to what
// readCommitHeader parses, whatever the worker count.
func TestEnumeratePackedCommits_CoversWholeCommits(t *testing.T) {
	s := createScannerForRepo(t, "very-large-repo-1k")
	defer s.Close()
	got := s.store.enumeratePackedCommits(4)
	var want []commitInfo
	for _, pf := range s.store.packs {
		for i, e := range pf.entries {
			typ, _, err := peekObjectType(pf.pack, e.offset)
			require.NoError(t, err)
			if typ != ObjCommit {
				continue
			}
			hdr, err := s.store.readCommitHeader(pf.oidTable[i])
			require.NoError(t, err)
			info, err := parseCommitInfoFromHeader(pf.oidTable[i], hdr)
			require.NoError(t, err)
			want = append(want, info)
		}
	}
	require.NotEmpty(t, want)
	requireSameCommitSet(t, want, got)
	requireSameCommitSet(t, want, s.store.enumeratePackedCommits(1))
}

// buildEnumRepo creates a repository whose history mixes every shape the
// load must cover: packed commits, a commit left loose after the repack, an
// annotated tag as a ref tip, and a packed commit that a deleted branch made
// unreachable. It returns the .git directory, the OIDs of the commits git
// reports reachable, and the unreachable commit.
func buildEnumRepo(t *testing.T) (gitDir string, reachable map[Hash]bool, unreachable Hash) {
	t.Helper()
	requireGit(t)
	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet", "-b", "main")
	write := func(name, content string) {
		require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte(content), 0o644))
		runGit(t, repo, "add", name)
	}
	write("a.txt", "one\n")
	runGit(t, repo, "commit", "-q", "-m", "one")
	write("b.txt", "two\n")
	runGit(t, repo, "commit", "-q", "-m", "two")
	runGit(t, repo, "tag", "-a", "-m", "release", "v1")
	runGit(t, repo, "checkout", "-q", "-b", "side")
	write("c.txt", "side\n")
	runGit(t, repo, "commit", "-q", "-m", "side")
	unreachable = gitRevParse(t, repo, "side")
	runGit(t, repo, "checkout", "-q", "main")
	// Pack everything, including the side branch, then drop the branch so
	// the pack holds an unreachable commit.
	runGit(t, repo, "repack", "-a", "-d", "-q")
	runGit(t, repo, "branch", "-D", "side")
	// A commit after the repack stays loose.
	write("d.txt", "loose\n")
	runGit(t, repo, "commit", "-q", "-m", "loose")

	reachable = map[Hash]bool{}
	for _, ref := range []string{"main", "main~1", "main~2"} {
		reachable[gitRevParse(t, repo, ref)] = true
	}
	return filepath.Join(repo, ".git"), reachable, unreachable
}

func gitRevParse(t *testing.T, repo, ref string) Hash {
	t.Helper()
	out, err := gitTestCommand(repo, "rev-parse", ref+"^{commit}").Output()
	require.NoError(t, err)
	h, err := ParseHash(string(out[:40]))
	require.NoError(t, err)
	return h
}

// TestLoadFromRefs_MixedStorageAndUnreachable pins the reachable-walk
// semantics over the enumeration: packed commits reachable only through a
// loose commit and an annotated tag are loaded, and a packed commit no ref
// reaches is left out.
func TestLoadFromRefs_MixedStorageAndUnreachable(t *testing.T) {
	gitDir, reachable, unreachable := buildEnumRepo(t)
	s, err := NewHistoryScanner(gitDir)
	require.NoError(t, err)
	defer s.Close()

	// The pack holds the unreachable commit, so the enumeration sees it.
	var enumerated []Hash
	for _, c := range s.store.enumeratePackedCommits(2) {
		enumerated = append(enumerated, c.OID)
	}
	require.Contains(t, enumerated, unreachable)

	got, _, _, err := s.loadFromRefs()
	require.NoError(t, err)
	var oids []Hash
	for _, c := range got {
		oids = append(oids, c.OID)
	}
	sort.Slice(oids, func(i, j int) bool { return oids[i].String() < oids[j].String() })
	require.Len(t, oids, len(reachable))
	for _, oid := range oids {
		require.Truef(t, reachable[oid], "loaded %s, which no ref reaches", oid)
	}
	require.NotContains(t, oids, unreachable)
	requireSameCommitSet(t, refWalkCommits(t, s), got)
}

// TestLoadFromRefs_LooseOnlyRepository covers a history with no packs at
// all: the enumeration finds nothing and every commit is read through the
// walk.
func TestLoadFromRefs_LooseOnlyRepository(t *testing.T) {
	requireGit(t)
	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet", "-b", "main")
	for i, name := range []string{"a", "b", "c"} {
		require.NoError(t, os.WriteFile(filepath.Join(repo, name), []byte{byte('0' + i), '\n'}, 0o644))
		runGit(t, repo, "add", name)
		runGit(t, repo, "commit", "-q", "-m", name)
	}
	s, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	require.NoError(t, err)
	defer s.Close()
	require.Empty(t, s.store.enumeratePackedCommits(2))
	got, _, _, err := s.loadFromRefs()
	require.NoError(t, err)
	require.Len(t, got, 3)
	requireSameCommitSet(t, refWalkCommits(t, s), got)
}

// TestBuildCommitGraphIndexed_MatchesLookupBuilder pins the remapped parent
// indexes to the OID-lookup builder on the loaded commits of every packed
// fixture, and the ordering permutation to the ordered list.
func TestBuildCommitGraphIndexed_MatchesLookupBuilder(t *testing.T) {
	for _, repo := range []string{"with-merges", "large-repo", "very-large-repo-1k"} {
		s := createScannerForRepo(t, repo)
		ordered, parents, perm, err := s.loadFromRefs()
		require.NoError(t, err, repo)
		want := buildCommitGraphFromCommits(ordered)
		got := buildCommitGraphIndexed(ordered, parents, perm)
		require.Equal(t, want.OrderedOIDs, got.OrderedOIDs, repo)
		require.Equal(t, want.TreeOIDs, got.TreeOIDs, repo)
		require.Equal(t, want.Timestamps, got.Timestamps, repo)
		require.Equal(t, want.OIDToIndex, got.OIDToIndex, repo)
		require.Equal(t, want.Parents, got.Parents, repo)
		require.Equal(t, len(want.parentIndices), len(got.parentIndices), repo)
		for i := range want.parentIndices {
			require.Equalf(t, want.parentIndices[i], got.parentIndices[i], "%s: commit %d", repo, i)
		}
		// Each ordered commit sits at perm[k] in the pre-order list the
		// walk produced, and parents index that list.
		require.Len(t, perm, len(ordered))
		require.Len(t, parents.start, len(ordered)+1)
		require.Equal(t, parents.start[len(ordered)], int32(len(parents.idx)))
		s.Close()
	}
}
