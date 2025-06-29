package objstore

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

// newHash creates a deterministic Hash whose first byte is the first byte of s.
func newHash(s string) Hash {
	var h Hash
	for i := 0; i < len(s) && i < len(h); i++ {
		h[i] = s[i]
	}
	return h
}

// newTree builds a *Tree whose sortedEntries slice is sorted by Name.
func newTree(entries ...treeEntry) *Tree {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return &Tree{sortedEntries: entries}
}

// buildCache constructs a treeCache with the given map already populated.
func buildCache(trees map[Hash]*Tree) *treeCache {
	tc := newTreeCache(nil) // Store is nil – not needed in unit tests.
	tc.mem = trees
	return tc
}

// capture helper: run walkDiff and collect every call into []change.
type change struct {
	Path     string
	Old, New Hash
	NewMode  uint32
}

func collect(tc *treeCache, parent, child Hash, prefix string) ([]change, error) {
	var out []change
	err := walkDiff(tc, parent, child, prefix, func(p string, old, new Hash, mode uint32) error {
		out = append(out, change{p, old, new, mode})
		return nil
	})
	return out, err
}

func equalChanges(a, b []change) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Slice(a, func(i, j int) bool { return a[i].Path < a[j].Path })
	sort.Slice(b, func(i, j int) bool { return b[i].Path < b[j].Path })
	return reflect.DeepEqual(a, b)
}

func TestWalkDiff_EmptyAndIdenticalTrees(t *testing.T) {
	empty := newTree()
	tc := buildCache(map[Hash]*Tree{{}: empty})

	// Empty vs empty.
	ch, err := collect(tc, Hash{}, Hash{}, "")
	assert.NoError(t, err)
	assert.Empty(t, ch, "empty vs empty should produce no changes")

	// Identical non-empty trees.
	treeOID := newHash("T")
	tc.mem[treeOID] = newTree(
		treeEntry{Name: "a.txt", Mode: 0100644, OID: newHash("a")},
		treeEntry{Name: "b.txt", Mode: 0100755, OID: newHash("b")},
	)
	ch, err = collect(tc, treeOID, treeOID, "")
	assert.NoError(t, err)
	assert.Empty(t, ch, "identical trees should produce no changes")
}

func TestWalkDiff_InsertModifyDeleteMode(t *testing.T) {
	// Blobs.
	oa := newHash("a1")
	na := newHash("a2")
	nb := newHash("b")
	parentOID := newHash("P")
	childOID := newHash("C")

	tc := buildCache(map[Hash]*Tree{
		parentOID: newTree(treeEntry{Name: "a.txt", Mode: 0100644, OID: oa}),
		childOID: newTree(
			treeEntry{Name: "a.txt", Mode: 0100644, OID: na}, // Content change.
			treeEntry{Name: "b.txt", Mode: 0100755, OID: nb}, // Insertion.
		),
	})

	got, err := collect(tc, parentOID, childOID, "")
	assert.NoError(t, err)

	want := []change{
		{"a.txt", oa, na, 0100644},
		{"b.txt", Hash{}, nb, 0100755},
	}
	assert.True(t, equalChanges(got, want), "insert/modify diff mismatch\nwant %+v\ngot  %+v", want, got)
}

func TestWalkDiff_ModeOnlyChange(t *testing.T) {
	h := newHash("x")
	pt := newTree(treeEntry{Name: "exec.sh", Mode: 0100644, OID: h})
	ct := newTree(treeEntry{Name: "exec.sh", Mode: 0100755, OID: h})
	tc := buildCache(map[Hash]*Tree{{1}: pt, {2}: ct})

	got, err := collect(tc, Hash{1}, Hash{2}, "")
	assert.NoError(t, err)
	want := []change{{"exec.sh", h, h, 0100755}}
	assert.True(t, equalChanges(got, want), "mode change not reported: %+v", got)
}

func TestWalkDiff_DeletionIgnored(t *testing.T) {
	h := newHash("d")
	pt := newTree(treeEntry{Name: "gone.txt", Mode: 0100644, OID: h})
	ct := newTree() // Deleted.
	tc := buildCache(map[Hash]*Tree{{1}: pt, {2}: ct})

	got, err := collect(tc, Hash{1}, Hash{2}, "")
	assert.NoError(t, err)
	assert.Empty(t, got, "deletion should be ignored")
}

func TestWalkDiff_RecursiveAndPrefix(t *testing.T) {
	// Directory structure.
	subParent := newHash("sp")
	subChild := newHash("sc")
	rootP := newHash("rp")
	rootC := newHash("rc")
	tc := buildCache(map[Hash]*Tree{
		subParent: newTree(treeEntry{Name: "f.txt", Mode: 0100644, OID: newHash("1")}),
		subChild: newTree(
			treeEntry{Name: "f.txt", Mode: 0100644, OID: newHash("2")}, // Modified.
			treeEntry{Name: "g.txt", Mode: 0100644, OID: newHash("3")}, // New.
		),
		rootP: newTree(treeEntry{Name: "dir", Mode: 040000, OID: subParent}),
		rootC: newTree(
			treeEntry{Name: "dir", Mode: 040000, OID: subChild},
			treeEntry{Name: "h.txt", Mode: 0100644, OID: newHash("4")}, // New at root.
		),
		{}: newTree(),
	})

	got, err := collect(tc, rootP, rootC, "prefix/")
	assert.NoError(t, err)

	want := []change{
		{"prefix/dir/f.txt", newHash("1"), newHash("2"), 0100644},
		{"prefix/dir/g.txt", Hash{}, newHash("3"), 0100644},
		{"prefix/h.txt", Hash{}, newHash("4"), 0100644},
	}
	assert.True(t, equalChanges(got, want), "recursive diff mismatch\nwant %+v\ngot  %+v", want, got)
}

func TestWalkDiff_SpecialNames(t *testing.T) {
	names := []string{
		"spaced file.txt", "dash-file.txt", "under_score.txt",
		"dot.name.txt", "weird@chars#$.txt",
	}

	entries := make([]treeEntry, len(names))
	for i, n := range names {
		entries[i] = treeEntry{Name: n, Mode: 0100644, OID: newHash(string('a' + byte(i)))}
	}
	parentOID, childOID := newHash("P"), newHash("C")
	tc := buildCache(map[Hash]*Tree{
		parentOID: newTree(), // Empty.
		childOID:  newTree(entries...),
	})
	got, err := collect(tc, parentOID, childOID, "")
	assert.NoError(t, err)
	assert.Len(t, got, len(names), "want %d additions", len(names))

	sort.Strings(names)
	sort.Slice(got, func(i, j int) bool { return got[i].Path < got[j].Path })

	for i, n := range names {
		assert.Equal(t, n, got[i].Path, "name preserved mismatch")
	}
}

func TestWalkDiff_FileDirConversions(t *testing.T) {
	// File → directory.
	dirOID := newHash("D")
	tc := buildCache(map[Hash]*Tree{
		{1}:    newTree(treeEntry{Name: "foo", Mode: 0100644, OID: newHash("file")}),
		{2}:    newTree(treeEntry{Name: "foo", Mode: 040000, OID: dirOID}),
		dirOID: newTree(treeEntry{Name: "bar", Mode: 0100644, OID: newHash("bar")}),
		{}:     newTree(),
	})
	got, err := collect(tc, Hash{1}, Hash{2}, "")
	assert.NoError(t, err)
	assert.Len(t, got, 1, "file→dir conversion should report one change")
	assert.Equal(t, "foo", got[0].Path, "file→dir conversion path mismatch")

	// Directory → file.
	tc = buildCache(map[Hash]*Tree{
		{3}:    newTree(treeEntry{Name: "foo", Mode: 040000, OID: dirOID}),
		{4}:    newTree(treeEntry{Name: "foo", Mode: 0100644, OID: newHash("file")}),
		dirOID: newTree(treeEntry{Name: "bar", Mode: 0100644, OID: newHash("bar")}),
		{}:     newTree(),
	})
	got, err = collect(tc, Hash{3}, Hash{4}, "")
	assert.NoError(t, err)
	assert.Len(t, got, 1, "dir→file conversion should report one change")
	assert.Equal(t, "foo", got[0].Path, "dir→file conversion path mismatch")
}

func TestWalkDiff_EmitErrorPropagates(t *testing.T) {
	tc := buildCache(map[Hash]*Tree{
		{1}: newTree(),
		{2}: newTree(treeEntry{Name: "a", Mode: 0100644, OID: newHash("a")}),
	})
	sentinel := errors.New("boom")
	err := walkDiff(tc, Hash{1}, Hash{2}, "", func(string, Hash, Hash, uint32) error {
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel, "emit error not propagated")
}

func TestWalkDiff_CacheMiss(t *testing.T) {
	tc := buildCache(map[Hash]*Tree{}) // Empty map, not nil.
	err := walkDiff(tc, Hash{}, newHash("x"), "", func(string, Hash, Hash, uint32) error {
		return nil
	})
	assert.Error(t, err, "expected error on cache miss")
}

func TestWalkEntry_FileAndDir(t *testing.T) {
	fileOID := newHash("F")
	dirOID := newHash("D")
	tc := buildCache(map[Hash]*Tree{
		dirOID: newTree(treeEntry{Name: "deep.txt", Mode: 0100644, OID: newHash("deep")}),
		{}:     newTree(),
	})
	// File.
	var got change
	err := walkEntry(tc, "p/", treeEntry{Name: "f.txt", Mode: 0100644, OID: fileOID}, newHash("old"),
		func(p string, old, new Hash, mode uint32) error {
			got = change{p, old, new, mode}
			return nil
		})
	assert.NoError(t, err)
	want := change{"p/f.txt", newHash("old"), fileOID, 0100644}
	assert.Equal(t, want, got, "walkEntry file mismatch")

	// Directory.
	var dirCalls []string
	err = walkEntry(tc, "", treeEntry{Name: "dir", Mode: 040000, OID: dirOID}, Hash{},
		func(p string, old, new Hash, mode uint32) error {
			dirCalls = append(dirCalls, p)
			return nil
		})
	assert.NoError(t, err)
	assert.Len(t, dirCalls, 1, "walkEntry dir should recurse once")
	if len(dirCalls) > 0 {
		assert.Equal(t, "dir/deep.txt", dirCalls[0], "walkEntry dir recurse path mismatch")
	}
}

func TestWalkDiff_DeepRecursion(t *testing.T) {
	depth := 40
	tc := buildCache(map[Hash]*Tree{{}: newTree()})

	leafFile := treeEntry{Name: "leaf.txt", Mode: 0100644, OID: newHash("leaf")}

	// Build from bottom up.
	for i := depth - 1; i >= 0; i-- {
		parentOID := newHash(fmt.Sprintf("P%d", i))
		childOID := newHash(fmt.Sprintf("C%d", i))

		childEntries := []treeEntry{}
		if i == depth-1 {
			// Bottom level: contains the leaf file.
			childEntries = append(childEntries, leafFile)
		} else {
			// Other levels: point to the NEXT level (i+1).
			nextChildOID := newHash(fmt.Sprintf("C%d", i+1))
			childEntries = append(childEntries, treeEntry{
				Name: "nested",
				Mode: 040000,
				OID:  nextChildOID, // Points to next level.
			})
		}

		tc.mem[parentOID] = newTree() // Parent empty at each level.
		tc.mem[childOID] = newTree(childEntries...)
	}

	// Test starts at level 0.
	changes, err := collect(tc, newHash("P0"), newHash("C0"), "")
	assert.NoError(t, err)
	assert.Len(t, changes, 1, "deep recursion should produce one change")

	// Build expected path.
	exp := ""
	for i := 0; i < depth-1; i++ {
		exp += "nested/"
	}
	exp += "leaf.txt"

	assert.Equal(t, exp, changes[0].Path, "deep path mismatch")
}

func BenchmarkWalkDiff_SmallTree(b *testing.B) { benchmarkWalkDiff(b, 64) }
func BenchmarkWalkDiff_LargeTree(b *testing.B) { benchmarkWalkDiff(b, 4096) }

func benchmarkWalkDiff(b *testing.B, n int) {
	parentOID := newHash("P")
	childOID := newHash("C")
	makeEntries := func(start int) []treeEntry {
		e := make([]treeEntry, n)
		for i := 0; i < n; i++ {
			e[i] = treeEntry{
				Name: fmt.Sprintf("file%06d.txt", i),
				Mode: 0100644,
				OID:  newHash(fmt.Sprintf("%d", start+i)),
			}
		}
		return e
	}
	tc := buildCache(map[Hash]*Tree{
		parentOID: newTree(makeEntries(0)...),
		childOID:  newTree(makeEntries(1)...),
	})

	for b.Loop() {
		_ = walkDiff(tc, parentOID, childOID, "", func(string, Hash, Hash, uint32) error { return nil })
	}
}
