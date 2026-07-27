// diff_tree_test.go tests walkDiff, which computes the set of blob-level
// changes between two Git tree objects. The function recursively descends
// into sub-trees and emits a change for every file that was added, modified
// (content or mode), or deleted.
//
// The tests exercise:
//   - Empty and identical trees (no changes expected).
//   - Insert, modify, and delete of regular files.
//   - Mode-only changes (e.g., 0644 -> 0755).
//   - Gitlink (submodule) entries treated as opaque, non-recursive changes.
//   - Recursive descent with a path prefix.
//   - Every in-place entry-type transition, reported as a deletion of the old
//     entry followed by an addition of the new one.
//   - A conversion whose merge-join cursors never pair, because a sibling name
//     sorts between the two spellings of the converted name.
//   - Error propagation from the emit callback.
//   - Cache misses on tree resolution.
//   - Deeply nested directory structures (depth 40).
//   - Benchmarks for small (64-entry) and large (4096-entry) flat trees.

package objstore

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"testing"

	"github.com/hashicorp/golang-lru/arc/v2"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/mmap"
)

// newHash creates a deterministic Hash whose first byte is the first byte of s.
func newHash(s string) Hash {
	var h Hash
	for i := 0; i < len(s) && i < len(h); i++ {
		h[i] = s[i]
	}
	return h
}

// treeEntry names the struct createRawTreeData accepts. It is an alias rather
// than a definition so it stays identical to that variadic parameter's type.
type treeEntry = struct {
	mode uint32
	name string
	hash Hash
}

// createRawTreeData builds raw tree data in Git tree object format where each
// entry follows the pattern: "<mode> <name>\0<20-byte-hash>".
func createRawTreeData(entries ...treeEntry) []byte {
	if len(entries) == 0 {
		return []byte{}
	}

	// Git tree sort order, which is not plain lexicographic order: see the
	// precondition on walkDiff. A synthetic tree in any other order makes the
	// merge-join comparisons wrong and produces a silently incorrect diff.
	sort.Slice(entries, func(i, j int) bool {
		return treeSortKey(entries[i]) < treeSortKey(entries[j])
	})

	var raw []byte
	for _, entry := range entries {
		raw = append(raw, []byte(diffTestOctStr(entry.mode))...)
		raw = append(raw, ' ')
		raw = append(raw, []byte(entry.name)...)
		raw = append(raw, 0) // null terminator
		raw = append(raw, entry.hash[:]...)
	}
	return raw
}

// treeSortKey returns the name Git compares when ordering e among its siblings.
// Appending '/' to a tree's name reproduces Git's rule that the separator
// counts for directories, which is why "foo.c" precedes "foo" when "foo" is a
// tree.
func treeSortKey(e treeEntry) string {
	if isTreeMode(e.mode) {
		return e.name + "/"
	}
	return e.name
}

// diffTestOctStr converts a uint32 to an octal string.
func diffTestOctStr(n uint32) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte(n&7) + '0'
		n >>= 3
	}
	return string(buf[i:])
}

// buildTestStore creates a Store with pre-cached tree objects for testing.
func buildTestStore(trees map[Hash][]byte) *store {
	store := &store{
		maxDeltaDepth: defaultMaxDeltaDepth,
		packs:         []*idxFile{},
		packMap:       make(map[string]*mmap.ReaderAt),
		dw:            newRefCountedDeltaWindow(),
	}

	const defaultCacheSize = 1 << 14
	cache, err := arc.NewARC[Hash, cachedObj](defaultCacheSize)
	if err != nil {
		panic(fmt.Sprintf("failed to create ARC cache: %v", err))
	}
	store.cache = cache

	for hash, treeData := range trees {
		store.cache.Add(hash, cachedObj{data: treeData, typ: ObjTree})
	}

	return store
}

// collect runs walkDiff and captures all reported changes for testing.
//
// Mode is the mode of the side the event describes: the new mode for an
// addition or a modification, the old mode for a deletion.
type change struct {
	Path     string
	Old, New Hash
	Mode     uint32
}

func collect(tc *store, parent, child Hash, prefix string) ([]change, error) {
	var out []change
	err := walkDiff(tc, parent, child, prefix, func(p string, old, new Hash, mode uint32) error {
		out = append(out, change{p, old, new, mode})
		return nil
	})
	return out, err
}

// equalChanges reports whether two change slices contain the same elements,
// regardless of order. Both slices are sorted by Path before comparison so
// that tests are not sensitive to the traversal order of walkDiff, which may
// vary depending on the tree entry layout.
//
// Path is the whole sort key, so a change set holding two events for one path
// -- a type transition -- must be compared with assert.Equal against the
// emitted order instead.
func equalChanges(a, b []change) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Slice(a, func(i, j int) bool { return a[i].Path < a[j].Path })
	sort.Slice(b, func(i, j int) bool { return b[i].Path < b[j].Path })
	return reflect.DeepEqual(a, b)
}

// treeEntryNames lists the entry names of the tree stored under oid in the
// order TreeIter yields them, which is the order walkDiff's merge-join sees.
func treeEntryNames(t *testing.T, tc *store, oid Hash) []string {
	t.Helper()
	iter, err := tc.treeIter(oid)
	if err != nil {
		t.Fatalf("treeIter(%s): %v", oid, err)
	}
	defer putTreeIter(iter)

	var names []string
	for {
		name, _, _, ok, err := iter.Next()
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("TreeIter.Next on %s: %v", oid, err)
		}
		if !ok {
			return names
		}
		names = append(names, name)
	}
}

func TestWalkDiff_EmptyAndIdenticalTrees(t *testing.T) {
	tc := &store{}

	ch, err := collect(tc, Hash{}, Hash{}, "")
	assert.NoError(t, err)
	assert.Empty(t, ch, "empty vs empty should produce no changes")

	treeOID := newHash("T")
	ch, err = collect(tc, treeOID, treeOID, "")
	assert.NoError(t, err)
	assert.Empty(t, ch, "identical trees should produce no changes")
}

func TestWalkDiff_InsertModifyDeleteMode(t *testing.T) {
	oa := newHash("a1")
	na := newHash("a2")
	nb := newHash("b")
	parentOID := newHash("P")
	childOID := newHash("C")

	tc := buildTestStore(map[Hash][]byte{
		parentOID: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "a.txt", oa},
		),
		childOID: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "a.txt", na}, // modified content
			struct {
				mode uint32
				name string
				hash Hash
			}{0100755, "b.txt", nb}, // new file
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
	tc := buildTestStore(map[Hash][]byte{
		{1}: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "exec.sh", h},
		),
		{2}: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100755, "exec.sh", h},
		),
	})

	got, err := collect(tc, Hash{1}, Hash{2}, "")
	assert.NoError(t, err)
	want := []change{{"exec.sh", h, h, 0100755}}
	assert.True(t, equalChanges(got, want), "mode change not reported: %+v", got)
}

func TestWalkDiff_GitlinkNotRecursed(t *testing.T) {
	oldSub := newHash("sub-old")
	newSub := newHash("sub-new")
	parentOID := newHash("parent-sub")
	childOID := newHash("child-sub")

	tc := buildTestStore(map[Hash][]byte{
		parentOID: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0160000, "vendor", oldSub},
		),
		childOID: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0160000, "vendor", newSub},
		),
	})

	got, err := collect(tc, parentOID, childOID, "")
	assert.NoError(t, err)

	want := []change{
		{"vendor", oldSub, newSub, 0160000},
	}
	assert.True(t, equalChanges(got, want), "gitlink change should be reported as a file replacement")
}

func TestWalkDiff_RecursiveAndPrefix(t *testing.T) {
	subParent := newHash("sp")
	subChild := newHash("sc")
	rootP := newHash("rp")
	rootC := newHash("rc")

	tc := buildTestStore(map[Hash][]byte{
		subParent: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "f.txt", newHash("1")},
		),
		subChild: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "f.txt", newHash("2")}, // modified content
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "g.txt", newHash("3")}, // new file
		),
		rootP: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{040000, "dir", subParent},
		),
		rootC: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{040000, "dir", subChild},
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "h.txt", newHash("4")}, // new file at root
		),
		{}: createRawTreeData(),
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

	var entries []struct {
		mode uint32
		name string
		hash Hash
	}
	for i, n := range names {
		entries = append(entries, struct {
			mode uint32
			name string
			hash Hash
		}{
			0100644, n, newHash(string('a' + byte(i))),
		})
	}

	parentOID, childOID := newHash("P"), newHash("C")
	tc := buildTestStore(map[Hash][]byte{
		parentOID: createRawTreeData(),
		childOID:  createRawTreeData(entries...),
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

// TestWalkDiff_FileDirConversions checks both directions of file/directory
// conversion. Nothing is carried across a type change, so each direction is
// reported as a deletion of the old entry followed by an addition of the new
// one -- the pair `git diff-tree -r` produces:
//
//   - File -> Directory ("foo" was a regular file, now it is a tree containing
//     "bar"). The blob at "foo" leaves and "foo/bar" arrives, because handleAdd
//     recurses into the new tree instead of reporting one tree-mode event that
//     every blob-filtering caller would discard.
//
//   - Directory -> File ("foo" was a tree, now it is a regular file).
//     "foo/bar" leaves, because handleDel recurses into the old tree, and the
//     blob at "foo" arrives.
//
// Each event carries the mode of the side it describes and a zero OID on the
// side where the entry does not exist.
func TestWalkDiff_FileDirConversions(t *testing.T) {
	dirOID := newHash("D")
	fileOID := newHash("file")
	barOID := newHash("bar")
	dirTree := createRawTreeData(treeEntry{0100644, "bar", barOID})

	tc := buildTestStore(map[Hash][]byte{
		{1}:    createRawTreeData(treeEntry{0100644, "foo", fileOID}),
		{2}:    createRawTreeData(treeEntry{040000, "foo", dirOID}),
		dirOID: dirTree,
	})

	got, err := collect(tc, Hash{1}, Hash{2}, "")
	assert.NoError(t, err)
	want := []change{
		{"foo", fileOID, Hash{}, 0100644},
		{"foo/bar", Hash{}, barOID, 0100644},
	}
	assert.True(t, equalChanges(got, want),
		"file→dir conversion mismatch\nwant %+v\ngot  %+v", want, got)

	tc = buildTestStore(map[Hash][]byte{
		{3}:    createRawTreeData(treeEntry{040000, "foo", dirOID}),
		{4}:    createRawTreeData(treeEntry{0100644, "foo", fileOID}),
		dirOID: dirTree,
	})

	got, err = collect(tc, Hash{3}, Hash{4}, "")
	assert.NoError(t, err)
	want = []change{
		{"foo", Hash{}, fileOID, 0100644},
		{"foo/bar", barOID, Hash{}, 0100644},
	}
	assert.True(t, equalChanges(got, want),
		"dir→file conversion mismatch\nwant %+v\ngot  %+v", want, got)
}

// TestWalkDiff_ConversionIsAlignmentIndependent pins that a conversion produces
// the same logical change set whether or not the merge-join cursors ever meet
// on the converted name.
//
// Git compares a tree's name as if it ended in '/', so a sibling can sort
// between the two spellings of one name: with "foo" a blob on the old side and
// a tree on the new side, the old tree yields "foo" then "foo.c" while the new
// tree yields "foo.c" then "foo". The cursors never sit on the same name, and
// the conversion reaches the callback through the deletion and addition
// branches rather than the equal-name branch. Cursor alignment is an artifact
// of a neighbour's name, so it must not change what the diff means.
func TestWalkDiff_ConversionIsAlignmentIndependent(t *testing.T) {
	var (
		oldTree = Hash{1}
		newTree = Hash{2}
		dirOID  = newHash("D")
		fileOID = newHash("file")
		barOID  = newHash("bar")
		sibOID  = newHash("sibling")

		fooFile = treeEntry{0100644, "foo", fileOID}
		fooDir  = treeEntry{040000, "foo", dirOID}
		sibling = treeEntry{0100644, "foo.c", sibOID}
	)
	newStore := func(oldEntries, newEntries []treeEntry) *store {
		return buildTestStore(map[Hash][]byte{
			oldTree: createRawTreeData(oldEntries...),
			newTree: createRawTreeData(newEntries...),
			dirOID:  createRawTreeData(treeEntry{0100644, "bar", barOID}),
		})
	}

	cases := []struct {
		name           string
		pairedOld      []treeEntry
		pairedNew      []treeEntry
		unpairedOld    []treeEntry
		unpairedNew    []treeEntry
		wantOldOrder   []string
		wantNewOrder   []string
		wantConversion []change
	}{
		{
			name:         "FileToDir",
			pairedOld:    []treeEntry{fooFile},
			pairedNew:    []treeEntry{fooDir},
			unpairedOld:  []treeEntry{fooFile, sibling},
			unpairedNew:  []treeEntry{fooDir, sibling},
			wantOldOrder: []string{"foo", "foo.c"},
			wantNewOrder: []string{"foo.c", "foo"},
			wantConversion: []change{
				{"foo", fileOID, Hash{}, 0100644},
				{"foo/bar", Hash{}, barOID, 0100644},
			},
		},
		{
			name:         "DirToFile",
			pairedOld:    []treeEntry{fooDir},
			pairedNew:    []treeEntry{fooFile},
			unpairedOld:  []treeEntry{fooDir, sibling},
			unpairedNew:  []treeEntry{fooFile, sibling},
			wantOldOrder: []string{"foo.c", "foo"},
			wantNewOrder: []string{"foo", "foo.c"},
			wantConversion: []change{
				{"foo", Hash{}, fileOID, 0100644},
				{"foo/bar", barOID, Hash{}, 0100644},
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			paired, err := collect(newStore(tt.pairedOld, tt.pairedNew), oldTree, newTree, "")
			assert.NoError(t, err)
			assert.True(t, equalChanges(paired, tt.wantConversion),
				"paired conversion mismatch\nwant %+v\ngot  %+v", tt.wantConversion, paired)

			tc := newStore(tt.unpairedOld, tt.unpairedNew)
			// The fixture only exercises the unpaired route while the sibling
			// really does sort between the two spellings of "foo", so assert
			// the order the merge-join will see rather than assuming it.
			assert.Equal(t, tt.wantOldOrder, treeEntryNames(t, tc, oldTree))
			assert.Equal(t, tt.wantNewOrder, treeEntryNames(t, tc, newTree))

			unpaired, err := collect(tc, oldTree, newTree, "")
			assert.NoError(t, err)
			// The unchanged sibling emits nothing, so the two alignments must
			// agree on the whole change set.
			assert.True(t, equalChanges(unpaired, tt.wantConversion),
				"unpaired conversion mismatch\nwant %+v\ngot  %+v", tt.wantConversion, unpaired)
		})
	}
}

// TestWalkDiff_TypeTransitionMatrix walks every in-place entry-type transition
// and pins the per-side shape: a deletion carrying the old mode and the old
// OID, then an addition carrying the new mode and the new OID, with a zero OID
// on the side where the entry is absent. A transition that keeps one OID still
// splits, because the two sides are different kinds of object. The
// permission-only case is the control that must stay a single event.
//
// The gitlink cases name a commit OID that is absent from the store, which is
// what a submodule looks like from the superproject: a walk that recursed into
// one could not resolve it, so a nil error is itself the assertion that
// gitlinks stay opaque.
func TestWalkDiff_TypeTransitionMatrix(t *testing.T) {
	const (
		modeFile = 0100644
		modeExec = 0100755
		modeLink = 0120000
		modeSub  = 0160000
		modeDir  = 040000
	)
	var (
		oldTree   = Hash{1}
		newTree   = Hash{2}
		dirOID    = newHash("D")
		nestedOID = newHash("nested")
		blobOID   = newHash("blob")
		altOID    = newHash("alt")
		subOID    = newHash("submodule-commit")
	)

	cases := []struct {
		name     string
		oldEntry treeEntry
		newEntry treeEntry
		want     []change
	}{
		{
			name:     "FileToSymlink",
			oldEntry: treeEntry{modeFile, "p", blobOID},
			newEntry: treeEntry{modeLink, "p", altOID},
			want: []change{
				{"p", blobOID, Hash{}, modeFile},
				{"p", Hash{}, altOID, modeLink},
			},
		},
		{
			name:     "SymlinkToFile",
			oldEntry: treeEntry{modeLink, "p", altOID},
			newEntry: treeEntry{modeFile, "p", blobOID},
			want: []change{
				{"p", altOID, Hash{}, modeLink},
				{"p", Hash{}, blobOID, modeFile},
			},
		},
		{
			// A regular file whose bytes are exactly a path string hashes to
			// the same OID as a symlink to that path, so the OIDs match while
			// the entries describe different objects.
			name:     "FileToSymlinkSameOID",
			oldEntry: treeEntry{modeFile, "p", blobOID},
			newEntry: treeEntry{modeLink, "p", blobOID},
			want: []change{
				{"p", blobOID, Hash{}, modeFile},
				{"p", Hash{}, blobOID, modeLink},
			},
		},
		{
			name:     "SymlinkToFileSameOID",
			oldEntry: treeEntry{modeLink, "p", blobOID},
			newEntry: treeEntry{modeFile, "p", blobOID},
			want: []change{
				{"p", blobOID, Hash{}, modeLink},
				{"p", Hash{}, blobOID, modeFile},
			},
		},
		{
			name:     "FileToGitlink",
			oldEntry: treeEntry{modeFile, "p", blobOID},
			newEntry: treeEntry{modeSub, "p", subOID},
			want: []change{
				{"p", blobOID, Hash{}, modeFile},
				{"p", Hash{}, subOID, modeSub},
			},
		},
		{
			name:     "GitlinkToFile",
			oldEntry: treeEntry{modeSub, "p", subOID},
			newEntry: treeEntry{modeFile, "p", blobOID},
			want: []change{
				{"p", subOID, Hash{}, modeSub},
				{"p", Hash{}, blobOID, modeFile},
			},
		},
		{
			name:     "SymlinkToGitlink",
			oldEntry: treeEntry{modeLink, "p", altOID},
			newEntry: treeEntry{modeSub, "p", subOID},
			want: []change{
				{"p", altOID, Hash{}, modeLink},
				{"p", Hash{}, subOID, modeSub},
			},
		},
		{
			name:     "GitlinkToSymlink",
			oldEntry: treeEntry{modeSub, "p", subOID},
			newEntry: treeEntry{modeLink, "p", altOID},
			want: []change{
				{"p", subOID, Hash{}, modeSub},
				{"p", Hash{}, altOID, modeLink},
			},
		},
		{
			name:     "GitlinkToDir",
			oldEntry: treeEntry{modeSub, "p", subOID},
			newEntry: treeEntry{modeDir, "p", dirOID},
			want: []change{
				{"p", subOID, Hash{}, modeSub},
				{"p/bar", Hash{}, nestedOID, modeFile},
			},
		},
		{
			name:     "DirToGitlink",
			oldEntry: treeEntry{modeDir, "p", dirOID},
			newEntry: treeEntry{modeSub, "p", subOID},
			want: []change{
				{"p/bar", nestedOID, Hash{}, modeFile},
				{"p", Hash{}, subOID, modeSub},
			},
		},
		{
			name:     "SymlinkToDir",
			oldEntry: treeEntry{modeLink, "p", altOID},
			newEntry: treeEntry{modeDir, "p", dirOID},
			want: []change{
				{"p", altOID, Hash{}, modeLink},
				{"p/bar", Hash{}, nestedOID, modeFile},
			},
		},
		{
			name:     "DirToSymlink",
			oldEntry: treeEntry{modeDir, "p", dirOID},
			newEntry: treeEntry{modeLink, "p", altOID},
			want: []change{
				{"p/bar", nestedOID, Hash{}, modeFile},
				{"p", Hash{}, altOID, modeLink},
			},
		},
		{
			name:     "FileToDir",
			oldEntry: treeEntry{modeFile, "p", blobOID},
			newEntry: treeEntry{modeDir, "p", dirOID},
			want: []change{
				{"p", blobOID, Hash{}, modeFile},
				{"p/bar", Hash{}, nestedOID, modeFile},
			},
		},
		{
			name:     "DirToFile",
			oldEntry: treeEntry{modeDir, "p", dirOID},
			newEntry: treeEntry{modeFile, "p", blobOID},
			want: []change{
				{"p/bar", nestedOID, Hash{}, modeFile},
				{"p", Hash{}, blobOID, modeFile},
			},
		},
		{
			// Same type on both sides: one event, both OIDs, no split.
			name:     "PermissionOnly",
			oldEntry: treeEntry{modeFile, "p", blobOID},
			newEntry: treeEntry{modeExec, "p", blobOID},
			want: []change{
				{"p", blobOID, blobOID, modeExec},
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tc := buildTestStore(map[Hash][]byte{
				oldTree: createRawTreeData(tt.oldEntry),
				newTree: createRawTreeData(tt.newEntry),
				dirOID:  createRawTreeData(treeEntry{modeFile, "bar", nestedOID}),
			})

			got, err := collect(tc, oldTree, newTree, "")
			assert.NoError(t, err)
			// Compared in emission order: two events can share one path, which
			// equalChanges cannot order.
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWalkDiff_EmitErrorPropagates(t *testing.T) {
	tc := buildTestStore(map[Hash][]byte{
		{1}: createRawTreeData(),
		{2}: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "a", newHash("a")},
		),
	})

	sentinel := errors.New("boom")
	err := walkDiff(tc, Hash{1}, Hash{2}, "", func(string, Hash, Hash, uint32) error {
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel, "emit error not propagated")
}

func TestWalkDiff_CacheMiss(t *testing.T) {
	tc := buildTestStore(map[Hash][]byte{})
	err := walkDiff(tc, Hash{}, newHash("x"), "", func(string, Hash, Hash, uint32) error {
		return nil
	})
	assert.Error(t, err, "expected error on cache miss")
}

func TestWalkDiff_DeepRecursion(t *testing.T) {
	depth := 40
	trees := map[Hash][]byte{
		{}: createRawTreeData(),
	}

	// Build nested directory structure from bottom up.
	for i := depth - 1; i >= 0; i-- {
		currentOID := newHash(fmt.Sprintf("C%d", i))
		if i == depth-1 {
			// Leaf level contains the target file.
			trees[currentOID] = createRawTreeData(
				struct {
					mode uint32
					name string
					hash Hash
				}{0100644, "leaf.txt", newHash("leaf")},
			)
		} else {
			// Intermediate levels point to the next level down.
			nextChildOID := newHash(fmt.Sprintf("C%d", i+1))
			trees[currentOID] = createRawTreeData(
				struct {
					mode uint32
					name string
					hash Hash
				}{040000, "nested", nextChildOID},
			)
		}
	}

	tc := buildTestStore(trees)

	parentOID := newHash("P0")
	tc.cache.Add(parentOID, cachedObj{data: createRawTreeData(), typ: ObjTree})

	changes, err := collect(tc, parentOID, newHash("C0"), "")
	assert.NoError(t, err)
	assert.Len(t, changes, 1, "deep recursion should produce one change")

	// Build expected nested path.
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

	makeEntries := func(start int) []struct {
		mode uint32
		name string
		hash Hash
	} {
		var entries []struct {
			mode uint32
			name string
			hash Hash
		}
		for i := 0; i < n; i++ {
			entries = append(entries, struct {
				mode uint32
				name string
				hash Hash
			}{
				0100644,
				fmt.Sprintf("file%06d.txt", i),
				newHash(fmt.Sprintf("%d", start+i)),
			})
		}
		return entries
	}

	tc := buildTestStore(map[Hash][]byte{
		parentOID: createRawTreeData(makeEntries(0)...),
		childOID:  createRawTreeData(makeEntries(1)...),
	})

	for b.Loop() {
		_ = walkDiff(tc, parentOID, childOID, "", func(string, Hash, Hash, uint32) error { return nil })
	}
}
