package objstore

import (
	"errors"
	"fmt"
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

// createRawTreeData builds raw tree data in Git tree object format where each
// entry follows the pattern: "<mode> <name>\0<20-byte-hash>".
func createRawTreeData(entries ...struct {
	mode uint32
	name string
	hash Hash
}) []byte {
	if len(entries) == 0 {
		return []byte{}
	}

	// Sort entries by name as required by Git tree object format.
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

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
type change struct {
	Path     string
	Old, New Hash
	NewMode  uint32
}

func collect(tc *store, parent, child Hash, prefix string) ([]change, error) {
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

func TestWalkDiff_FileDirConversions(t *testing.T) {
	dirOID := newHash("D")
	tc := buildTestStore(map[Hash][]byte{
		{1}: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "foo", newHash("file")},
		),
		{2}: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{040000, "foo", dirOID},
		),
		dirOID: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "bar", newHash("bar")},
		),
		{}: createRawTreeData(),
	})

	got, err := collect(tc, Hash{1}, Hash{2}, "")
	assert.NoError(t, err)
	assert.Len(t, got, 1, "file→dir conversion should report one change")
	assert.Equal(t, "foo", got[0].Path, "file→dir conversion path mismatch")

	tc = buildTestStore(map[Hash][]byte{
		{3}: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{040000, "foo", dirOID},
		),
		{4}: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "foo", newHash("file")},
		),
		dirOID: createRawTreeData(
			struct {
				mode uint32
				name string
				hash Hash
			}{0100644, "bar", newHash("bar")},
		),
		{}: createRawTreeData(),
	})

	got, err = collect(tc, Hash{3}, Hash{4}, "")
	assert.NoError(t, err)
	assert.Len(t, got, 1, "dir→file conversion should report one change")
	assert.Equal(t, "foo", got[0].Path, "dir→file conversion path mismatch")
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
