// tree_test.go
package objstore

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustHash(hexStr string) (h Hash) {
	b, err := hex.DecodeString(hexStr)
	if err != nil || len(b) != 20 {
		panic("bad test vector")
	}
	copy(h[:], b)
	return
}

func makeEntry(mode uint32, name string, h Hash) []byte {
	modePart := []byte(strconv.FormatUint(uint64(mode), 8))
	var buf bytes.Buffer
	buf.Write(modePart)
	buf.WriteByte(' ')
	buf.WriteString(name)
	buf.WriteByte(0)
	buf.Write(h[:])
	return buf.Bytes()
}

func buildRaw(entries []treeEntry) []byte {
	var raw []byte
	for _, e := range entries {
		raw = append(raw, makeEntry(e.Mode, e.Name, e.OID)...)
	}
	return raw
}

// Deterministic hash samples.
var (
	hash0 = mustHash("000102030405060708090a0b0c0d0e0f10111213")
	hash1 = mustHash("1111111111111111111111111111111111111111")
	hash2 = mustHash("2222222222222222222222222222222222222222")
)

func TestParseTree_Empty(t *testing.T) {
	tree, err := parseTree(nil)
	require.NoError(t, err, "empty tree should parse")
	assert.Len(t, tree.sortedEntries, 0, "want 0 entries")
	assert.Nil(t, tree.index, "index should be nil for empty tree")
}

func TestParseTree_SingleEntry(t *testing.T) {
	raw := buildRaw([]treeEntry{{OID: hash0, Name: "file.txt", Mode: 0100644}})
	tree, err := parseTree(raw)
	require.NoError(t, err, "parse should succeed")

	got, ok := tree.get("file.txt")
	require.True(t, ok, "lookup should succeed")
	assert.Equal(t, hash0, got.OID, "OID should match")
	assert.Equal(t, uint32(0100644), got.Mode, "Mode should match")
}

func TestParseTree_UTF8Name(t *testing.T) {
	raw := buildRaw([]treeEntry{{OID: hash1, Name: "файл.txt", Mode: 0100644}})
	tree, err := parseTree(raw)
	require.NoError(t, err, "parse should succeed")

	_, ok := tree.get("файл.txt")
	assert.True(t, ok, "UTF‑8 name should be found")
}

func TestParseTree_IndexBuiltAboveThreshold(t *testing.T) {
	const n = indexThreshold + 1
	entries := make([]treeEntry, n)
	for i := range entries {
		name := fmt.Sprintf("f%04d", i)
		entries[i] = treeEntry{OID: hash0, Name: name, Mode: 0100644}
	}
	tree, err := parseTree(buildRaw(entries))
	require.NoError(t, err, "parse should succeed")
	require.NotNil(t, tree.index, "aux index should be built for %d entries", n)

	// Random spot‑check.
	want := entries[n/2]
	got, ok := tree.get(want.Name)
	require.True(t, ok, "index lookup should succeed")
	assert.Equal(t, want, got, "index lookup should return correct entry")
}

func TestParseTree_DuplicateNames(t *testing.T) {
	// Duplicate names should cause parsing to fail with ErrCorruptTree.
	//
	// This test ensures that the tree parser enforces strict name ordering
	// and rejects duplicate entries.
	raw := append(
		makeEntry(0100644, "dup", hash0),
		makeEntry(0100755, "dup", hash1)...,
	)
	_, err := parseTree(raw)
	require.Error(t, err, "expected error for duplicate names")
	assert.Equal(t, ErrCorruptTree, err, "expected ErrCorruptTree for duplicate names")
}

func TestTreeGet_LinearVsIndexed(t *testing.T) {
	// Build exactly indexThreshold entries so no index is built.
	entries := make([]treeEntry, indexThreshold)
	for i := range entries {
		entries[i] = treeEntry{OID: hash2, Name: fmt.Sprintf("n%04d", i), Mode: 040000}
	}
	tree, err := parseTree(buildRaw(entries))
	require.NoError(t, err, "parse should succeed")
	assert.Nil(t, tree.index, "index should be nil when len==indexThreshold")

	_, ok := tree.get("n0007")
	assert.True(t, ok, "linear scan should succeed")
}

func TestParseTree_MalformedInputs(t *testing.T) {
	bad := []struct {
		name string
		raw  []byte
	}{
		{
			"no-space-after-mode",
			[]byte("100644"),
		},
		{
			"non‑octal-mode",
			append([]byte("100a44 file\000"), bytes.Repeat([]byte{1}, 20)...),
		},
		{
			"no‑nul‑terminator",
			[]byte("100644 file "),
		},
		{
			"truncated-hash",
			append([]byte("100644 file\000"), bytes.Repeat([]byte{0}, 10)...),
		},
		{
			"out-of-order-names",

			append(makeEntry(0100644, "z_last", hash0), makeEntry(0100644, "a_first", hash1)...),
		},
	}

	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseTree(tc.raw)
			assert.Error(t, err, "expected error for %s", tc.name)
		})
	}
}

func TestTreeGet_ConcurrentReads(t *testing.T) {
	const n = indexThreshold + 50
	entries := make([]treeEntry, n)
	for i := range entries {
		entries[i] = treeEntry{OID: hash0, Name: fmt.Sprintf("k%04d", i), Mode: 0100644}
	}
	tree, err := parseTree(buildRaw(entries))
	require.NoError(t, err, "parse should succeed")

	wg := sync.WaitGroup{}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range n {
				name := fmt.Sprintf("k%04d", rand.Intn(n))
				_, ok := tree.get(name)
				assert.True(t, ok, "should find %s", name)
			}
		}()
	}
	wg.Wait()
}

func FuzzParseTree(f *testing.F) {
	// Seed corpus with a valid single‑entry tree.
	seed := buildRaw([]treeEntry{{OID: hash0, Name: "seed", Mode: 0100644}})
	f.Add(seed)

	f.Fuzz(func(t *testing.T, data []byte) {
		tree, err := parseTree(data)
		if err != nil {
			return // Errors are expected; only care about panics.
		}
		// Invariants to validate when parsing succeeds.
		require.NotNil(t, tree, "tree should not be nil with nil error")

		if len(tree.sortedEntries) > indexThreshold {
			assert.NotNil(t, tree.index, "large tree should have index")
		}

		for _, e := range tree.sortedEntries {
			assert.NotEmpty(t, e.Name, "entry should not have empty filename")
		}
	})
}

func BenchmarkParseTree(b *testing.B) {
	const n = indexThreshold * 4
	ent := make([]treeEntry, n)
	for i := range ent {
		ent[i].Mode = 0100644
		ent[i].Name = fmt.Sprintf("bench%04d.txt", i)
		ent[i].OID = hash0
	}
	raw := buildRaw(ent)

	for b.Loop() {
		_, err := parseTree(raw)
		require.NoError(b, err)
	}
}

// Make sure the RNG is unpredictable across 'go test' runs.
func init() { rand.Seed(time.Now().UnixNano()) }
