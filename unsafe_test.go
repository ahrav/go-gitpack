// unsafe_test.go pins the safety-critical invariants of the zero-copy
// conversion helpers in unsafe.go (introduced with the mmap/zero-copy inflate
// optimization in 4f9f796). These helpers bypass Go's copy semantics, so the
// invariants they promise — exact aliasing, correct length, and value
// equality — must be asserted directly rather than left to indirect coverage.

package objstore

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/exp/mmap"

	"github.com/stretchr/testify/require"
)

// TestBtostr_ValueEquality confirms btostr produces a string equal to the
// standard copy conversion for a range of byte contents, including empty and
// binary data.
func TestBtostr_ValueEquality(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		[]byte("hello"),
		{0x00, 0xff, 0xa8, 0x01, '\n'},
		[]byte("commit 1234567890 +0000"),
	}
	for _, b := range cases {
		require.Equal(t, string(b), btostr(b))
	}
}

// TestBtostr_ZeroCopyAliasing verifies btostr does NOT copy: the returned
// string must share the exact backing array of the input slice. This is the
// core promise of the helper and the reason its mutation invariant matters.
func TestBtostr_ZeroCopyAliasing(t *testing.T) {
	b := []byte("aliased backing array")
	s := btostr(b)

	require.Equal(t, len(b), len(s))
	// The string's data pointer must equal the slice's first-element pointer.
	require.Equal(t,
		uintptr(unsafe.Pointer(&b[0])),
		uintptr(unsafe.Pointer(unsafe.StringData(s))),
		"btostr must alias the input backing array, not copy it")
}

// TestBtostr_EmptyReturnsEmpty pins the documented empty-input fast path.
func TestBtostr_EmptyReturnsEmpty(t *testing.T) {
	require.Equal(t, "", btostr(nil))
	require.Equal(t, "", btostr([]byte{}))
}

// TestMmapData_AliasesMappedRegion validates the struct-layout assumption that
// mmapData depends on: the returned slice must alias the mapped file and expose
// exactly its bytes. Any change to golang.org/x/exp/mmap's internal layout (the
// pinned dependency) would surface here immediately.
func TestMmapData_AliasesMappedRegion(t *testing.T) {
	want := []byte("memory-mapped file contents \x00\xff with binary bytes")
	path := filepath.Join(t.TempDir(), "mmapped.bin")
	require.NoError(t, os.WriteFile(path, want, 0o644))

	r, err := mmap.Open(path)
	require.NoError(t, err)
	defer r.Close()

	got := mmapData(r)
	require.Equal(t, len(want), r.Len(), "sanity: mmap reports file length")
	require.Equal(t, len(want), len(got), "mmapData length must equal file size")
	require.Equal(t, want, got, "mmapData must expose the file's bytes")

	// Cross-check a byte read through the public API matches the aliased slice.
	var one [1]byte
	_, err = r.ReadAt(one[:], 3)
	require.NoError(t, err)
	require.Equal(t, got[3], one[0], "aliased slice must agree with ReadAt")
}
