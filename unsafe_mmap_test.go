//go:build linux || darwin || windows

// unsafe_mmap_test.go holds the part of the unsafe-helper suite that depends on
// mmapData, which inflate_mmap.go defines only for the mmap-backed platforms.
// The build constraint here must track that file's: on the fallback targets the
// package still builds (inflate_fallback.go uses the public ReaderAt API and
// never takes the unsafe cast), so an untagged reference to mmapData would fail
// test compilation on a platform whose library build is fine. The portable
// btostr assertions stay in unsafe_test.go.

package objstore

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/exp/mmap"

	"github.com/stretchr/testify/require"
)

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
