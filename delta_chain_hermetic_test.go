// delta_chain_hermetic_test.go builds explicit multi-hop delta chains by hand
// (no git dependency) so the multi-hop delta-resolution paths are covered even
// on machines without the git CLI. In particular it drives:
//
//   - inflateDeltaChainBorrowed (store.go), reached only via the borrowed
//     (cacheResult=false) path of inflateFromPackWithOptions, which the
//     git-differential test also covers but which would otherwise be 0%
//     without a git executable;
//   - the ping-pong arena walk in applyDeltaStackCached (len(stack) > 1);
//   - walkUpDeltaChain climbing a genuine ref-delta chain across pack offsets.
//
// The chain is stored as N stacked ref-deltas: level i is a ref-delta whose
// base is the resolved blob of level i-1. Because Git names an object by its
// *resolved* content, each delta object is indexed under the SHA-1 of the blob
// it reconstructs, so requesting the top OID forces a full N-hop walk.

package objstore

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zlib"
	"github.com/stretchr/testify/require"
)

// buildRefDeltaChainPack writes a pack containing one base blob followed by
// `levels` stacked ref-deltas, and a matching v2 index. It returns the pack
// directory, the resolved content of every level (index 0 == base), and the
// resolved OID of every level.
func buildRefDeltaChainPack(t *testing.T, levels int) (packDir string, contents [][]byte, oids []Hash) {
	t.Helper()
	require.Greater(t, levels, 1, "need >=2 levels to exercise the multi-hop path")

	dir := t.TempDir()
	packDir = filepath.Join(dir, "pack")
	require.NoError(t, os.MkdirAll(packDir, 0o755))
	packPath := filepath.Join(packDir, "chain.pack")
	idxPath := filepath.Join(packDir, "chain.idx")

	// Distinct, non-trivial content per level so a wrong hop is caught. The
	// shared test helper (buildSelfContainedDelta) encodes each delta as a
	// single insert whose size is one byte, so every level's target must stay
	// <= 127 bytes; the content below tops out near 70 bytes for levels<=12.
	contents = make([][]byte, levels+1)
	oids = make([]Hash, levels+1)
	for i := 0; i <= levels; i++ {
		var b bytes.Buffer
		fmt.Fprintf(&b, "L%02d:", i)
		// Vary length and bytes per level; include 0x00 and high bytes.
		b.Write(bytes.Repeat([]byte{byte(i), 0x00, 0xa8, byte(255 - i)}, 10+i))
		contents[i] = b.Bytes()
		require.LessOrEqual(t, len(contents[i]), 127, "level content must fit single-byte insert")
		oids[i] = calculateHash(ObjBlob, contents[i])
	}

	var pack bytes.Buffer
	pack.WriteString("PACK")
	binary.Write(&pack, binary.BigEndian, uint32(2))
	binary.Write(&pack, binary.BigEndian, uint32(levels+1))

	offsets := make([]uint64, levels+1)

	// Base blob (level 0) as a plain object.
	offsets[0] = uint64(pack.Len())
	pack.Write(encodeObjHeader(uint8(ObjBlob), uint64(len(contents[0]))))
	pack.Write(zlibCompress(t, contents[0]))

	// Stacked ref-deltas: level i transforms contents[i-1] -> contents[i] and
	// references oids[i-1] as its base.
	for i := 1; i <= levels; i++ {
		offsets[i] = uint64(pack.Len())
		obj, err := createRefDeltaObject(oids[i-1], contents[i], contents[i-1])
		require.NoError(t, err)
		pack.Write(obj)
	}

	trailer := sha1.Sum(pack.Bytes())
	pack.Write(trailer[:])
	require.NoError(t, os.WriteFile(packPath, pack.Bytes(), 0o644))

	require.NoError(t, createV2IndexFile(idxPath, oids, offsets))
	return packDir, contents, oids
}

// zlibCompress returns the zlib-compressed form of data.
func zlibCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	_, err := zw.Write(data)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// TestMultiHopRefDeltaChain_BorrowedAndStreaming resolves a deep ref-delta chain
// through both store entry points and asserts each level materializes to its
// expected content, and that the borrowed and streaming paths agree.
func TestMultiHopRefDeltaChain_BorrowedAndStreaming(t *testing.T) {
	const levels = 12
	packDir, contents, oids := buildRefDeltaChainPack(t, levels)

	// Borrowed path first, on a cold store so walkUpDeltaChain performs the
	// full multi-hop climb (the offset cache is empty).
	t.Run("borrowed_cold", func(t *testing.T) {
		st, err := OpenForTesting(packDir)
		require.NoError(t, err)
		defer st.Close()

		top := oids[levels]
		p, off, ok := st.findPackedObject(top)
		require.True(t, ok, "top-of-chain object must be packed")

		got, typ, err := st.getPackedObjectNoCache(p, off, top)
		require.NoError(t, err)
		require.Equal(t, ObjBlob, typ)
		require.Equal(t, contents[levels], got, "borrowed multi-hop result mismatch")
	})

	// Every level must materialize to its exact content via the streaming
	// (cached) path, which also validates intermediate chain lengths 1..N.
	t.Run("streaming_all_levels", func(t *testing.T) {
		st, err := OpenForTesting(packDir)
		require.NoError(t, err)
		defer st.Close()

		for i := 0; i <= levels; i++ {
			got, typ, err := st.getMaterialized(oids[i])
			require.NoErrorf(t, err, "level %d", i)
			require.Equalf(t, ObjBlob, typ, "level %d type", i)
			require.Equalf(t, contents[i], got, "level %d content mismatch", i)
		}
	})

	// Borrowed and streaming paths must return identical bytes for the same
	// object on independent (cold) stores.
	t.Run("borrowed_equals_streaming", func(t *testing.T) {
		stA, err := OpenForTesting(packDir)
		require.NoError(t, err)
		defer stA.Close()
		stB, err := OpenForTesting(packDir)
		require.NoError(t, err)
		defer stB.Close()

		for i := 1; i <= levels; i++ {
			p, off, ok := stA.findPackedObject(oids[i])
			require.True(t, ok)
			borrowed, _, err := stA.getPackedObjectNoCache(p, off, oids[i])
			require.NoError(t, err)

			streaming, _, err := stB.getMaterialized(oids[i])
			require.NoError(t, err)

			require.Equalf(t, streaming, borrowed, "path divergence at level %d", i)
			require.Equalf(t, contents[i], borrowed, "wrong content at level %d", i)
		}
	})
}
