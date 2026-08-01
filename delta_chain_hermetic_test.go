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
// directory, the resolved content of every level (index 0 == base), the
// resolved OID of every level, and each level's byte offset within the pack.
//
// The offsets are returned because the offset cache is keyed by (pack, offset),
// not by OID: asserting which intermediate hops a chain published requires the
// same offsets the fixture wrote. See streaming_cold_per_level.
func buildRefDeltaChainPack(t *testing.T, levels int) (packDir string, contents [][]byte, oids []Hash, offsets []uint64) {
	t.Helper()
	require.Greater(t, levels, 1, "need >=2 levels to exercise the multi-hop path")

	dir := t.TempDir()
	packDir = filepath.Join(dir, "pack")
	require.NoError(t, os.MkdirAll(packDir, 0o755))
	packPath := filepath.Join(packDir, "chain.pack")
	idxPath := filepath.Join(packDir, "chain.idx")

	// Level 0 is a plain blob; every level above it is derived from its base by
	// makeChainHop, which returns the content and the delta that rebuilds it
	// from one description so the two cannot drift apart.
	contents = make([][]byte, levels+1)
	oids = make([]Hash, levels+1)
	deltas := make([][]byte, levels+1)

	var base bytes.Buffer
	base.WriteString("L00:")
	// Vary the bytes, including 0x00 and high bytes, so a byte-swapped or
	// truncated hop is visible in the comparison.
	base.Write(bytes.Repeat([]byte{0x00, 0xa8, 0xff, 0x41}, 12))
	contents[0] = base.Bytes()
	oids[0] = calculateHash(ObjBlob, contents[0])

	for i := 1; i <= levels; i++ {
		contents[i], deltas[i] = makeChainHop(t, contents[i-1], i)
		oids[i] = calculateHash(ObjBlob, contents[i])
	}

	var pack bytes.Buffer
	pack.WriteString("PACK")
	binary.Write(&pack, binary.BigEndian, uint32(2))
	binary.Write(&pack, binary.BigEndian, uint32(levels+1))

	offsets = make([]uint64, levels+1)

	// Base blob (level 0) as a plain object.
	offsets[0] = uint64(pack.Len())
	pack.Write(encodeObjHeader(uint8(ObjBlob), uint64(len(contents[0]))))
	pack.Write(zlibCompress(t, contents[0]))

	// Stacked ref-deltas: level i transforms contents[i-1] -> contents[i] and
	// references oids[i-1] as its base.
	for i := 1; i <= levels; i++ {
		offsets[i] = uint64(pack.Len())
		pack.Write(encodeObjHeader(uint8(ObjRefDelta), uint64(len(deltas[i]))))
		pack.Write(oids[i-1][:])
		pack.Write(zlibCompress(t, deltas[i]))
	}

	trailer := sha1.Sum(pack.Bytes())
	pack.Write(trailer[:])
	require.NoError(t, os.WriteFile(packPath, pack.Bytes(), 0o644))

	require.NoError(t, createV2IndexFile(idxPath, oids, offsets))
	return packDir, contents, oids, offsets
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

// makeChainHop derives one chain level's content from its base and returns it
// together with the delta payload that rebuilds it. Both come out of a single
// description, so the declared content and the instructions can never disagree.
//
// Every hop emits at least one copy command, which is the point. The shared
// buildSelfContainedDelta helper encodes a delta as one insert of the entire
// target, so each hop's output depends only on its base's *length*: a resolver
// that fed the next hop a corrupted intermediate buffer of the right size would
// still reconstruct the correct final bytes, and the chain assertions would pass
// while proving nothing about intermediate materialization. Copying from the
// base makes each hop read what the previous hop actually produced, so a
// corrupted or misaligned arena hand-off propagates into the result.
//
// Both hop shapes copy EVERY base byte, so no corrupted base byte can be
// swallowed by the hop that reads it. The odd shape rotates rather than
// truncates for exactly this reason: copying only base[skip:] would discard
// base[:skip], and corruption confined to those bytes would vanish at the hop
// instead of propagating to the final result — silently voiding the invariant
// the chain assertions rest on.
//
// The two shapes alternate so both copy-operand cases are covered: a copy at
// offset 0 (no offset operand bytes at all) and a copy at a non-zero offset.
// The odd shape emits both, since the rotation needs a non-zero-offset copy for
// the suffix and an offset-0 copy for the wrapped prefix.
func makeChainHop(t *testing.T, base []byte, level int) (target, delta []byte) {
	t.Helper()
	require.Greater(t, len(base), chainHopCopySkip, "base too short to copy from")

	marker := []byte(fmt.Sprintf("|L%02d|", level))

	var instructions []byte
	if level%2 == 0 {
		// target = base ++ marker; copy starts at offset 0.
		target = append(append([]byte(nil), base...), marker...)
		instructions = appendDeltaCopy(t, instructions, 0, len(base))
		instructions = appendDeltaInsert(t, instructions, marker)
	} else {
		// target = marker ++ base[skip:] ++ base[:skip]: the base is rotated,
		// not truncated, so the copy starts at a non-zero offset while every
		// base byte still reaches the target.
		target = append(append([]byte(nil), marker...), base[chainHopCopySkip:]...)
		target = append(target, base[:chainHopCopySkip]...)
		instructions = appendDeltaInsert(t, instructions, marker)
		instructions = appendDeltaCopy(t, instructions,
			chainHopCopySkip, len(base)-chainHopCopySkip)
		instructions = appendDeltaCopy(t, instructions, 0, chainHopCopySkip)
	}

	require.Len(t, target, len(base)+len(marker),
		"every hop must carry all base bytes plus its marker")

	var d bytes.Buffer
	writeVarInt(&d, uint64(len(base)))
	writeVarInt(&d, uint64(len(target)))
	d.Write(instructions)
	return target, d.Bytes()
}

// chainHopCopySkip is the non-zero copy offset used by odd chain levels.
const chainHopCopySkip = 3

// appendDeltaCopy appends a Git delta copy command for base[off:off+size].
//
// The command byte's low 7 bits say which operand bytes follow: bits 0-3 select
// bytes of the offset, bits 4-6 bytes of the size, little-endian, and an omitted
// byte decodes as zero. Only non-zero bytes are emitted, matching what Git
// writes. A size of zero is not encodable — the format reads it back as
// 0x10000 — so it is rejected rather than silently emitted.
func appendDeltaCopy(t *testing.T, dst []byte, off, size int) []byte {
	t.Helper()
	require.Positive(t, size, "copy size 0 encodes as 0x10000")
	require.LessOrEqual(t, size, 0xffffff, "copy size exceeds the 3-byte operand")
	require.GreaterOrEqual(t, off, 0)
	require.LessOrEqual(t, off, 0xffffffff, "copy offset exceeds the 4-byte operand")

	cmd := byte(0x80)
	var ops []byte
	for shift, bit := 0, byte(0x01); shift < 32; shift, bit = shift+8, bit<<1 {
		if b := byte(off >> shift); b != 0 {
			cmd |= bit
			ops = append(ops, b)
		}
	}
	for shift, bit := 0, byte(0x10); shift < 24; shift, bit = shift+8, bit<<1 {
		if b := byte(size >> shift); b != 0 {
			cmd |= bit
			ops = append(ops, b)
		}
	}
	return append(append(dst, cmd), ops...)
}

// appendDeltaInsert appends Git delta insert commands carrying lit verbatim.
// One command holds at most 127 bytes (the command byte is the length and must
// keep its high bit clear), so longer literals are split across commands.
func appendDeltaInsert(t *testing.T, dst []byte, lit []byte) []byte {
	t.Helper()
	require.NotEmpty(t, lit, "insert of 0 bytes is not encodable")
	for len(lit) > 0 {
		n := min(len(lit), 127)
		dst = append(dst, byte(n))
		dst = append(dst, lit[:n]...)
		lit = lit[n:]
	}
	return dst
}

// TestMultiHopRefDeltaChain_BorrowedAndStreaming resolves a deep ref-delta chain
// through both store entry points and asserts each level materializes to its
// expected content, and that the borrowed and streaming paths agree.
func TestMultiHopRefDeltaChain_BorrowedAndStreaming(t *testing.T) {
	const levels = 12
	packDir, contents, oids, offsets := buildRefDeltaChainPack(t, levels)

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
	// (cached) path, reading ascending on one warm store — the repeated-read
	// pattern a real scan produces. Chain depths 1..N are covered by
	// streaming_cold_per_level below, not here; see the note there.
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

	// The same levels again, each on its own store, so the streaming path
	// performs a genuine i-hop climb at every level.
	//
	// streaming_all_levels above shares one store and reads ascending, so by
	// the time level i is requested its base is already published under its
	// pack offset: walkUpDeltaChain short-circuits after one hop and every
	// level resolves through the single-hop fast path. That covers depth 1
	// repeatedly, not depths 1..N. A cold store per level is what makes the
	// streaming path build a multi-entry stack, which is also the only way to
	// reach the intermediate-publication branch of applyDeltaStackCached
	// (non-nil cache with more than one hop remaining) — the borrowed path
	// walks with a nil cache and can never enter it.
	//
	// Reaching that branch is not the same as proving it correct: the final
	// bytes are reconstructed from the arena regardless of what the cache
	// records, so dropping the intermediate oc.add calls, publishing under the
	// wrong offset, or storing wrong bytes or a wrong type would leave the
	// content assertions green. The offset-cache state is therefore asserted
	// directly after each cold read.
	t.Run("streaming_cold_per_level", func(t *testing.T) {
		for i := 1; i <= levels; i++ {
			st, err := OpenForTesting(packDir)
			require.NoError(t, err)

			got, typ, err := st.getMaterialized(oids[i])
			require.NoErrorf(t, err, "level %d", i)
			require.Equalf(t, ObjBlob, typ, "level %d type", i)
			require.Equalf(t, contents[i], got, "level %d content mismatch", i)

			// One pack holds every level, so any level's lookup yields the
			// handle the offset cache is keyed by.
			p, _, ok := st.findPackedObject(oids[i])
			require.True(t, ok)

			// Resolving level i publishes the whole prefix it walked:
			// walkUpDeltaChain caches the level-0 base record, the ping-pong
			// loop publishes each intermediate hop (levels 1..i-1), and the
			// final add publishes level i. Asserting bytes and type at each
			// offset is what makes a dropped, misplaced, or corrupt
			// publication a test failure.
			for k := 0; k <= i; k++ {
				cached, cachedTyp, ok := st.offCache.get(p, offsets[k])
				require.Truef(t, ok,
					"level %d read must publish level %d at offset %d", i, k, offsets[k])
				require.Equalf(t, ObjBlob, cachedTyp,
					"level %d read cached wrong type for level %d", i, k)
				require.Equalf(t, contents[k], cached,
					"level %d read cached wrong bytes for level %d", i, k)
			}

			// Nothing above the requested level was touched, so publishing an
			// intermediate under a neighbouring offset cannot pass the loop
			// above by accident.
			for k := i + 1; k <= levels; k++ {
				_, _, ok := st.offCache.get(p, offsets[k])
				require.Falsef(t, ok,
					"level %d read must not publish level %d", i, k)
			}

			require.NoError(t, st.Close())
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
