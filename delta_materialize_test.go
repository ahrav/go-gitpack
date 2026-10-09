package objstore

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A cold multi-hop chain resolved through get publishes every hop under its
// pack offset, the published bytes are the hop's exact content, and the
// buffers never alias one another, so a later chain sharing the tail stops at
// a cached hop and reads an immutable base.
func TestMultiHopChain_PublishesExactHopsToOffsetCache(t *testing.T) {
	const levels = 6
	packDir, contents, oids := buildRefDeltaChainPack(t, levels)
	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()

	top, typ, err := st.get(oids[levels])
	require.NoError(t, err)
	require.Equal(t, ObjBlob, typ)
	require.Equal(t, contents[levels], top)
	require.Equal(t, len(top), cap(top), "final hop buffer is sized to its target")

	seen := map[*byte]int{}
	for i := 0; i <= levels; i++ {
		p, off, ok := st.findPackedObject(oids[i])
		require.True(t, ok)
		data, typ, ok := st.offCache.get(p, off)
		require.Truef(t, ok, "level %d is published under its offset", i)
		assert.Equal(t, ObjBlob, typ)
		assert.Equalf(t, contents[i], data, "level %d content", i)
		if len(data) > 0 {
			seen[&data[0]]++
		}
	}
	for ptr, n := range seen {
		assert.Equalf(t, 1, n, "buffer %p is shared by %d hops", ptr, n)
	}

	// A second store resolving the chain from its top with a warm tail
	// yields the same bytes.
	warm, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer warm.Close()
	mid, _, err := warm.get(oids[levels/2])
	require.NoError(t, err)
	require.Equal(t, contents[levels/2], mid)
	again, _, err := warm.get(oids[levels])
	require.NoError(t, err)
	require.Equal(t, contents[levels], again)
}

// copyDeltaObject encodes a ref-delta whose target is `repeat` copies of the
// base's first `chunk` bytes followed by tail, using copy instructions, so a
// small payload materializes a target far larger than its base.
func copyDeltaObject(t *testing.T, baseOID Hash, base []byte, chunk, repeat int, tail []byte) (obj []byte, target []byte) {
	t.Helper()
	var d bytes.Buffer
	target = make([]byte, 0, chunk*repeat+len(tail))
	for range repeat {
		target = append(target, base[:chunk]...)
	}
	target = append(target, tail...)
	writeVarInt(&d, uint64(len(base)))
	writeVarInt(&d, uint64(len(target)))
	for range repeat {
		for off := 0; off < chunk; off += 0x10000 {
			n := min(0x10000, chunk-off)
			// Copy: offset in 3 bytes, size in 2 bytes (0 encodes 0x10000).
			d.WriteByte(0x80 | 0x01 | 0x02 | 0x04 | 0x10 | 0x20)
			d.WriteByte(byte(off))
			d.WriteByte(byte(off >> 8))
			d.WriteByte(byte(off >> 16))
			d.WriteByte(byte(n))
			d.WriteByte(byte(n >> 8))
		}
	}
	for i := 0; i < len(tail); i += 127 {
		n := min(127, len(tail)-i)
		d.WriteByte(byte(n))
		d.Write(tail[i : i+n])
	}
	var buf bytes.Buffer
	buf.Write(encodeObjHeader(uint8(ObjRefDelta), uint64(d.Len())))
	buf.Write(baseOID[:])
	buf.Write(zlibCompress(t, d.Bytes()))
	return buf.Bytes(), target
}

// A chain whose hops grow past 16 MiB materializes to the bytes its OIDs
// name. The earlier ping-pong arena regrew mid-chain for such targets and
// corrupted two of 635 blobs over 1 MiB in the Linux history; exact per-hop
// buffers have no size-dependent path.
func TestMultiHopChain_LargeTargetsMaterializeExactly(t *testing.T) {
	const chunk = 1 << 20
	base := make([]byte, chunk)
	for i := range base {
		base[i] = byte(i*7 + i>>8)
	}
	baseOID := calculateHash(ObjBlob, base)

	// Level 1: 20 MiB (above the old 16 MiB arena half), level 2 appends to
	// level 1 through copies of its first MiB plus an inserted tail.
	obj1, lvl1 := copyDeltaObject(t, baseOID, base, chunk, 20, []byte("tail-one\n"))
	oid1 := calculateHash(ObjBlob, lvl1)
	obj2, lvl2 := copyDeltaObject(t, oid1, lvl1, chunk, 21, []byte("tail-two\n"))
	oid2 := calculateHash(ObjBlob, lvl2)

	dir := t.TempDir()
	packDir := filepath.Join(dir, "pack")
	require.NoError(t, os.MkdirAll(packDir, 0o755))
	var pack bytes.Buffer
	pack.WriteString("PACK")
	binary.Write(&pack, binary.BigEndian, uint32(2))
	binary.Write(&pack, binary.BigEndian, uint32(3))
	offsets := make([]uint64, 3)
	offsets[0] = uint64(pack.Len())
	pack.Write(encodeObjHeader(uint8(ObjBlob), uint64(len(base))))
	pack.Write(zlibCompress(t, base))
	offsets[1] = uint64(pack.Len())
	pack.Write(obj1)
	offsets[2] = uint64(pack.Len())
	pack.Write(obj2)
	trailer := sha1.Sum(pack.Bytes())
	pack.Write(trailer[:])
	require.NoError(t, os.WriteFile(filepath.Join(packDir, "big.pack"), pack.Bytes(), 0o644))
	require.NoError(t, createV2IndexFile(filepath.Join(packDir, "big.idx"), []Hash{baseOID, oid1, oid2}, offsets))

	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()
	st.SetMaxDeltaObjectSize(0)

	for _, want := range []struct {
		oid  Hash
		data []byte
	}{{oid2, lvl2}, {oid1, lvl1}, {baseOID, base}} {
		got, typ, err := st.get(want.oid)
		require.NoError(t, err)
		require.Equal(t, ObjBlob, typ)
		require.Equal(t, len(want.data), len(got))
		require.Equal(t, want.oid, calculateHash(ObjBlob, got), "materialized bytes hash to the object's OID")
		require.True(t, bytes.Equal(want.data, got))
	}
}
