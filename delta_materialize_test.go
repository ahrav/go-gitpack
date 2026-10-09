package objstore

import (
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
