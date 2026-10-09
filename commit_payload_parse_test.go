package objstore

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestIndexBlankLine compares indexBlankLine with bytes.Index on boundary
// cases and randomized newline-dense inputs, including pairs that straddle
// 8-byte words and the scalar tail.
func TestIndexBlankLine(t *testing.T) {
	cases := []string{
		"", "\n", "\n\n", "a\n\n", "\n\nb", "abc", "a\nb\nc\n", "\nabc",
		strings.Repeat("x", 7) + "\n\n",
		strings.Repeat("x", 8) + "\n\n",
		strings.Repeat("x", 15) + "\n\n",
		strings.Repeat("x", 9) + "\n",
		strings.Repeat("x", 7) + "\n" + "\n",
		strings.Repeat("x", 15) + "\n" + "\n",
		strings.Repeat("\n", 17),
		"tree x\nparent y\nauthor a <a@b> 1 +0000\ncommitter c <c@d> 2 +0000\ngpgsig -----BEGIN\n abc\n def\n -----END\n\nmsg\n\nmore\n",
	}
	for _, c := range cases {
		got := indexBlankLine([]byte(c))
		want := bytes.Index([]byte(c), []byte("\n\n"))
		require.Equalf(t, want, got, "indexBlankLine(%q)", c)
	}

	rng := rand.New(rand.NewPCG(1, 2))
	alphabet := []byte("ab\n")
	for n := 0; n < 50000; n++ {
		buf := make([]byte, rng.IntN(70))
		for i := range buf {
			buf[i] = alphabet[rng.IntN(len(alphabet))]
		}
		got := indexBlankLine(buf)
		want := bytes.Index(buf, []byte("\n\n"))
		require.Equalf(t, want, got, "indexBlankLine(%q)", buf)
	}
}

// TestParseCommitPayloadMatchesSplitThenParse requires parseCommitPayload to
// agree with splitCommitPayload followed by parseAuthorHeader on hand-built
// hazards and on randomized payloads assembled from header-like lines, blank
// lines, and message text.
func TestParseCommitPayloadMatchesSplitThenParse(t *testing.T) {
	const (
		tree      = "tree 1234567890abcdef1234567890abcdef12345678\n"
		author    = "author Real Author <real@example.com> 1500000000 +0000\n"
		committer = "committer Real Committer <rc@example.com> 1600000000 +0000\n"
		impostor  = "author Evil <evil@example.com> 999 +0000\n"
		gpgsig    = "gpgsig -----BEGIN PGP SIGNATURE-----\n \n iQEzBAABCAAdFiEE\n =abcd\n -----END PGP SIGNATURE-----\n"
	)
	fixed := []string{
		"",
		"\n",
		"\n\n",
		"\nmessage\n",
		tree + author + committer + "\nmsg\n",
		tree + author + committer + "\n" + impostor,
		tree + author + committer + gpgsig + "\nmsg\n\npara 2\n",
		tree + committer + "\n" + impostor,
		tree + author + "\n" + impostor,
		tree + author,
		tree + author + strings.TrimSuffix(committer, "\n"),
		tree + author + committer,
		tree + committer + author + "\nmsg\n",
		author + author + committer + "\nmsg\n",
		"author  <x@y> 1 +0000\n\nmsg",
		"author broken line\n\nmsg",
		tree + "\n" + author + committer + "\nmsg\n",
	}
	for _, p := range fixed {
		assertFusedParseMatches(t, []byte(p))
	}

	pieces := []string{
		tree, author, committer, impostor, gpgsig, "\n", "parent 0123456789abcdef0123456789abcdef01234567\n",
		"encoding ISO-8859-1\n", "msg line\n", " continuation\n", "author", "committer ", "x",
	}
	rng := rand.New(rand.NewPCG(9, 4))
	for n := 0; n < 20000; n++ {
		var b strings.Builder
		for k := rng.IntN(9); k > 0; k-- {
			b.WriteString(pieces[rng.IntN(len(pieces))])
		}
		assertFusedParseMatches(t, []byte(b.String()))
	}
}

func assertFusedParseMatches(t *testing.T, payload []byte) {
	t.Helper()
	hdr, wantMsg := splitCommitPayload(payload)
	wantAI, wantErr := parseAuthorHeader(hdr)

	gotAI, gotMsg, gotErr := parseCommitPayload(payload)
	require.Equalf(t, wantErr != nil, gotErr != nil, "error presence for %q: split-then-parse %v, fused %v", payload, wantErr, gotErr)
	if wantErr != nil {
		require.ErrorIsf(t, gotErr, wantErr, "error identity for %q", payload)
		return
	}
	require.Equalf(t, wantAI, gotAI, "author for %q", payload)
	require.Equalf(t, string(wantMsg), string(gotMsg), "message for %q", payload)
}

// slabProbeReader implements commitPayloadReaderTo so the slab path of
// metaCache can be driven with synthetic payloads.
type slabProbeReader struct {
	payloads map[Hash][]byte
}

func (r *slabProbeReader) readCommitPayload(oid Hash) ([]byte, error) {
	return r.readCommitPayloadTo(oid, heapSink{})
}

func (r *slabProbeReader) readCommitPayloadTo(oid Hash, sink payloadSink) ([]byte, error) {
	p, ok := r.payloads[oid]
	if !ok {
		return nil, fmt.Errorf("object %x not found", oid)
	}
	dst := sink.reserve(len(p))
	copy(dst, p)
	return dst, nil
}

// TestMetaCacheSlabRetention fills several slabs through the sink path and
// then checks every cached entry: regions handed out before a slab was
// replaced must still hold their own bytes, and entries must not alias.
func TestMetaCacheSlabRetention(t *testing.T) {
	reader := &slabProbeReader{payloads: map[Hash][]byte{}}
	rng := rand.New(rand.NewPCG(5, 6))
	const commits = 400 // ~400 * ~700 B spans several 64 KiB slabs
	oids := make([]Hash, commits)
	for i := range oids {
		var oid Hash
		oid[0], oid[1] = byte(i>>8), byte(i)
		oids[i] = oid
		msg := strings.Repeat(fmt.Sprintf("m%d ", i), 20+rng.IntN(200))
		reader.payloads[oid] = []byte(fmt.Sprintf(
			"tree 1234567890abcdef1234567890abcdef12345678\n"+
				"author A%d <a%d@example.com> %d +0000\n"+
				"committer C%d <c%d@example.com> %d +0000\n\n%s",
			i, i, 1500000000+i, i, i, 1600000000+i, msg))
	}
	// One oversized payload takes the dedicated-allocation branch.
	bigOID := Hash{0xff}
	reader.payloads[bigOID] = []byte("author Big <big@example.com> 1 +0000\n\n" + strings.Repeat("z", metaSlabSize+10))
	oids = append(oids, bigOID)

	cache := newMetaCache(nil, reader)
	for _, oid := range oids {
		_, err := cache.get(oid)
		require.NoError(t, err)
	}
	require.Greater(t, cap(cache.slab), 0, "slab path was not exercised")

	for i, oid := range oids {
		meta, err := cache.get(oid)
		require.NoError(t, err)
		_, wantMsg := splitCommitPayload(reader.payloads[oid])
		require.Equalf(t, string(wantMsg), meta.Message, "message of entry %d", i)
		if oid != bigOID {
			require.Equalf(t, fmt.Sprintf("A%d", i), meta.Author.Name, "name of entry %d", i)
			require.Equalf(t, fmt.Sprintf("a%d@example.com", i), meta.Author.Email, "email of entry %d", i)
			require.Equalf(t, int64(1500000000+i), meta.Timestamp, "timestamp of entry %d", i)
		}
	}
}

// TestMetaCacheReserveRegions checks reserve directly: regions are exactly
// sized, capacity-clipped, disjoint, and a region reserved before a slab
// replacement keeps its bytes afterwards.
func TestMetaCacheReserveRegions(t *testing.T) {
	c := newMetaCache(nil, newMockCommitPayloadReader())
	first := c.reserve(100)
	require.Len(t, first, 100)
	require.Equal(t, 100, cap(first), "region capacity must not expose the rest of the slab")
	for i := range first {
		first[i] = 0xa5
	}
	// Exhaust the current slab so the next reservation starts a new one.
	for reserved := 100; reserved+metaSlabSize/2 <= metaSlabSize; reserved += metaSlabSize / 2 {
		c.reserve(metaSlabSize / 2)
	}
	second := c.reserve(metaSlabSize / 2)
	require.Len(t, second, metaSlabSize/2)
	for i := range second {
		second[i] = 0x5a
	}
	for i := range first {
		require.Equalf(t, byte(0xa5), first[i], "byte %d of the first region changed after slab replacement", i)
	}
	huge := c.reserve(metaSlabSize + 1)
	require.Len(t, huge, metaSlabSize+1)
}

// TestMetaCacheAttachGraphRefreshesTimestamps pins that a graph attached
// after entries were cached takes effect on the next lookup, and that
// detaching the graph falls back to the parsed author timestamp.
func TestMetaCacheAttachGraphRefreshesTimestamps(t *testing.T) {
	oid := Hash{0x42}
	reader := newMockCommitPayloadReader()
	reader.addCommit(oid, "Jane", "jane@example.com", 1500000000)

	g1 := &commitGraphData{Timestamps: []int64{1000}, OIDToIndex: map[Hash]int{oid: 0}}
	g2 := &commitGraphData{Timestamps: []int64{2000}, OIDToIndex: map[Hash]int{oid: 0}}

	cache := newMetaCache(g1, reader)
	meta, err := cache.get(oid)
	require.NoError(t, err)
	require.Equal(t, int64(1000), meta.Timestamp)

	cache.attachGraph(g2)
	meta, err = cache.get(oid)
	require.NoError(t, err)
	require.Equal(t, int64(2000), meta.Timestamp)

	cache.attachGraph(nil)
	meta, err = cache.get(oid)
	require.NoError(t, err)
	require.Equal(t, time.Unix(1500000000, 0).Unix(), meta.Timestamp)
	require.Equal(t, "Jane", meta.Author.Name)
}

// TestMetaCacheHitPathResolvesUnsetTimestamp covers entries inserted without
// a resolved timestamp: the hit path resolves them against the graph.
func TestMetaCacheHitPathResolvesUnsetTimestamp(t *testing.T) {
	oid := Hash{0x7}
	g := &commitGraphData{Timestamps: []int64{4242}, OIDToIndex: map[Hash]int{oid: 0}}
	cache := newMetaCache(g, newMockCommitPayloadReader())
	cache.m[oid] = metaEntry{ai: AuthorInfo{Name: "x", When: time.Unix(1, 0)}, msg: "m"}

	meta, err := cache.get(oid)
	require.NoError(t, err)
	require.Equal(t, int64(4242), meta.Timestamp)
	require.Equal(t, "m", meta.Message)
}
