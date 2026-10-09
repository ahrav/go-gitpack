package objstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPairFirstSeen_RegisterMinKeepsLowestPosition(t *testing.T) {
	r := newPairFirstSeen()
	k := makePairKey(Hash{1}, Hash{2})

	_, ok := r.lookup(k)
	assert.False(t, ok, "unregistered pair")

	r.registerMin(k, 40, false)
	e, ok := r.lookup(k)
	require.True(t, ok)
	assert.Equal(t, uint64(40), e.seq)
	assert.False(t, e.binary)

	r.registerMin(k, 90, true)
	e, _ = r.lookup(k)
	assert.Equal(t, uint64(40), e.seq, "later position keeps the earlier entry")
	assert.False(t, e.binary)

	r.registerMin(k, 10, true)
	e, _ = r.lookup(k)
	assert.Equal(t, uint64(10), e.seq, "earlier position replaces the entry")
	assert.True(t, e.binary)

	_, ok = r.lookup(makePairKey(Hash{2}, Hash{1}))
	assert.False(t, ok, "reversed pair is a distinct key")
}

// repeatedPairRepo builds a history where the same (old, new) blob transition
// happens on two branches, so the dedup pipeline meets the pair twice: on
// the earlier branch its lines are new, on the later one every line is a
// repeat. A binary file goes through the same transition on both branches.
func repeatedPairRepo(t *testing.T) string {
	b := newDedupRepoBuilder(t)
	b.write("shared.txt", uniqueText("shared-v1", 8))
	b.write("blob.bin", "\x00\x01\x02v1\n")
	b.commit("base")
	base := b.git("rev-parse", "HEAD")

	b.write("shared.txt", uniqueText("shared-v2", 8))
	b.write("blob.bin", "\x00\x01\x02v2\n")
	b.commit("first transition")

	b.git("checkout", "-q", "-b", "side", base)
	b.write("other.txt", uniqueText("other", 3))
	b.commit("side filler")
	b.write("shared.txt", uniqueText("shared-v2", 8))
	b.write("blob.bin", "\x00\x01\x02v2\n")
	b.commit("second transition")
	return b.finish()
}

func TestDiffHistoryHunksDedup_RepeatedPairMatchesSerialReference(t *testing.T) {
	gitDir := repeatedPairRepo(t)
	want := serialDedupReference(t, gitDir, false, nil)
	got := collectHunkScan(t, gitDir, WithHunkLineDedup(true))
	require.Equal(t, want, got)

	text, binary := 0, 0
	for _, h := range got {
		switch {
		case strings.Contains(h, "|shared.txt|"):
			text++
		case strings.Contains(h, "|blob.bin|"):
			binary++
		}
	}
	// The base commit adds both files; the v1->v2 transition then appears
	// once for the text file and twice for the binary file.
	assert.Equal(t, 2, text, "repeated text pair is emitted at its first introduction only")
	assert.Equal(t, 3, binary, "binary hunks are emitted at every occurrence")
}

func TestDiffHistoryHunksDedup_SaturatedSetEmitsSkippedRepeats(t *testing.T) {
	gitDir := repeatedPairRepo(t)
	// A 16-byte budget saturates the fingerprint table within the first
	// commit, so every later verdict fails open.
	s, err := NewHistoryScanner(gitDir, WithHunkLineDedup(true), WithHunkDedupBudget(16))
	require.NoError(t, err)
	defer s.Close()
	got := collectHunkScanWith(t, s)
	want := collectHunkScan(t, gitDir)
	require.Equal(t, want, got, "saturated dedup scan equals the plain scan")
}
