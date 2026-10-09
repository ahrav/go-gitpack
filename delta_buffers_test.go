package objstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeltaBufClassBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		n     int
		class int
	}{
		{0, 0},
		{1, 0},
		{1 << deltaBufMinShift, 0},
		{1<<deltaBufMinShift + 1, 1},
		{1 << (deltaBufMinShift + 1), 1},
		{1 << deltaBufMaxShift, deltaBufClasses - 1},
		{1<<deltaBufMaxShift + 1, -1},
	}
	for _, c := range cases {
		require.Equal(t, c.class, deltaBufClass(c.n), "n=%d", c.n)
	}
}

func TestGetDeltaBufCapacityAndReuse(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 100, 4096, 4097, 1 << 20, 1<<24 + 5} {
		b := getDeltaBuf(n)
		require.Zero(t, len(b), "n=%d", n)
		require.GreaterOrEqual(t, cap(b), n, "n=%d", n)
		if class := deltaBufClass(n); class >= 0 {
			require.Equal(t, 1<<(class+deltaBufMinShift), cap(b), "pooled class capacity for n=%d", n)
		} else {
			require.Equal(t, n, cap(b), "direct allocation is exact for n=%d", n)
		}
		putDeltaBuf(b)
	}

	// A buffer written through the pool keeps its bytes until reused: the
	// pool hands out dirty buffers, so callers must overwrite what they read.
	b := getDeltaBuf(100)
	b = append(b[:0], "payload"...)
	putDeltaBuf(b)
	again := getDeltaBuf(100)
	require.Zero(t, len(again))
	require.GreaterOrEqual(t, cap(again), 100)
}

func TestPutDeltaBufDropsNonClassCapacities(t *testing.T) {
	t.Parallel()
	// Capacities that are not a pooled power of two are dropped silently.
	putDeltaBuf(make([]byte, 0, 1000))
	putDeltaBuf(make([]byte, 0, 1<<(deltaBufMaxShift+1)))
	putDeltaBuf(nil)
}

func TestSetDeltaArenaBudgetRecordsPrevious(t *testing.T) {
	first := SetDeltaArenaBudget(4 * DeltaArenaSize)
	defer SetDeltaArenaBudget(first)
	require.Equal(t, 4*DeltaArenaSize, SetDeltaArenaBudget(2*DeltaArenaSize),
		"the deprecated setter reports the value it replaced")
}
