// concurrent_read_test.go is the standing data-race gate for the store's
// materialization fast paths. The optimizations under scrutiny share mutable
// structures across readers — the (pack,offset) offset cache, the sharded delta
// window, and the ping-pong arena free-list — so many goroutines resolving the
// same delta chains concurrently is the scenario most likely to expose a race.
//
// Run under the race detector to get value from it:
//
//	go test -race -run TestStore_ConcurrentReaders
//
// The correctness oracle is per-object: every concurrent read of an object must
// return exactly the content computed once, up front, on a separate store.

package objstore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStore_ConcurrentReaders hammers a shared store from many goroutines,
// resolving a deep delta chain repeatedly, and asserts every read matches the
// authoritative content. It is hermetic (no git dependency) so it always runs
// in CI, where it is most useful under -race.
func TestStore_ConcurrentReaders(t *testing.T) {
	const levels = 12
	packDir, contents, oids := buildRefDeltaChainPack(t, levels)

	// Compute the authoritative content of every object on an isolated store so
	// the oracle is independent of the store under concurrent load.
	want := make(map[Hash][]byte, len(oids))
	{
		ref, err := OpenForTesting(packDir)
		require.NoError(t, err)
		for i, oid := range oids {
			got, _, err := ref.getMaterialized(oid)
			require.NoError(t, err)
			require.Equal(t, contents[i], got)
			want[oid] = append([]byte(nil), got...)
		}
		require.NoError(t, ref.Close())
	}

	st, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer st.Close()

	const (
		workers        = 32
		itersPerWorker = 200
	)
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for it := 0; it < itersPerWorker; it++ {
				// Deterministically rotate across all levels, including the
				// deepest, so chains are resolved concurrently and repeatedly.
				oid := oids[(seed+it)%len(oids)]
				got, _, err := st.getMaterialized(oid)
				if err != nil {
					errs <- err
					return
				}
				if string(got) != string(want[oid]) {
					errs <- errConcurrentMismatch(oid)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

// errConcurrentMismatch builds a descriptive error for a concurrent-read
// content mismatch without importing fmt at call sites.
func errConcurrentMismatch(oid Hash) error {
	return &concurrentMismatchError{oid: oid}
}

type concurrentMismatchError struct{ oid Hash }

func (e *concurrentMismatchError) Error() string {
	return "concurrent read returned wrong content for " + e.oid.String()
}
