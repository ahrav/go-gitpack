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
//
// Both subtests here start their workers behind a release barrier, because the
// races worth catching live in the code that runs before the caches are warm.
// getMaterialized consults the delta window and the ARC cache before doing any
// work, so on a shared warm store all but the first read of each OID returns
// from cache without touching walk-up, reconstruction, or the arena. Launching
// goroutines in a loop and hoping they overlap leaves that window to the
// scheduler: one worker can warm every chain before another starts, and the run
// still passes having exercised almost nothing concurrently.

package objstore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStore_ConcurrentReaders hammers the store from many goroutines, resolving
// a deep delta chain repeatedly, and asserts every read matches the
// authoritative content. It is hermetic (no git dependency) so it always runs in
// CI, where it is most useful under -race.
func TestStore_ConcurrentReaders(t *testing.T) {
	const levels = 12
	packDir, contents, oids, _ := buildRefDeltaChainPack(t, levels)

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

	const (
		workers        = 32
		itersPerWorker = 200
		coldWaves      = 8
	)

	// runConcurrently releases `workers` goroutines simultaneously and fails the
	// test if any reports an error. read is invoked with the worker index and
	// iteration so callers can spread work across the chain deterministically.
	runConcurrently := func(t *testing.T, iters int, read func(worker, iter int) error) {
		t.Helper()
		var (
			start   = make(chan struct{})
			wg      sync.WaitGroup
			errs    = make(chan error, workers)
			pending sync.WaitGroup
		)
		pending.Add(workers)
		for w := range workers {
			wg.Add(1)
			go func(seed int) {
				defer wg.Done()
				// Report readiness, then block until every worker is parked on
				// start, so the cold phase is genuinely contended.
				pending.Done()
				<-start
				for it := range iters {
					if err := read(seed, it); err != nil {
						errs <- err
						return
					}
				}
			}(w)
		}
		pending.Wait()
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
	}

	// checkOID resolves one OID through the cached path and compares it against
	// the oracle.
	checkOID := func(st *store, oid Hash) error {
		got, _, err := st.getMaterialized(oid)
		if err != nil {
			return err
		}
		if string(got) != string(want[oid]) {
			return errConcurrentMismatch(oid)
		}
		return nil
	}

	// Cold waves: each wave gets a brand-new store, so every wave's contended
	// phase starts with an empty offset cache, delta window, and ARC. The delta
	// window has no reset hook, so a fresh store is how a wave is made cold.
	// Repeating the wave is what turns the narrow cold window into a scenario
	// the race detector gets many chances to observe.
	t.Run("cold_waves", func(t *testing.T) {
		for wave := range coldWaves {
			st, err := OpenForTesting(packDir)
			require.NoErrorf(t, err, "wave %d", wave)

			// Short waves keep the run inside the cold window rather than
			// spending it on cache hits; cold_waves proves contention during
			// first materialization, and no_cache below covers sustained
			// full-chain concurrency.
			runConcurrently(t, 4, func(seed, it int) error {
				// Rotate across all levels, deepest included, so different
				// workers climb overlapping chains at the same time.
				return checkOID(st, oids[(seed+it)%len(oids)])
			})

			require.NoErrorf(t, st.Close(), "wave %d", wave)
		}
	})

	// Sustained full-chain concurrency. getPackedObjectNoCache takes the
	// borrowed path, which neither consults nor publishes the offset cache and
	// never writes the delta window (see inflateDeltaChainBorrowed), so every
	// one of these reads performs a real multi-hop walk plus a full ping-pong
	// reconstruction no matter how many reads preceded it. That makes the arena
	// free-list and the walk-up path contended for the whole run rather than
	// only until the caches warm.
	t.Run("no_cache", func(t *testing.T) {
		st, err := OpenForTesting(packDir)
		require.NoError(t, err)
		defer st.Close()

		runConcurrently(t, itersPerWorker, func(seed, it int) error {
			oid := oids[(seed+it)%len(oids)]
			p, off, ok := st.findPackedObject(oid)
			if !ok {
				return errConcurrentMismatch(oid)
			}
			got, _, err := st.getPackedObjectNoCache(p, off, oid)
			if err != nil {
				return err
			}
			if string(got) != string(want[oid]) {
				return errConcurrentMismatch(oid)
			}
			return nil
		})
	})
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
