// deltawindow.go
//
// Delta resolution cache for Git packfile object inflation.
// The cache maps *recently inflated object hashes* → *decompressed object data*
// to optimize delta chain resolution by avoiding redundant decompression during
// sequential pack traversal. This provides significant performance improvements
// when multiple delta objects reference the same base within a short timeframe.
//
// The implementation uses a bounded LRU cache with a 32 MiB memory budget,
// automatically evicting least‑recently‑used entries when the limit is exceeded.
// Large objects that exceed the budget are deliberately skipped to prevent
// cache thrashing and maintain working set locality.

package objstore

import (
	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	maxDeltaChainDepth = 50       // Git default protection
	windowBudget       = 32 << 20 // 32 MiB
)

// windowEntry holds the fully-inflated form of a single Git object.
//
// A windowEntry lives only inside a deltaWindow and exists for as long as it
// is needed to resolve delta chains during pack-file decoding. Callers must
// treat the contained data as immutable.
type windowEntry struct {
	// oid uniquely identifies the object the entry represents. The hash is
	// stored in canonical object-ID form.
	oid Hash

	// data contains the object's entire, decompressed byte contents. The slice
	// must not be modified after it has been added to the cache.
	data []byte

	// size records len(data) in bytes so callers can reason about the memory
	// footprint without repeatedly recomputing it.
	size int
}

// deltaWindow caches recently-inflated objects while resolving
// delta-compressed entries in a pack file.
//
// The cache is bounded by windowBudget and evicts objects in least-recently-
// used (LRU) order. The type wraps an lru.Cache that is safe for concurrent
// use, so deltaWindow may be shared freely among goroutines.
type deltaWindow struct {
	// entries maps an object's Hash to its inflated data. The underlying cache
	// enforces the LRU eviction policy and guards all internal state with its
	// own synchronization primitives.
	entries *lru.Cache[Hash, []byte]
}

// newDeltaWindow allocates and returns a deltaWindow whose underlying LRU
// cache is capped at windowBudget bytes.
//
// The function returns an error if the lru package fails to create the cache,
// which is considered a non-recoverable initialization error.
func newDeltaWindow() (*deltaWindow, error) {
	cache, err := lru.New[Hash, []byte](windowBudget)
	return &deltaWindow{entries: cache}, err
}

// lookup returns the fully-inflated data associated with the provided Hash.
// The boolean result reports whether the entry was found in the window.
func (w *deltaWindow) lookup(h Hash) ([]byte, bool) { return w.entries.Get(h) }

// add inserts the given decompressed object into the window so that subsequent
// delta resolution can reuse it.
//
// Objects whose size exceeds windowBudget are deliberately skipped to prevent
// a single, very large object from evicting the entire working set.
func (w *deltaWindow) add(h Hash, buf []byte) {
	if len(buf) > windowBudget {
		return // Too big – skip caching.
	}
	w.entries.Add(h, buf)
}
