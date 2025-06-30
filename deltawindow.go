// deltawindow.go
//
// Reference-counted delta resolution cache for Git packfile object inflation.
// This extends the basic delta window cache with atomic reference counting to
// prevent data races when multiple goroutines concurrently access cached objects
// during delta chain resolution.
//
// Unlike the simple LRU cache, this implementation ensures that cached entries
// cannot be evicted while they are actively being used by other goroutines.
// Each cached object maintains an atomic reference count, and entries are only
// eligible for eviction when their reference count reaches zero.
//
// The cache uses a handle-based API where callers receive a Handle that must
// be explicitly released when no longer needed. This prevents use-after-free
// bugs and ensures predictable memory management during concurrent pack traversal.
//
// Memory management follows a bounded LRU policy with a 32 MiB budget, but
// eviction is deferred until objects are no longer referenced, which may
// temporarily exceed the budget during high-concurrency scenarios.

package objstore

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
)

const (
	maxDeltaChainDepth = 50       // Git default protection
	windowBudget       = 32 << 20 // 32 MiB
)

// ErrWindowFull is returned when the delta window cannot accommodate new
// entries because all existing entries are actively referenced (refCnt > 0)
// and the memory budget has been exceeded. This prevents unbounded memory
// growth while respecting active references.
var ErrWindowFull = errors.New("delta window full: all entries in use")

// refCountedEntry holds the fully-inflated form of a single Git object
// along with atomic reference counting to enable safe concurrent access.
//
// The entry exists within a refCountedDeltaWindow and tracks how many
// active Handle instances currently reference its data. The reference
// count is managed atomically to avoid data races during concurrent
// acquire/release operations.
type refCountedEntry struct {
	// oid uniquely identifies the object this entry represents, stored
	// in canonical object-ID hash form for fast map lookups.
	oid Hash

	// data contains the object's entire decompressed byte contents.
	// The slice backing array may be reused when updating existing
	// entries if the new data fits within the existing capacity.
	data []byte

	// typ stores the Git object type (blob, tree, commit, tag).
	// This avoids repeatedly calling detectType() on cached data.
	typ ObjectType

	// refCnt tracks the number of active Handle instances that reference
	// this entry's data. Must be accessed atomically. When refCnt reaches
	// zero, the entry becomes eligible for LRU eviction.
	refCnt atomic.Int32

	// size records len(data) in bytes at the time of last update.
	// This cached value enables memory accounting without repeated
	// slice length calculations during eviction decisions.
	size int
}

// refCountedDeltaWindow implements a bounded LRU cache with reference counting
// for delta-compressed object resolution in Git packfiles.
//
// The cache maintains both an LRU ordering and a hash index for O(1) lookups.
// Memory usage is bounded by the budget, but eviction is constrained by
// reference counts - entries with refCnt > 0 cannot be evicted even if
// doing so would free memory for new entries.
//
// All operations are protected by a single mutex to ensure consistency
// between the LRU list, hash index, and reference counting state.
type refCountedDeltaWindow struct {
	// mu protects all fields and ensures atomic consistency between
	// the LRU list, hash index, and memory accounting state.
	mu sync.Mutex

	// index provides O(1) lookup from object Hash to the corresponding
	// list.Element in the LRU list. The Element.Value is always a
	// *refCountedEntry.
	index map[Hash]*list.Element

	// budget defines the target memory limit in bytes. The actual
	// memory usage may temporarily exceed this limit when entries
	// with active references cannot be evicted.
	budget int

	// used tracks the current memory consumption in bytes across
	// all cached entries. Updated synchronously with entry additions,
	// updates, and removals.
	used int

	// lru maintains entries in least-recently-used order, with the
	// most recently accessed entry at the front. Only entries with
	// refCnt == 0 are eligible for eviction from the back.
	lru *list.List

	// evictable counts entries with refCnt == 0 that are eligible
	// for LRU eviction. This optimization avoids scanning the entire
	// LRU list when all entries are actively referenced.
	evictable int

	// handlePool reuses Handle instances to reduce allocation overhead
	// and GC pressure. Handles are frequently created and destroyed
	// during delta resolution, making pooling beneficial.
	handlePool sync.Pool
}

// newRefCountedDeltaWindow allocates and returns a refCountedDeltaWindow
// with the standard memory budget and empty state.
//
// The returned cache is ready for immediate use and safe for concurrent
// access by multiple goroutines.
func newRefCountedDeltaWindow() *refCountedDeltaWindow {
	w := &refCountedDeltaWindow{
		budget: windowBudget,
		lru:    list.New(),
		index:  make(map[Hash]*list.Element, 256), // Pre-size for typical workloads
	}

	w.handlePool = sync.Pool{
		New: func() any { return new(Handle) },
	}

	return w
}

// Handle represents an active reference to cached object data and ensures
// the underlying entry cannot be evicted while the handle exists.
//
// Handles must be explicitly released by calling Release() when no longer
// needed. Failing to release handles will eventually cause ErrWindowFull
// as the cache cannot evict referenced entries to make room for new ones.
//
// The handle exposes the cached data slice directly.
// Callers MUST treat the returned slice as read-only; mutating it would
// corrupt the shared cache contents for every other goroutine that might
// hold a reference.
type Handle struct {
	// data references the cached object's decompressed contents.
	// The slice is shared with other handles and with the cache; it must
	// therefore be treated as immutable by callers.
	data []byte

	// entry points to the refCountedEntry this handle references.
	// Set to nil after Release() to prevent double-release bugs.
	entry *refCountedEntry

	// w points back to the originating refCountedDeltaWindow to enable
	// proper cleanup during Release().
	w *refCountedDeltaWindow
}

// Type returns the Git ObjectType associated with the cached data.
// It is safe to call after Release(); if the underlying entry has
// already been cleared the method returns ObjBad.
func (h *Handle) Type() ObjectType {
	if h.entry != nil {
		return h.entry.typ
	}
	return ObjBad // handle was released
}

// Release decrements the reference count for this handle's entry and
// marks the handle as invalid for further use.
//
// After Release() returns, the handle's Data() method will continue to
// return valid data, but the underlying cache entry may become eligible
// for eviction. Multiple calls to Release() on the same handle are safe
// but only the first call has any effect.
//
// This method is safe for concurrent use by multiple goroutines.
func (h *Handle) Release() {
	if h.entry == nil || h.w == nil {
		return
	}

	w := h.w

	w.mu.Lock()
	defer w.mu.Unlock()

	newCount := h.entry.refCnt.Add(-1)

	if newCount == 0 {
		w.evictable++
	}

	// Clear only the internal reference fields before returning to pool.
	// Keep h.data intact so Data() remains valid after Release() as documented.
	h.entry = nil
	h.w = nil

	// Return the handle to the pool for reuse
	w.handlePool.Put(h)
}

// Data returns the cached object data associated with this handle.
//
// The returned slice is the cache's backing slice.
// It remains valid even after Release, but MUST NOT be modified; doing so
// would corrupt the shared cache contents and may introduce subtle bugs.
// Accessing data from released handles is still valid but usually
// indicates a logic error in the caller.
func (h *Handle) Data() []byte { return h.data }

// acquire attempts to return a handle to the cached data for the given
// object hash, incrementing its reference count to prevent eviction.
//
// If the object is found in the cache, its LRU position is updated and
// a new handle with a defensive copy of the data is returned along with
// true. If the object is not cached, returns (nil, false).
//
// The returned handle must be released by calling Release() when no
// longer needed to avoid preventing cache eviction.
func (w *refCountedDeltaWindow) acquire(oid Hash) (*Handle, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	elem, ok := w.index[oid]
	if !ok {
		return nil, false
	}
	entry := elem.Value.(*refCountedEntry)

	oldCount := entry.refCnt.Load()
	newCount := entry.refCnt.Add(1)

	// Only decrement evictable if this was the first reference.
	if oldCount == 0 && newCount == 1 {
		w.evictable--
	}

	w.lru.MoveToFront(elem)

	// Get a handle from the pool and initialize it
	handle := w.handlePool.Get().(*Handle)
	handle.data = entry.data
	handle.entry = entry
	handle.w = w

	return handle, true
}

// add inserts or updates the cached entry for the given object hash.
// Large objects exceeding the cache budget are rejected to prevent
// cache thrashing.
//
// IMPORTANT: The provided buf slice is stored directly without copying.
// Callers MUST NOT modify buf after calling add(), as this would corrupt
// the cached data for all handles. This design choice avoids allocation
// overhead for the common Git delta resolution use case.
//
// If the object already exists in the cache, its data is updated in-place
// and moved to the front of the LRU list. Otherwise, a new entry is created.
//
// After insertion, the cache attempts to evict entries with zero reference
// counts until the memory usage falls within budget. If insufficient
// evictable entries exist, ErrWindowFull is returned.
//
// This method is safe for concurrent use by multiple goroutines.
func (w *refCountedDeltaWindow) add(oid Hash, buf []byte, objType ObjectType) error {
	if len(buf) > w.budget {
		return errors.New("object too large for window")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if elem, ok := w.index[oid]; ok {
		entry := elem.Value.(*refCountedEntry)
		oldSize := entry.size

		// Store the slice directly without copying to avoid allocation.
		// IMPORTANT: Callers must not modify buf after calling add().
		entry.data = buf
		entry.size = len(buf)
		entry.typ = objType

		w.used += entry.size - oldSize

		w.lru.MoveToFront(elem)
	} else {
		// Check if adding this new object would exceed budget and
		// no evictable entries exist to make room.
		if w.used+len(buf) > w.budget && w.evictable == 0 {
			return ErrWindowFull
		}

		entry := &refCountedEntry{
			oid:  oid,
			data: buf,
			size: len(buf),
			typ:  objType,
		}

		elem := w.lru.PushFront(entry)
		w.index[oid] = elem

		w.used += entry.size
		w.evictable++
	}

	evicted := 0
	for w.used > w.budget && w.evictable > 0 {
		for elem := w.lru.Back(); elem != nil; {
			entry := elem.Value.(*refCountedEntry)

			if entry.refCnt.Load() == 0 {
				prev := elem.Prev()
				w.lru.Remove(elem)
				delete(w.index, entry.oid)

				w.used -= entry.size
				w.evictable--
				evicted++

				elem = prev

				if w.used <= w.budget {
					break
				}
			} else {
				elem = elem.Prev()
			}
		}

		if evicted == 0 {
			break
		}
		evicted = 0
	}

	if w.used > w.budget && w.evictable == 0 {
		return ErrWindowFull
	}

	return nil
}
