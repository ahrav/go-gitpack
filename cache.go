// package objstore provides a minimal, memory-mapped Git object store that
// resolves objects directly from *.pack files without shelling out to the
// Git executable.
//
// This file implements two cache layers used during object resolution:
//
//   - refCountedDeltaWindow: a bounded, reference-counted LRU cache for
//     intermediate delta bases. It keeps recently resolved objects in memory
//     so that subsequent delta applications can reuse them without re-reading
//     the packfile. The window cooperates with Handle-based reference
//     counting so that actively-used entries are never evicted.
//
//   - arcCache: an Adaptive Replacement Cache (ARC) that sits above the delta
//     window and caches fully resolved objects. ARC balances recency and
//     frequency to handle both scan-like (one-pass) and lookup-like (repeated
//     access) workloads.
//
// The two caches serve complementary roles: the delta window is scoped to
// packfile delta resolution and is consulted during the innermost inflate
// loop, while the ARC cache is the top-level "have I already resolved this
// OID?" check used by store.get().
package objstore

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/golang-lru/arc/v2"
)

const (
	// windowBudget (32 MiB) is the memory budget for the refCountedDeltaWindow.
	// This value matches Git's default pack.windowMemory setting. 32 MiB is
	// large enough to hold several dozen typical delta bases concurrently,
	// which is sufficient to resolve most delta chains without eviction
	// pressure, while remaining small enough to avoid significant RSS impact
	// when multiple HistoryScanners coexist in a process.
	windowBudget = 32 << 20
)

// ErrWindowFull is returned when the delta window cannot accommodate new
// entries because all existing entries are actively referenced (refCnt > 0)
// and the memory budget has been exceeded. This prevents unbounded memory
// growth while respecting active references.
var (
	ErrWindowFull     = errors.New("delta window full: all entries in use")
	ErrObjectTooLarge = errors.New("object too large for window")
)

// refCountedEntry holds the fully-inflated form of a single Git object
// along with atomic reference counting to enable safe concurrent access.
//
// The entry participates in an intrusive doubly-linked list for LRU ordering,
// eliminating the per-element heap allocation of container/list.
type refCountedEntry struct {
	oid  Hash
	data []byte
	typ  ObjectType

	// refCnt tracks the number of active Handle instances that reference
	// this entry's data. Must be accessed atomically.
	refCnt atomic.Int32

	size int

	// Intrusive linked-list pointers for LRU ordering.
	prev, next *refCountedEntry
}

// intrusiveList is a doubly-linked list with sentinel nodes that avoids
// per-element heap allocation. All entry nodes embed prev/next pointers
// directly, keeping LRU traversal cache-friendly.
//
// Sentinel node invariant: head and tail are non-nil dummy entries that are
// NEVER removed or evicted. They exist solely to eliminate nil checks in the
// insert/remove paths. When the list is empty, head.next == tail and
// tail.prev == head. Data entries always satisfy entry != head && entry !=
// tail. All list mutation methods (pushFront, remove, moveToFront) guard
// against being called with a sentinel.
type intrusiveList struct {
	// head.next is the most recently used entry.
	// tail.prev is the least recently used entry.
	head, tail *refCountedEntry
	len        int
}

func newIntrusiveList() *intrusiveList {
	l := &intrusiveList{
		head: &refCountedEntry{},
		tail: &refCountedEntry{},
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	return l
}

func (l *intrusiveList) Len() int { return l.len }

// pushFront inserts entry at the front (MRU position).
func (l *intrusiveList) pushFront(e *refCountedEntry) {
	if e == nil || l == nil || l.head == nil || l.tail == nil {
		return
	}
	// Defensive: if the caller tries to push an already-linked node, unlink it
	// first to keep list links and length consistent.
	if e.prev != nil && e.next != nil {
		l.remove(e)
	}
	e.prev = l.head
	e.next = l.head.next
	l.head.next.prev = e
	l.head.next = e
	l.len++
}

// remove unlinks entry from the list.
func (l *intrusiveList) remove(e *refCountedEntry) {
	if e == nil || l == nil || l.head == nil || l.tail == nil {
		return
	}
	if e == l.head || e == l.tail {
		return
	}
	// If pointers are missing, the entry is already detached.
	if e.prev == nil || e.next == nil {
		e.prev = nil
		e.next = nil
		return
	}
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = nil
	e.next = nil
	if l.len > 0 {
		l.len--
	}
}

// moveToFront moves an already-linked entry to the front.
func (l *intrusiveList) moveToFront(e *refCountedEntry) {
	if e == nil || l == nil || l.head == nil || l.tail == nil {
		return
	}
	if e == l.head || e == l.tail {
		return
	}
	// If the node is detached for any reason, reinsert it at the front.
	if e.prev == nil || e.next == nil {
		l.pushFront(e)
		return
	}
	if l.head.next == e {
		return // already at front
	}
	// Unlink.
	e.prev.next = e.next
	e.next.prev = e.prev
	// Re-link at front.
	e.prev = l.head
	e.next = l.head.next
	l.head.next.prev = e
	l.head.next = e
}

// back returns the least recently used entry, or nil if empty.
func (l *intrusiveList) back() *refCountedEntry {
	if l == nil || l.head == nil || l.tail == nil {
		return nil
	}
	entry := l.tail.prev
	if entry == nil || entry == l.head {
		return nil
	}
	return entry
}

// refCountedDeltaWindow implements a bounded LRU cache with reference counting
// for delta-compressed object resolution in Git packfiles.
//
// The cache uses an intrusive doubly-linked list for LRU ordering, keeping all
// entry metadata contiguous and avoiding per-element heap allocations.
//
// Thread safety: all public methods (acquire, add) are protected by mu. The
// evictable counter uses atomic operations for fast-path reads (e.g.
// checking whether any entry can be evicted before acquiring the lock), but
// mutations to evictable are always performed while mu is held to maintain
// the invariant:
//
//	evictable == (number of entries in the LRU with refCnt == 0)
//
// Relationship to arcCache: refCountedDeltaWindow lives inside the delta
// resolution hot path and caches *intermediate* base objects needed to apply
// OFS_DELTA / REF_DELTA instructions. The arcCache, by contrast, caches
// *fully resolved* objects at the store.get() level. A single object may
// appear in both caches simultaneously (the delta window retains the base,
// while the ARC cache retains the final result). The two caches have
// independent eviction policies and memory budgets.
type refCountedDeltaWindow struct {
	mu sync.Mutex

	// index provides O(1) lookup from object Hash to entry.
	index map[Hash]*refCountedEntry

	budget int
	used   int

	lru *intrusiveList

	// evictable counts entries with refCnt == 0. It is kept in sync with the
	// actual count of zero-refCnt entries in the LRU under mu. The atomic
	// type allows fast reads outside the lock (e.g. in add's pre-check) but
	// writes always happen under mu to prevent drift.
	evictable atomic.Int32

	handlePool sync.Pool
}

// newRefCountedDeltaWindow allocates and returns a refCountedDeltaWindow
// with the standard memory budget and empty state.
func newRefCountedDeltaWindow() *refCountedDeltaWindow {
	const defaultIndexSize = 256
	w := &refCountedDeltaWindow{
		budget: windowBudget,
		lru:    newIntrusiveList(),
		index:  make(map[Hash]*refCountedEntry, defaultIndexSize),
	}

	w.handlePool = sync.Pool{
		New: func() any { return new(Handle) },
	}

	return w
}

// Handle represents an active reference to cached object data and ensures
// the underlying entry cannot be evicted while the handle exists.
type Handle struct {
	data  []byte
	entry *refCountedEntry
	w     *refCountedDeltaWindow
}

// Type returns the Git ObjectType associated with the cached data.
func (h *Handle) Type() ObjectType {
	if h.entry != nil {
		return h.entry.typ
	}
	return ObjBad
}

// Release decrements the reference count for this handle's entry and
// marks the handle as invalid for further use.
//
// Idempotency: Release is safe to call multiple times. After the first call,
// h.entry and h.w are set to nil, so subsequent calls are no-ops. This makes
// it safe to defer Release() and also call it explicitly in error paths
// without risk of double-decrementing the reference count.
//
// After Release returns, the Handle is returned to the sync.Pool for reuse.
// The caller MUST NOT access h.Data() after calling Release.
func (h *Handle) Release() {
	if h.entry == nil || h.w == nil {
		return
	}
	w := h.w
	entry := h.entry

	// Clear handle fields before returning to pool.
	h.entry = nil
	h.w = nil

	w.mu.Lock()
	newCount := entry.refCnt.Add(-1)
	if newCount == 0 {
		w.evictable.Add(1)
	}
	w.mu.Unlock()

	w.handlePool.Put(h)
}

// Data returns the cached object data associated with this handle.
//
// Lifetime: the returned slice is valid only as long as the Handle has not
// been released. Once Release() is called, the underlying entry may be
// evicted and its data buffer reused or garbage-collected. Callers that need
// the data beyond the Handle's lifetime MUST copy the slice before releasing.
func (h *Handle) Data() []byte { return h.data }

// acquire attempts to return a handle to the cached data for the given
// object hash, incrementing its reference count to prevent eviction.
func (w *refCountedDeltaWindow) acquire(oid Hash) (*Handle, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry, ok := w.index[oid]
	if !ok {
		return nil, false
	}

	newCount := entry.refCnt.Add(1)
	if newCount == 1 {
		w.evictable.Add(-1)
	}

	w.lru.moveToFront(entry)

	handle := w.handlePool.Get().(*Handle)
	handle.data = entry.data
	handle.entry = entry
	handle.w = w

	return handle, true
}

// add inserts or updates the cached entry for the given object hash.
//
// If the OID already exists in the window, the entry's data, size, and type
// are updated in-place without changing the reference count or creating a new
// entry node. The memory accounting (w.used) is adjusted by the size delta,
// and the entry is promoted to MRU position. This in-place update avoids
// dangling Handle references that would occur if the old entry were evicted
// and a new one inserted.
//
// If the OID is new, the eviction algorithm proceeds as follows:
//
//  1. If the new entry fits within the remaining budget (used + len(buf) <=
//     budget), it is inserted at the MRU position with refCnt 0 (evictable).
//
//  2. If the budget would be exceeded, the algorithm walks the LRU list from
//     the tail (least recently used) toward the head, evicting entries whose
//     refCnt is 0. Entries with refCnt > 0 are skipped because they are
//     actively borrowed via Handles -- evicting them would corrupt in-flight
//     delta resolution.
//
//  3. The walk continues until either enough space has been freed or no more
//     evictable entries remain. If the budget is still exceeded and no
//     evictable entries exist, ErrWindowFull is returned so the caller can
//     fall back to an uncached code path.
//
// Objects larger than the entire budget are rejected immediately with
// ErrObjectTooLarge to avoid evicting every other entry for a single object.
func (w *refCountedDeltaWindow) add(oid Hash, buf []byte, objType ObjectType) error {
	if len(buf) > w.budget {
		return ErrObjectTooLarge
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if entry, ok := w.index[oid]; ok {
		oldSize := entry.size

		entry.data = buf
		entry.size = len(buf)
		entry.typ = objType

		w.used += entry.size - oldSize

		w.lru.moveToFront(entry)
	} else {
		evictableCount := int(w.evictable.Load())
		if w.used+len(buf) > w.budget && evictableCount == 0 {
			return ErrWindowFull
		}

		entry := &refCountedEntry{
			oid:  oid,
			data: buf,
			size: len(buf),
			typ:  objType,
		}

		w.lru.pushFront(entry)
		w.index[oid] = entry

		w.used += entry.size
		w.evictable.Add(1)
	}

	evicted := 0
	for w.used > w.budget && w.evictable.Load() > 0 {
		for entry := w.lru.back(); entry != nil; {
			if entry.refCnt.Load() == 0 {
				prev := entry.prev
				w.lru.remove(entry)
				delete(w.index, entry.oid)

				w.used -= entry.size
				w.evictable.Add(-1)
				evicted++

				// Don't follow prev if it's the sentinel.
				if prev == w.lru.head {
					break
				}
				entry = prev

				if w.used <= w.budget {
					break
				}
			} else {
				prev := entry.prev
				if prev == w.lru.head {
					break
				}
				entry = prev
			}
		}

		if evicted == 0 {
			break
		}
		evicted = 0
	}

	if w.used > w.budget && w.evictable.Load() == 0 {
		return ErrWindowFull
	}

	return nil
}

// cachedObj represents a Git object stored in the cache along with its type.
type cachedObj struct {
	data []byte
	typ  ObjectType
}

// arcCache is a wrapper around arc.ARCCache that implements the ObjectCache
// interface.
type arcCache struct {
	arc *arc.ARCCache[Hash, cachedObj]
}

// NewARCCache creates a new ARC cache with the specified size and returns it
// as an ObjectCache.
func NewARCCache(size int) (ObjectCache, error) {
	arc, err := arc.NewARC[Hash, cachedObj](size)
	if err != nil {
		return nil, err
	}
	return &arcCache{arc: arc}, nil
}

// Get retrieves an object from the cache.
func (c *arcCache) Get(key Hash) (cachedObj, bool) { return c.arc.Get(key) }

// Add adds an object to the cache.
func (c *arcCache) Add(key Hash, value cachedObj) { c.arc.Add(key, value) }

// Purge clears the cache.
func (c *arcCache) Purge() { c.arc.Purge() }
