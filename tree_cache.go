package objstore

import "sync"

// treeCache caches parsed Tree objects keyed by their object ID (Hash).
// The cache is shared by a Store instance and guarantees that each tree
// object is parsed at most once during the Store's lifetime.  Callers
// should use (*treeCache).get to obtain a *Tree; the method is safe for
// concurrent use and returns the same *Tree instance for identical hashes.
type treeCache struct {
	// store provides the raw object data used to populate the cache.
	store *Store

	// mu serializes access to mem. Read-only operations use RLock/RUnlock;
	// writes take the full Lock to preserve consistency.
	mu sync.RWMutex

	// mem holds already-parsed trees. A nil entry is never stored—the
	// presence of a key implies the *Tree has been fully parsed.
	mem map[Hash]*Tree
}

func newTreeCache(s *Store) *treeCache {
	return &treeCache{store: s, mem: make(map[Hash]*Tree)}
}

func (c *treeCache) get(oid Hash) (*Tree, error) {
	if oid == (Hash{}) { // empty tree for root diffs
		return &Tree{}, nil
	}

	// Fast path: check cache with read lock.
	c.mu.RLock()
	if t, ok := c.mem[oid]; ok {
		c.mu.RUnlock()
		return t, nil
	}
	c.mu.RUnlock()

	// Slow path: need to fetch and parse.
	// Take write lock immediately to prevent race.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check: another goroutine might have loaded it.
	if t, ok := c.mem[oid]; ok {
		return t, nil
	}

	if c.store == nil {
		return nil, ErrTreeNotFound
	}

	// Now we're the only one who will load this object.
	raw, typ, err := c.store.Get(oid)
	if err != nil {
		return nil, err
	}
	if typ != ObjTree {
		return nil, ErrTypeMismatch
	}

	t, err := parseTree(raw)
	if err != nil {
		return nil, err
	}

	c.mem[oid] = t
	return t, nil
}
