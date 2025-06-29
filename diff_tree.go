package objstore

// walkDiff walks the directory trees identified by parentOID and childOID
// and calls emit for every file that is new or has changed in the child tree.
//
// The algorithm performs a merge-walk over the 2 sorted entry lists that make
// up each tree object.  It behaves similarly to a 2-way diff: insertions and
// modifications are reported to the caller, while deletions are *ignored* (the
// caller can reconstruct them from the parent side if necessary).
//
// For entries that are sub-trees (mode bit 040000) walkDiff recurses so that
// the caller receives *file*-level changes, never tree objects themselves.
//
// The supplied treeCache tc is consulted for every tree OID that needs to be
// materialized.  This keeps repeated look-ups of the same subtree cheap.
//
// The emit callback receives
//   - path   – the full, slash-separated path of the entry relative to the
//     prefix that was passed to this invocation,
//   - old    – the Hash of the entry in the parent tree (zero value if the
//     entry did not previously exist),
//   - new    – the Hash of the entry in the child tree,
//   - mode   – the mode taken from the *child* side.
//
// If emit returns a non-nil error the traversal stops immediately and that
// error is propagated up-stack.
func walkDiff(
	tc *treeCache,
	parentOID, childOID Hash,
	prefix string,
	emit func(path string, old, new Hash, mode uint32) error,
) error {
	pt, err := tc.get(parentOID)
	if err != nil {
		return err
	}
	ct, err := tc.get(childOID)
	if err != nil {
		return err
	}

	// Merge-walk indices for parent and child tree entries.
	pIdx, cIdx := 0, 0
	pEntries, cEntries := pt.sortedEntries, ct.sortedEntries

	// Walk until both slices have been exhausted.
	for pIdx < len(pEntries) || cIdx < len(cEntries) {
		switch {
		case pIdx == len(pEntries):
			// All entries in parent are consumed – cEntries[cIdx] is an insertion.
			if err := walkEntry(tc, prefix, cEntries[cIdx], Hash{}, emit); err != nil {
				return err
			}
			cIdx++

		case cIdx == len(cEntries):
			// All entries in child are consumed – pEntries[pIdx] was deleted.
			// Deletions are intentionally ignored.
			pIdx++

		default:
			pEntry, cEntry := pEntries[pIdx], cEntries[cIdx]
			switch {
			case pEntry.Name == cEntry.Name:
				if pEntry.OID != cEntry.OID || pEntry.Mode != cEntry.Mode {
					// Entry exists on both sides but differs.
					if pEntry.Mode&040000 != 0 && cEntry.Mode&040000 != 0 {
						// Both sides are trees – recurse.
						if err := walkDiff(tc, pEntry.OID, cEntry.OID, prefix+pEntry.Name+"/", emit); err != nil {
							return err
						}
					} else {
						// Regular blob update.
						if err := emit(prefix+pEntry.Name, pEntry.OID, cEntry.OID, cEntry.Mode); err != nil {
							return err
						}
					}
				}
				// Advance both slices.
				pIdx, cIdx = pIdx+1, cIdx+1

			case pEntry.Name < cEntry.Name:
				// pEntries[pIdx] was deleted – skip.
				pIdx++

			default:
				// cEntries[cIdx] was inserted.
				if err := walkEntry(tc, prefix, cEntry, Hash{}, emit); err != nil {
					return err
				}
				cIdx++
			}
		}
	}
	return nil
}

// walkEntry resolves a single tree entry.
// If the entry is itself a tree it delegates to walkDiff so that callers
// still receive changes on a per-file basis; otherwise it forwards the entry
// directly to emit.
func walkEntry(
	tc *treeCache,
	prefix string,
	e treeEntry,
	old Hash,
	emit func(path string, old, new Hash, mode uint32) error,
) error {
	if e.Mode&040000 != 0 {
		// Descend into the subtree with an empty parent (no counterpart).
		return walkDiff(tc, Hash{}, e.OID, prefix+e.Name+"/", emit)
	}
	return emit(prefix+e.Name, old, e.OID, e.Mode)
}
