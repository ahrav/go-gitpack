// diff_tree.go implements a streaming, memory-efficient tree-to-tree diff for
// Git object trees.
//
// The algorithm performs a merge-join over the sorted entries of two trees,
// emitting per-file change callbacks without materialising the full trees in
// memory. This design supports arbitrarily large repositories because only
// one tree level is traversed at a time.
//
// PRECONDITION: Git tree entries are stored in Git tree sort order, which is
// *not* plain lexicographic order. Directories are compared as if their name
// had a trailing '/' appended (e.g. "foo" < "foo-bar" < "foo.c" < "foo/"
// when "foo" is a tree). The TreeIter returned by store.treeIter MUST yield
// entries in this canonical order for the merge-join comparisons (oln < nln,
// oln == nln) to be correct. Violating this precondition will produce
// incorrect diffs silently.
package objstore

import (
	"errors"
	"fmt"
	"io"
)

// joinPath builds a Git-style forward-slash path by simple string
// concatenation. This is intentionally used instead of filepath.Join +
// filepath.ToSlash because:
//
//  1. filepath.Join allocates an intermediate slice via filepath.Clean and
//     then filepath.ToSlash allocates again for the slash conversion. In a
//     large repository diff this function is called millions of times, so the
//     double allocation is measurable.
//  2. Git tree entries already use forward slashes, so no OS-specific path
//     normalisation is needed.
func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	// Avoid double slash when prefix already ends with '/'.
	if prefix[len(prefix)-1] == '/' {
		return prefix + name
	}
	return prefix + "/" + name
}

// walkDiff streams the differences between two Git trees.
//
// walkDiff performs a *merge-like* traversal over the **sorted** directory
// entries of `oldTree` and `newTree`.
// Instead of materializing entire trees in memory, it obtains directory
// contents lazily through `TreeIter`, which keeps peak memory usage constant
// and enables diffing arbitrarily large repositories.
//
// For every *file* that has changed, `fn` is invoked exactly once.
// Directories are handled transparently: additions or deletions of a directory
// cause `walkDiff` to recurse so that the callback is still issued per file,
// never for the directory objects themselves.
//
// The callback receives
//
//   - the path relative to the walk root (always Unix-style slashes),
//   - the object ID in the old tree (zero if the file was just created),
//   - the object ID in the new tree (zero if the file was deleted), and
//   - the file mode that is recorded in the *new* tree (or the old mode if the
//     file vanished).
//
// Error semantics
//   - Any error returned by `TreeIter.Next` other than io.EOF is propagated
//     verbatim.
//   - An error returned by `fn` immediately aborts the traversal and is
//     forwarded to the caller.
//   - A `nil` error is returned when the diff completed successfully.
//
// Concurrency / side-effects: walkDiff itself is single-threaded and free of
// global state; callers may invoke it concurrently on separate Stores.
func walkDiff(
	tc *store,
	oldTreeOID, newTreeOID Hash,
	prefix string,
	fn func(path string, oldOID, newOID Hash, mode uint32) error,
) error {

	// Fast path: identical sub-tree ⇒ nothing to do.
	if oldTreeOID == newTreeOID {
		return nil
	}

	// Helper that turns a zero hash into a nil iterator to simplify the
	// merge-loop below.
	iterFor := func(h Hash) (*TreeIter, error) {
		if h.IsZero() {
			return nil, nil
		}
		iter, err := tc.treeIter(h)
		if err != nil {
			return nil, fmt.Errorf("failed to create tree iterator for %s: %w", h, err)
		}
		return iter, nil
	}

	oldIter, err := iterFor(oldTreeOID)
	if err != nil {
		return err
	}
	newIter, err := iterFor(newTreeOID)
	if err != nil {
		return err
	}
	defer putTreeIter(oldIter)
	defer putTreeIter(newIter)

	// State of the "current" entry of each iterator.
	var (
		oln, nln         string // names
		oidOld, oidNew   Hash
		modeOld, modeNew uint32
		okOld, okNew     bool
	)

	// nextOld / nextNew advance the respective iterators and normalize EOF to
	// ok* == false so the main loop can treat "exhausted" like "empty".
	nextOld := func() error {
		if oldIter == nil {
			okOld = false
			return nil
		}
		var err error
		oln, oidOld, modeOld, okOld, err = oldIter.Next()
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		return nil
	}
	nextNew := func() error {
		if newIter == nil {
			okNew = false
			return nil
		}
		var err error
		nln, oidNew, modeNew, okNew, err = newIter.Next()
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		return nil
	}

	// Prime the pump.
	if err := nextOld(); err != nil {
		return err
	}
	if err := nextNew(); err != nil {
		return err
	}

	for okOld || okNew {
		switch {
		case !okOld: // only additions remain
			if err := handleAdd(tc, prefix, nln, oidNew, modeNew, fn); err != nil {
				return err
			}
			if err := nextNew(); err != nil {
				return err
			}

		case !okNew: // only deletions remain
			if err := handleDel(tc, prefix, oln, oidOld, modeOld, fn); err != nil {
				return err
			}
			if err := nextOld(); err != nil {
				return err
			}

		case oln == nln: // possible modify / recurse / no-op
			switch {
			case oidOld == oidNew && modeOld == modeNew:
				// Identical entry → skip.
			case isTreeMode(modeOld) && isTreeMode(modeNew):
				// Directory exists on both sides → recurse.
				if err := walkDiff(
					tc,
					oidOld,
					oidNew,
					joinPath(prefix, nln),
					fn,
				); err != nil {
					return err
				}
			default:
				// Type transition or mode change. This covers cases such as:
				//   - A regular file replaced by a symlink (mode change).
				//   - A file replaced by a directory, or vice versa. When a
				//     file is replaced by a directory (or the reverse), the
				//     entry name is the same on both sides but the modes
				//     differ in their type bits. We report this as a single
				//     callback with both old and new OIDs rather than a
				//     delete + add pair, letting the caller decide how to
				//     interpret the transition.
				if err := fn(
					joinPath(prefix, nln),
					oidOld,
					oidNew,
					modeNew,
				); err != nil {
					return err
				}
			}
			if err := nextOld(); err != nil {
				return err
			}
			if err := nextNew(); err != nil {
				return err
			}

		case oln < nln: // deletion
			if err := handleDel(tc, prefix, oln, oidOld, modeOld, fn); err != nil {
				return err
			}
			if err := nextOld(); err != nil {
				return err
			}

		default: // nln < oln → addition
			if err := handleAdd(tc, prefix, nln, oidNew, modeNew, fn); err != nil {
				return err
			}
			if err := nextNew(); err != nil {
				return err
			}
		}
	}
	return nil
}

// handleAdd reports a newly added entry discovered by walkDiff.
//
// If the entry is a directory (mode & 040000 != 0) it recurses into the
// sub-tree so that the user callback is eventually invoked once per *file*.
// For regular files it calls fn immediately, passing Hash{} for the "old"
// object ID.
func handleAdd(
	tc *store,
	prefix, name string,
	oid Hash,
	mode uint32,
	fn func(path string, oldOID, newOID Hash, mode uint32) error,
) error {
	if isTreeMode(mode) { // directory
		return walkDiff(tc, Hash{}, oid, joinPath(prefix, name), fn)
	}
	return fn(joinPath(prefix, name), Hash{}, oid, mode)
}

// handleDel reports a deleted entry discovered by walkDiff.
//
// If the entry is a directory it recurses into the sub-tree so that fn is
// eventually called for each file that vanished.
// For regular files it calls fn immediately, passing Hash{} for the "new"
// object ID to signal that the file no longer exists.
func handleDel(
	tc *store,
	prefix, name string,
	oid Hash,
	mode uint32,
	fn func(path string, oldOID, newOID Hash, mode uint32) error,
) error {
	if isTreeMode(mode) { // directory
		return walkDiff(tc, oid, Hash{}, joinPath(prefix, name), fn)
	}
	return fn(joinPath(prefix, name), oid, Hash{}, mode)
}
