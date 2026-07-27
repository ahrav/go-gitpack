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
// entries in this canonical order, which is the order compareTreeEntryNames
// implements and the merge-join advances its two cursors by. Violating this
// precondition will produce incorrect diffs silently.
package objstore

import (
	"errors"
	"fmt"
	"io"
	"strings"
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

// compareTreeEntryNames orders two tree entries the way Git orders the entries
// of a tree, which is the order they are stored in and therefore the order
// TreeIter yields them. It returns a negative number, zero, or a positive
// number as the first entry sorts before, with, or after the second.
//
// Git's base_name_compare treats a tree's name as if the separator that would
// follow the directory were part of it: the byte just past the shorter name is
// '/' for a tree and absent (0) for every other entry type. Because
// '.' (0x2E) < '/' (0x2F) < '0' (0x30), a sibling blob "pkg.go" sorts before
// the tree "pkg" while "pkg0" sorts after it, and a blob and a tree that share
// a name do not compare equal. Comparing the names as plain strings puts "pkg"
// first in both cases, which steps the merge-join's two cursors out of order
// and leaves same-name entries unpaired.
//
// The ordering is total over valid Git entry names, which carry neither '/' nor
// NUL. A name that does carry '/' collides with the tree whose name it extends,
// because both sides then compare '/' at that offset: compareTreeEntryNames
// answers 0 for the blob "pkg/x" against the tree "pkg", and the relation is a
// preorder rather than an order — "pkg/y" and "pkg/x" both compare equal to
// "pkg" while "pkg/y" sorts after "pkg/x". Only a corrupt tree reaches that,
// since TreeIter parses names without rejecting '/', and base_name_compare
// answers the same way, so such a diff still matches what git shows for the
// same objects; breaking the tie here instead would make this walk disagree
// with git on input git itself reads. The cost is bounded: the merge-join pairs
// the colliding entries and enumerates each side whole, so a '/'-bearing name
// shadowing a 1024-file subtree reports 2049 changes for one real addition —
// the amplification an ordinary directory deletion already pays, with no
// recursion across the two sides and no unbounded walk.
func compareTreeEntryNames(name1 string, mode1 uint32, name2 string, mode2 uint32) int {
	if len(name1) == len(name2) {
		// Neither name reaches past the other, so the two implied bytes are
		// what meet and no name byte is read: the names decide unless they are
		// identical. This is where a merge-join spends most of its
		// comparisons, because most of a tree's entries pair with the other
		// side's.
		if c := strings.Compare(name1, name2); c != 0 {
			return c
		}
		return int(impliedNameByte(mode1)) - int(impliedNameByte(mode2))
	}

	shared := min(len(name1), len(name2))
	if c := strings.Compare(name1[:shared], name2[:shared]); c != 0 {
		return c
	}
	c1, c2 := entryNameByteAt(name1, mode1, shared), entryNameByteAt(name2, mode2, shared)
	switch {
	case c1 < c2:
		return -1
	case c1 > c2:
		return 1
	default:
		return 0
	}
}

// entryNameByteAt returns the byte Git compares at offset i of a tree entry
// whose name is at most i bytes long: the name's own byte when it extends that
// far, otherwise the byte the entry implies past its name.
func entryNameByteAt(name string, mode uint32, i int) byte {
	if len(name) > i {
		return name[i]
	}
	return impliedNameByte(mode)
}

// impliedNameByte returns the byte Git compares just past an entry's name: the
// separator that would follow a directory, and none for every other type.
func impliedNameByte(mode uint32) byte {
	if isTreeMode(mode) {
		return '/'
	}
	return 0
}

// walkDiff streams the differences between two Git trees.
//
// walkDiff performs a *merge-like* traversal over the **sorted** directory
// entries of `oldTree` and `newTree`.
// Instead of materializing entire trees in memory, it obtains directory
// contents lazily through `TreeIter`, which keeps peak memory usage constant
// and enables diffing arbitrarily large repositories.
//
// The two cursors advance by compareTreeEntryNames, the order the entries are
// stored in, so the sides of a valid name that exists in both trees always meet
// on the same iteration and are compared as one pair.
//
// For every changed file, `fn` is invoked once — an addition, a deletion, or a
// same-type modification is one event carrying whichever OIDs exist, and a
// permission-only change stays a single event. One case splits a file across
// two events: a path whose entry type changes in place. Nothing is carried
// across a type change, so that yields a deletion of the old entry and an
// addition of the new one. Git renders such a change the same way in patch
// form, and `git diff-tree -r` splits a file/directory conversion into a
// deletion and an addition of its own; its raw spelling of a *same-path* type
// change is instead one `T` entry carrying both OIDs, which one callback here
// cannot express because it would have to carry two modes.
//
// The order of that pair follows the entry order the merge-join walks. A
// transition between two non-tree types is one name on both sides, so its
// deletion precedes its addition. A transition involving a tree is two names —
// a tree sorts as if its name ended in '/' — so the non-tree side, which sorts
// first, is reported before the files of the tree side.
//
// Directories are handled transparently: additions or deletions of a directory
// cause `walkDiff` to recurse so that the callback is still issued per file,
// never for the directory objects themselves.
//
// The callback receives
//
//   - the path relative to the walk root (always Unix-style slashes),
//   - the object ID in the old tree (zero if the file was just created),
//   - the object ID in the new tree (zero if the file was deleted), and
//   - the mode of the side the event describes: the new mode for an addition
//     or a modification, the old mode for a deletion. The two modes behind a
//     single modification event share a type nibble, so that one mode
//     identifies the entry's type on both sides. A file split across two
//     events carries each side's own mode, and those two may differ in type.
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

		// Both cursors hold an entry, which is the only state in which their
		// order is defined; cmp is scoped to this arm so it cannot be read
		// against an exhausted cursor's leftover name and mode.
		default:
			switch cmp := compareTreeEntryNames(oln, modeOld, nln, modeNew); {
			case cmp == 0: // possible modify / recurse / no-op
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
				case modeOld&modeTypeMask != modeNew&modeTypeMask:
					// The name survives but its entry type does not: a blob, a
					// symlink, and a gitlink can each stand where one of the
					// others stood. For valid names those are the only three
					// types here, because a tree sorts as if its name ended in
					// '/' and so does not compare equal to a non-tree entry; a
					// corrupt name carrying '/' collides with the tree it
					// extends and does reach this arm, which stays correct
					// because handleDel and handleAdd each recurse on a tree
					// mode and each side keeps its own name.
					//
					// Nothing is carried between the two sides: the old object
					// leaves the tree and a new one arrives, so the transition
					// is reported as a deletion of the old entry followed by
					// an addition of the new one, which is how git renders it
					// in patch form. One merged event cannot express it,
					// because it has room for only one mode.
					if err := handleDel(tc, prefix, oln, oidOld, modeOld, fn); err != nil {
						return err
					}
					if err := handleAdd(tc, prefix, nln, oidNew, modeNew, fn); err != nil {
						return err
					}
				default:
					// Same entry type on both sides: the object ID and/or the
					// permission bits changed. One event describes the whole
					// change, and the new mode's type nibble equals the old
					// one's.
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

			case cmp < 0: // deletion
				// The old entry sorts first, so no entry on the new side
				// shares its place in the order and it is gone from the tree.
				// handleDel recurses into a tree, which keeps the callback
				// per-file and is also the route the departing half of a
				// tree/non-tree conversion takes: a tree and a non-tree entry
				// of one valid name sort as two distinct names and never meet
				// as a pair.
				if err := handleDel(tc, prefix, oln, oidOld, modeOld, fn); err != nil {
					return err
				}
				if err := nextOld(); err != nil {
					return err
				}

			default: // the new entry sorts first → addition
				// The mirror image: handleAdd recurses into an arriving tree,
				// so a directory that takes a name over reports the files it
				// brings rather than one tree-mode event every blob-filtering
				// caller would discard.
				if err := handleAdd(tc, prefix, nln, oidNew, modeNew, fn); err != nil {
					return err
				}
				if err := nextNew(); err != nil {
					return err
				}
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
