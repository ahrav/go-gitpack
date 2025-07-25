// tree_iter.go
//
// Zero‑allocation iterator for Git tree objects.
// Parses one entry at a time directly from the raw tree bytes so we do **not**
// materialize the whole tree in memory.

package objstore

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

var (
	ErrCorruptTree  = errors.New("corrupt tree object")
	ErrTypeMismatch = errors.New("unexpected object type")
	ErrTreeNotFound = errors.New("tree object not found")
)

// TreeIter provides a zero-allocation, forward-only iterator over the entries
// of a raw Git tree object.
//
// Callers create a TreeIter through Store.TreeIter or, internally, through
// treeCache.iter.  After creation, call Next repeatedly until it returns
// ok == false.  The iterator keeps a reference to the caller-supplied raw tree
// bytes and advances through that slice in place, so it never allocates or
// copies entry data except for the 20-byte object IDs it reports.  Each
// TreeIter instance must therefore remain confined to the goroutine that
// consumes it, and the underlying byte slice must stay immutable for the life
// of the iterator.
type TreeIter struct {
	// rest holds the unread portion of the raw tree object.
	// It is mutated by Next; external code must treat it as opaque.
	rest []byte
}

func newTreeIter(raw []byte) *TreeIter { return &TreeIter{rest: raw} }

// Next parses and returns the next entry in the raw Git tree.
//
// It yields the entry's file name, object ID, and file mode.
// When ok is false the iterator has been exhausted and, by convention,
// err is io.EOF.
// Any malformed input results in ok == false and a non-nil err,
// typically ErrCorruptTree.
//
// The iterator keeps a slice pointing at the original raw buffer;
// callers must therefore ensure that the underlying slice is not mutated
// while iteration is in progress.
// Next is not safe for concurrent use; each TreeIter instance must be
// confined to a single goroutine.
func (it *TreeIter) Next() (name string, oid Hash, mode uint32, ok bool, err error) {
	if len(it.rest) == 0 {
		return "", Hash{}, 0, false, io.EOF
	}

	// Safety check: if we have less than the minimum possible tree entry
	// (at least 1 char mode + space + 1 char name + null + 20 bytes SHA).
	if len(it.rest) < 24 {
		return "", Hash{}, 0, false, fmt.Errorf(
			"%w: insufficient data for tree entry (%d bytes)", ErrCorruptTree, len(it.rest),
		)
	}

	/* ---- <mode> (octal) -------------------------------------------- */
	// Scan up to the first space; everything before it must be an octal digit.
	sp := bytes.IndexByte(it.rest, ' ')
	if sp < 0 {
		return "", Hash{}, 0, false, fmt.Errorf(
			"%w: no space after mode (data: %s)", ErrCorruptTree, hex.EncodeToString(it.rest),
		)
	}
	for _, b := range it.rest[:sp] {
		if b < '0' || b > '7' {
			return "", Hash{}, 0, false, fmt.Errorf(
				"%w: invalid octal digit '%c' in mode (mode so far: %o)", ErrCorruptTree, b, mode,
			)
		}
		// Build the mode one octal digit at a time, avoiding strconv allocations.
		mode = mode<<3 | uint32(b-'0')
	}
	it.rest = it.rest[sp+1:]

	/* ---- <name>\0 --------------------------------------------------- */
	// The entry name is NUL-terminated.
	nul := bytes.IndexByte(it.rest, 0)
	if nul < 0 {
		return "", Hash{}, 0, false, fmt.Errorf(
			"%w: no null terminator after filename (data: %s)", ErrCorruptTree, hex.EncodeToString(it.rest),
		)
	}
	// Convert the slice header to string without copying bytes.
	//nolint:gosec // This is safe because we know nul is within the slice.
	name = btostr(it.rest[:nul])
	it.rest = it.rest[nul+1:]

	/* ---- <sha1> (20 bytes) ----------------------------------------- */
	if len(it.rest) < 20 {
		return "", Hash{}, 0, false, fmt.Errorf(
			"%w: insufficient bytes for SHA (%d < 20), name=%q, mode=%o", ErrCorruptTree, len(it.rest), name, mode,
		)
	}
	// Copy the 20-byte object ID into the Hash we return.
	copy(oid[:], it.rest[:20])
	it.rest = it.rest[20:]

	return name, oid, mode, true, nil
}
