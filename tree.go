// tree.go – parse one Git tree object
package objstore

import (
	"bytes"
	"errors"
)

var (
	ErrCorruptTree  = errors.New("corrupt tree object")
	ErrTypeMismatch = errors.New("unexpected object type")
	ErrTreeNotFound = errors.New("tree object not found")
)

// treeEntry represents a single "<mode> <name>\0<sha1>" record inside a Git
// tree object.
//
// A value holds exactly one entry—file, directory, or special object, addressed
// by its SHA-1 object ID (OID).  The struct is internal to the objstore
// package yet its fields are exported so that higher-level helpers can inspect
// raw tree contents without additional allocations.  Callers must treat the
// value as immutable.
type treeEntry struct {
	// OID holds the raw 20-byte object ID that the tree entry points to.
	// The hash is stored in binary form, not hexadecimal.
	OID Hash

	// Name is the entry's path component exactly as it appears in the tree.
	// The string is expected to be valid UTF-8 but the Git format does not
	// enforce this.
	Name string

	// Mode encodes the Unix file mode in the canonical Git octal form
	// (e.g., 0100644 for a regular file, 040000 for a directory).
	Mode uint32
}

// If a tree has more than this many entries we also build a name→entry map.
const indexThreshold = 256

// Tree is an in-memory view of a Git tree object.
//
// A Tree provides read-only, name-based access to the entries of a single Git
// tree.  The object can be shared across multiple diff computations or other
// read-only operations without additional parsing.  Construct a Tree via
// parseTree; the zero value is not valid.
//
// All entries are kept in ascending‐name order, and callers must treat the
// returned slice or map as immutable.
type Tree struct {
	// sortedEntries contains every entry in strictly ascending name order.
	// The slice must remain unmodified after construction to preserve the
	// ordering invariant expected by lookup algorithms.
	sortedEntries []treeEntry

	// index accelerates name-to-entry look-ups for large trees by mapping
	// entry names to their indices in sortedEntries. It is nil when the tree
	// holds fewer than indexThreshold entries.
	index map[string]uint32
}

// parseTree decodes a raw Git tree object and returns an immutable *Tree.
//
// The input must contain a sequence of "<mode> <name>\0<sha1>" records exactly
// as stored in a Git tree object payload.  If the byte slice is malformed
// parseTree returns ErrCorruptTree.  A non-nil error from strconv.ParseUint is
// propagated unchanged.
//
// For trees larger than indexThreshold entries the function additionally builds
// an auxiliary map to provide O(1) name look-ups in subsequent operations; for
// smaller trees it falls back to linear scans to avoid the memory overhead.
func parseTree(raw []byte) (*Tree, error) {
	var (
		out  []treeEntry
		prev string // remember last name to enforce ordering and uniqueness
	)
	for len(raw) > 0 {
		// Extract the file mode up to the first space (ASCII 0x20).
		sp := bytes.IndexByte(raw, ' ')
		if sp < 0 {
			return nil, ErrCorruptTree
		}

		// Parse octal chmod value (3-6 digits) directly from raw[:sp].
		// Each byte must be '0'-'7', multiply by 8 and add digit value.
		var mode uint32
		for _, b := range raw[:sp] {
			if b < '0' || b > '7' {
				return nil, ErrCorruptTree
			}
			mode = mode<<3 | uint32(b-'0') // multiply by 8 and add digit
		}

		raw = raw[sp+1:]

		// Extract the entry name up to the NUL terminator.
		nul := bytes.IndexByte(raw, 0)
		if nul < 0 {
			return nil, ErrCorruptTree
		}
		name := string(raw[:nul])
		raw = raw[nul+1:]

		// Enforce strict ascending order - duplicates and out-of-order entries are invalid.
		if name <= prev {
			return nil, ErrCorruptTree
		}
		prev = name

		if len(raw) < 20 {
			return nil, ErrCorruptTree
		}
		var h Hash
		copy(h[:], raw[:20])
		raw = raw[20:]

		out = append(out, treeEntry{h, name, mode})
	}

	t := &Tree{sortedEntries: out}

	// Build the auxiliary index only for large trees to trade memory
	// for faster look-ups.
	if len(out) > indexThreshold {
		m := make(map[string]uint32, len(out))
		for i, e := range out {
			m[e.Name] = uint32(i)
		}
		t.index = m
	}
	return t, nil
}

// get returns the treeEntry with the given name and a boolean that reports
// whether it was found.
//
// The method performs an O(1) map lookup when the auxiliary index is present.
// For smaller trees, where no index is built, it gracefully falls back to a
// linear scan.  The function does not allocate and is therefore safe to call
// in tight loops.
func (t *Tree) get(name string) (treeEntry, bool) {
	if t.index != nil {
		i, ok := t.index[name]
		if ok {
			return t.sortedEntries[i], true
		}
		return treeEntry{}, false
	}
	for _, e := range t.sortedEntries {
		if e.Name == name {
			return e, true
		}
	}
	return treeEntry{}, false
}
