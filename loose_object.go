// loose_object.go
//
// Reading Git loose objects from the on-disk object store.
//
// A loose object is a single zlib-compressed file stored at
// <objects-dir>/<xx>/<yy...> where <xx> are the first two hex characters of
// the SHA-1 OID and <yy...> the remaining 38 characters. The decompressed
// content has the format:
//
//	<type> <size>\0<body>
//
// where <type> is one of "commit", "tree", "blob", or "tag", <size> is the
// decimal byte length of <body>, and \0 is a single NUL byte separating the
// header from the payload.
package objstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

// readLooseObject loads and decompresses the loose object identified by oid
// from the on-disk object store.
//
// How it works:
//  1. Constructs the filesystem path via looseObjectPath.
//  2. Opens the file, wraps it in a pooled zlib reader and a pooled
//     bufio.Reader.
//  3. Reads the NUL-terminated header to extract the object type and
//     declared size.
//  4. Reads the remaining body and verifies its length matches the declared
//     size.
//
// Pool discipline: both the zlib reader (getZlibReader / putZlibReader) and
// the bufio.Reader (getBR / putBR) are obtained from sync.Pools and returned
// via deferred calls, so they are recycled even on error paths.
//
// Thread safety: readLooseObject is safe for concurrent use because it
// operates only on local variables and pooled readers; no shared mutable
// state is accessed.
func (s *store) readLooseObject(oid Hash) ([]byte, ObjectType, error) {
	if s == nil || s.objectsDir == "" {
		return nil, ObjBad, objectNotFoundError(oid)
	}

	path := looseObjectPath(s.objectsDir, oid)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ObjBad, objectNotFoundError(oid)
		}
		return nil, ObjBad, err
	}
	defer f.Close()

	zr, err := getZlibReader(f)
	if err != nil {
		return nil, ObjBad, err
	}
	defer putZlibReader(zr)

	br := getBR(zr)
	defer putBR(br)

	hdr, err := br.ReadBytes(0)
	if err != nil {
		return nil, ObjBad, err
	}
	if len(hdr) < 3 {
		return nil, ObjBad, fmt.Errorf("invalid loose object header for %x", oid)
	}
	// Trim the trailing NUL byte that ReadBytes includes in its result.
	// After this, hdr contains "<type> <size>" with no terminator.
	hdr = hdr[:len(hdr)-1]

	sp := bytes.IndexByte(hdr, ' ')
	if sp <= 0 || sp+1 >= len(hdr) {
		return nil, ObjBad, fmt.Errorf("invalid loose object header for %x", oid)
	}

	// btostr safety: hdr is a local slice owned by this call frame and will
	// not be reused after the function returns, so the zero-copy string
	// conversion is safe for the lifetime of the parseLooseObjectType call.
	typ, ok := parseLooseObjectType(btostr(hdr[:sp]))
	if !ok {
		return nil, ObjBad, fmt.Errorf("unsupported loose object type %q for %x", btostr(hdr[:sp]), oid)
	}

	size, err := strconv.ParseUint(btostr(hdr[sp+1:]), 10, 64)
	if err != nil {
		return nil, ObjBad, fmt.Errorf("invalid loose object size for %x: %w", oid, err)
	}

	body, err := io.ReadAll(br)
	if err != nil {
		return nil, ObjBad, err
	}
	// Size verification: the decompressed body length must match the size
	// declared in the header. A mismatch indicates a truncated or corrupted
	// object file.
	if uint64(len(body)) != size {
		return nil, ObjBad, fmt.Errorf(
			"loose object size mismatch for %x: want %d, got %d",
			oid, size, len(body),
		)
	}

	return body, typ, nil
}

// looseObjectPath returns the filesystem path for a loose object given the
// base objects directory and the object's hash. The path follows the standard
// Git fan-out layout: <objectsDir>/<first-two-hex-chars>/<remaining-hex-chars>.
func looseObjectPath(objectsDir string, oid Hash) string {
	h := oid.String()
	return filepath.Join(objectsDir, h[:2], h[2:])
}

// parseLooseObjectType maps a Git object type string ("commit", "tree",
// "blob", "tag") to the corresponding ObjectType constant. It returns
// (ObjBad, false) for any unrecognized type string.
func parseLooseObjectType(typ string) (ObjectType, bool) {
	switch typ {
	case "commit":
		return ObjCommit, true
	case "tree":
		return ObjTree, true
	case "blob":
		return ObjBlob, true
	case "tag":
		return ObjTag, true
	default:
		return ObjBad, false
	}
}
