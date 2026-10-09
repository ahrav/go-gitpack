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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

// "<type> <size>\0" is at most 6 type bytes, a space, 20 decimal digits of a
// uint64 size, and the NUL, so a stream with no NUL in its first 32 bytes is
// malformed and is rejected before the body read.
const maxLooseHeaderLen = 32

// looseObjectStream is an open loose object positioned at the first body
// byte: the "<type> <size>\0" header has been consumed and parsed. body
// yields the inflated body; release returns the pooled readers and closes
// the file, and must be called exactly once.
type looseObjectStream struct {
	body    *bufio.Reader
	typ     ObjectType
	size    uint64
	release func()
}

// openLooseObject opens the loose object identified by oid, inflates and
// parses its header, and returns the stream positioned at the body. The body
// is left uninflated so callers can bound how much of it they read:
// readLooseObject reads it whole, readCommitHeader stops at the committer
// line, and readCommitPayloadTo checks size against its cap first.
//
// Pool discipline: both the zlib reader (getZlibReader / putZlibReader) and
// the bufio.Reader (getBR / putBR) come from sync.Pools and are recycled by
// release, or here on an error path.
//
// Thread safety: openLooseObject is safe for concurrent use because it
// operates only on local variables and pooled readers.
func (s *store) openLooseObject(oid Hash) (*looseObjectStream, error) {
	if s == nil || s.objectsDir == "" {
		return nil, objectNotFoundError(oid)
	}

	path := looseObjectPath(s.objectsDir, oid)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, objectNotFoundError(oid)
		}
		return nil, err
	}

	zr, err := getZlibReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	br := getBR(zr)
	release := func() {
		putBR(br)
		putZlibReader(zr)
		f.Close()
	}

	hdr, err := br.Peek(maxLooseHeaderLen)
	nul := bytes.IndexByte(hdr, 0)
	if nul < 0 {
		release()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("loose object header exceeds %d bytes for %x", maxLooseHeaderLen, oid)
	}
	// hdr aliases the bufio buffer: parse it before reading the body.
	hdr = hdr[:nul]
	if _, err := br.Discard(nul + 1); err != nil {
		release()
		return nil, err
	}

	sp := bytes.IndexByte(hdr, ' ')
	if sp <= 0 || sp+1 >= len(hdr) {
		release()
		return nil, fmt.Errorf("invalid loose object header for %x", oid)
	}

	// btostr safety: hdr is a local slice owned by this call frame and will
	// not be reused after the function returns, so the zero-copy string
	// conversion is safe for the lifetime of the parseLooseObjectType call.
	typ, ok := parseLooseObjectType(btostr(hdr[:sp]))
	if !ok {
		err := fmt.Errorf("unsupported loose object type %q for %x", btostr(hdr[:sp]), oid)
		release()
		return nil, err
	}

	size, err := strconv.ParseUint(btostr(hdr[sp+1:]), 10, 64)
	if err != nil {
		release()
		return nil, fmt.Errorf("invalid loose object size for %x: %w", oid, err)
	}

	return &looseObjectStream{body: br, typ: typ, size: size, release: release}, nil
}

// readLooseObject loads and decompresses the loose object identified by oid
// from the on-disk object store: openLooseObject parses the header, then the
// whole body is read and its length verified against the declared size.
//
// Thread safety: readLooseObject is safe for concurrent use; see
// openLooseObject.
func (s *store) readLooseObject(oid Hash) ([]byte, ObjectType, error) {
	obj, err := s.openLooseObject(oid)
	if err != nil {
		return nil, ObjBad, err
	}
	defer obj.release()

	body, err := io.ReadAll(obj.body)
	if err != nil {
		return nil, ObjBad, err
	}
	// Size verification: the decompressed body length must match the size
	// declared in the header. A mismatch indicates a truncated or corrupted
	// object file.
	if uint64(len(body)) != obj.size {
		return nil, ObjBad, fmt.Errorf(
			"loose object size mismatch for %x: want %d, got %d",
			oid, obj.size, len(body),
		)
	}

	// Hand back a slice whose capacity is its length. io.ReadAll grows by
	// append and commonly returns spare capacity, and callers treat an object
	// buffer as costing exactly the bytes it reports: the offset cache admits
	// on len(data), and pairCache.add aliases a whole-blob buffer instead of
	// copying it precisely because a blob's capacity is its size. Slack here
	// would be retained by both while being charged by neither.
	//
	// The read stays bounded by the bytes zlib actually produces rather than
	// by the size the header declares, so a corrupt or hostile header cannot
	// turn this into a large allocation; the copy happens only after the
	// declared size is confirmed against what was read.
	if cap(body) > len(body) {
		exact := make([]byte, len(body))
		copy(exact, body)
		body = exact
	}

	return body, obj.typ, nil
}

// openLooseCommit is openLooseObject for a commit: any other type fails
// with ErrObjectNotCommit.
func (s *store) openLooseCommit(oid Hash) (*looseObjectStream, error) {
	obj, err := s.openLooseObject(oid)
	if err != nil {
		return nil, err
	}
	if obj.typ != ObjCommit {
		obj.release()
		return nil, fmt.Errorf("%w: %x", ErrObjectNotCommit, oid)
	}
	return obj, nil
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
