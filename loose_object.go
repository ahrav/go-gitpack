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
	hdr = hdr[:len(hdr)-1] // trim NUL terminator

	sp := bytes.IndexByte(hdr, ' ')
	if sp <= 0 || sp+1 >= len(hdr) {
		return nil, ObjBad, fmt.Errorf("invalid loose object header for %x", oid)
	}

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
	if uint64(len(body)) != size {
		return nil, ObjBad, fmt.Errorf(
			"loose object size mismatch for %x: want %d, got %d",
			oid, size, len(body),
		)
	}

	return body, typ, nil
}

func looseObjectPath(objectsDir string, oid Hash) string {
	h := oid.String()
	return filepath.Join(objectsDir, h[:2], h[2:])
}

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
