// commit_attribution.go
//
// Efficient extraction and caching of Git commit author metadata and
// commit messages.
//
// Every secret finding needs to be attributed to a commit author (name,
// email, timestamp) and its commit message. Inflating and parsing the raw
// commit object each time is expensive, so this file provides metaCache --
// a concurrency-safe, read-through cache that stores attribution entries
// keyed by commit OID. When a commit-graph file is available, timestamps
// are served from the precomputed graph slice instead of re-parsing the
// header.
package objstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"sync"
	"time"
)

// AuthorInfo describes the Git author metadata attached to a secret
// finding. It is a lightweight, immutable value that callers use to
// display ownership information; it never alters repository content
// and is safe for concurrent read-only access.
type AuthorInfo struct {
	// Name holds the personal name of the commit author exactly as it
	// appears in the Git commit header.
	Name string

	// Email contains the author's e-mail address from the commit
	// header. The value is not validated or normalized.
	Email string

	// When records the author timestamp in Coordinated Universal Time.
	// Consumers should treat it as the authoritative time a change was
	// made, not when it was committed.
	When time.Time
}

// commitPayloadReader provides access to raw Git commit payload data
// (header lines plus message). It abstracts the storage and retrieval of
// commit objects to support different backing stores and caching strategies.
type commitPayloadReader interface {
	// readCommitPayload retrieves the full uncompressed bytes of a commit
	// object: header lines, the blank separator line, and the message.
	// The returned slice is a fresh allocation owned by the caller; it must
	// not alias pooled or shared cache buffers, because the caller retains
	// strings sliced from it. It returns an error if the object cannot be
	// found or is not a commit.
	readCommitPayload(oid Hash) ([]byte, error)
}

// payloadSink hands out destination memory for a commit payload. reserve
// returns a writable slice of exactly n bytes that the caller then fills; the
// sink decides where those bytes live.
type payloadSink interface {
	reserve(n int) []byte
}

// commitPayloadReaderTo is an optional extension of commitPayloadReader for
// stores that can decode a commit payload directly into sink-provided memory,
// saving the per-commit allocation of readCommitPayload. The returned slice
// is either sink memory filled by the store or a fresh allocation; either
// way the caller owns it.
type commitPayloadReaderTo interface {
	readCommitPayloadTo(oid Hash, sink payloadSink) ([]byte, error)
}

// metaEntry is one cached attribution record: the parsed author identity and
// the raw commit message.
//
// Lifetime: ai.Name, ai.Email, and msg are views into the payload the entry
// was parsed from, which lives in a metaCache slab (see reserve), so each
// entry retains one payload's worth of bytes.
// The cache is insert-only and unbounded — typical messages are ~200-700 B,
// so even 10k commits cost only a few MB, comparable to the header bytes the
// cache has always retained. Monorepo-scale histories (~1M+ commits) would
// push this toward a GB; the recorded escape hatches (don't-retain-oversize
// flag, budgeted cache) are deliberately not built at this scale.
type metaEntry struct {
	ai  AuthorInfo
	msg string
	// ts is the committer timestamp from the commit graph attached when the
	// entry was inserted, else the parsed author timestamp.
	ts int64
}

// metaCache provides efficient access to Git commit metadata by caching author
// information and commit messages. It coordinates with a commit graph for fast
// timestamp lookups and uses a reader interface to load raw commit data only
// when needed.
type metaCache struct {
	// graph holds the commit graph structure used for traversal and lookups.
	graph *commitGraphData

	// store provides access to raw commit payload data when cache misses occur.
	store commitPayloadReader

	// ts contains commit timestamps from the graph for quick access.
	// This is an alias to graph.Timestamps - no data is copied.
	ts []int64

	// mu guards concurrent access to the cache map.
	mu sync.RWMutex

	// m caches attribution entries by commit hash to avoid repeated
	// inflation and parsing of commit payloads.
	m map[Hash]metaEntry

	// slab is the bump allocator behind cached payloads: reserve carves
	// regions from it under slabMu and replaces a full slab rather than
	// growing it, so regions handed out earlier stay valid. Entries are
	// never evicted, so a slab lives exactly as long as its entries would.
	slabMu sync.Mutex
	slab   []byte
}

// metaSlabSize is the backing-array size of metaCache.slab. Payloads larger
// than this get their own allocation.
const metaSlabSize = 64 << 10

// newMetaCache constructs a metaCache with the given commit graph (may be nil)
// and commit payload reader. It is called once during NewHistoryScanner
// initialization. The initial map capacity (1024) is a heuristic that avoids
// early rehashing for typical repository sizes without over-allocating for
// very small repos.
func newMetaCache(g *commitGraphData, s commitPayloadReader) *metaCache {
	const cacheSize = 1024
	var ts []int64
	if g != nil {
		ts = g.Timestamps
	}

	return &metaCache{
		graph: g,
		store: s,
		ts:    ts,
		m:     make(map[Hash]metaEntry, cacheSize),
	}
}

// attachGraph replaces the current commit-graph reference. This is used when
// the scanner discovers a commit-graph file after initial construction, or
// when the graph is invalidated. Passing nil clears both the graph and the
// timestamp slice so subsequent lookups fall back to header parsing. Cached
// entries carry the timestamp resolved against the graph current at insert
// time, so the cache is emptied and entries are rebuilt on their next miss.
func (c *metaCache) attachGraph(g *commitGraphData) {
	c.mu.Lock()
	defer c.mu.Unlock()

	clear(c.m)
	if g == nil {
		c.graph = nil
		c.ts = nil
		return
	}
	c.graph = g
	c.ts = g.Timestamps
}

// get returns the CommitMetadata for the given OID, using the cache when
// possible and falling back to payload parsing on a miss.
//
// Concurrency protocol:
//  1. Acquire RLock, probe the map, release RLock. A hit is complete: the
//     entry carries author, message, and resolved timestamp.
//  2. On miss (see miss): read and parse the payload outside any lock, then
//     acquire the write Lock, resolve the timestamp against the attached
//     graph, insert, release.
//
// Two goroutines missing the same OID may both parse it; metaEntry is an
// immutable value and the second insert overwrites with an identical value.
// The hit path is kept to this function so its code stays small and
// independent of the miss path.
func (c *metaCache) get(oid Hash) (CommitMetadata, error) {
	c.mu.RLock()
	entry, ok := c.m[oid]
	c.mu.RUnlock()
	if !ok {
		var err error
		if entry, err = c.miss(oid); err != nil {
			return CommitMetadata{}, err
		}
	}
	if entry.ts == 0 {
		// An entry inserted without a resolved timestamp (or whose author
		// timestamp is the Unix epoch) resolves against the current graph.
		c.mu.RLock()
		entry.ts = c.timestampLocked(oid, entry.ai)
		c.mu.RUnlock()
	}
	return CommitMetadata{
		Author:    entry.ai,
		Timestamp: entry.ts,
		Message:   entry.msg,
	}, nil
}

// miss reads, parses, and caches the entry for a commit absent from the map.
func (c *metaCache) miss(oid Hash) (metaEntry, error) {
	var payload []byte
	var err error
	if to, ok := c.store.(commitPayloadReaderTo); ok {
		payload, err = to.readCommitPayloadTo(oid, c)
	} else {
		payload, err = c.store.readCommitPayload(oid)
	}
	if err != nil {
		return metaEntry{}, err
	}
	ai, msg, err := parseCommitPayload(payload)
	if err != nil {
		return metaEntry{}, err
	}
	entry := metaEntry{ai: ai, msg: btostr(msg)}

	c.mu.Lock()
	entry.ts = c.timestampLocked(oid, ai)
	c.m[oid] = entry
	c.mu.Unlock()
	return entry, nil
}

// reserve implements payloadSink: it returns the next n bytes of the slab,
// starting a new slab when the current one cannot hold them. The region is
// exclusively the caller's; a failed decode leaves it unused until the slab
// is released with its entries.
func (c *metaCache) reserve(n int) []byte {
	if n > metaSlabSize {
		return make([]byte, n)
	}
	c.slabMu.Lock()
	if n > cap(c.slab)-len(c.slab) {
		c.slab = make([]byte, 0, metaSlabSize)
	}
	start := len(c.slab)
	c.slab = c.slab[:start+n]
	region := c.slab[start : start+n : start+n]
	c.slabMu.Unlock()
	return region
}

// timestampLocked resolves the commit timestamp with c.mu held. The
// commit-graph value is preferred because it is authoritative for the
// committer date; ts == 0 (commit absent from the graph, or genuinely the
// Unix epoch, which is astronomically unlikely for real commits) falls back
// to the parsed author timestamp.
func (c *metaCache) timestampLocked(oid Hash, ai AuthorInfo) int64 {
	var ts int64
	if c.graph != nil {
		if idx, ok := c.graph.OIDToIndex[oid]; ok && idx < len(c.ts) {
			ts = c.ts[idx]
		}
	}
	if ts == 0 {
		ts = ai.When.Unix()
	}
	return ts
}

// splitCommitPayload splits a raw commit payload into its header half and
// message half at the first blank line (the "\n\n" separator defined by the
// commit object format). The message keeps its raw bytes — no encoding
// normalization, no NUL truncation, trailing newline preserved — because
// those are presentation behaviors of git-log, not object format.
//
// A payload without a separator (header-only commit) yields the full payload
// as header and a nil message. The returned slices alias payload.
func splitCommitPayload(payload []byte) (header, message []byte) {
	if i := indexBlankLine(payload); i >= 0 {
		return payload[:i], payload[i+2:]
	}
	return payload, nil
}

// indexBlankLine returns the index of the first "\n\n" in b, or -1.
//
// Signed commits carry a gpgsig block of ~64-byte lines, so a search that
// stops at every newline pays a call per line. This walk instead tests
// eight bytes per step. x holds the word XOR '\n', so a '\n' byte is a zero
// byte of x: the cheap has-zero test rejects most words outright, and only
// words containing a newline compute the exact per-byte mask m (0x80 in each
// zero byte) to look for two adjacent marks, either within the word or across
// the top byte of the previous word (carry) and the bottom byte of this one.
func indexBlankLine(b []byte) int {
	const (
		lf  = 0x0a0a0a0a0a0a0a0a
		lo7 = 0x7f7f7f7f7f7f7f7f
		lo1 = 0x0101010101010101
		hi  = 0x8080808080808080
	)
	carry := uint64(0)
	i := 0
	for ; i+8 <= len(b); i += 8 {
		x := binary.LittleEndian.Uint64(b[i:]) ^ lf
		if (x-lo1)&^x&hi == 0 {
			carry = 0
			continue
		}
		m := ^(((x & lo7) + lo7) | x | lo7)
		pair := m & (m<<8 | carry)
		if pair != 0 {
			// pair marks the second '\n' of the first adjacent pair.
			return i + bits.TrailingZeros64(pair)/8 - 1
		}
		carry = m >> 56
	}
	if carry != 0 && i < len(b) && b[i] == '\n' {
		return i - 1
	}
	for ; i+1 < len(b); i++ {
		if b[i] == '\n' && b[i+1] == '\n' {
			return i
		}
	}
	return -1
}

var (
	ErrAuthorLineNotFound  = errors.New("author line not found")
	ErrMalformedAuthorLine = errors.New("malformed author line: missing '>'")
	ErrMissingEmail        = errors.New("malformed author line: missing email")
	ErrMissingTimestamp    = errors.New("malformed author line: missing timestamp")
)

// parseAuthorHeader extracts the author's name, e-mail, and timestamp from
// an uncompressed Git commit header.
//
// The function scans header lines up to and including the first "committer "
// line, taking the last "author " line seen before it. If no author line
// exists it falls back to the committer line, because some tooling (e.g.
// filter-branch, BFG) can produce commits where the author line is stripped
// but the committer line survives.
//
// It does zero-allocation substring slicing wherever possible and returns a
// descriptive error when the header is missing or malformed.
//
// Lifetime note: the returned AuthorInfo.Name and AuthorInfo.Email are
// produced via btostr and therefore alias the backing array of hdr. If the
// caller's hdr slice is pooled or reused, the strings must be copied before
// the slice is returned to the pool.
func parseAuthorHeader(hdr []byte) (AuthorInfo, error) {
	line, _, _ := walkCommitHeader(hdr)
	if line == nil {
		return AuthorInfo{}, ErrAuthorLineNotFound
	}
	return parseAuthorLine(line)
}

// parseCommitPayload is splitCommitPayload followed by parseAuthorHeader on
// the header half, in one pass over the payload. The identity walk stops at
// the committer line or at the blank separator line, whichever comes first,
// so a message line starting with "author " at column 0 is never read as a
// header; the separator search then resumes after the committer line.
func parseCommitPayload(payload []byte) (AuthorInfo, []byte, error) {
	line, next, sep := walkCommitHeader(payload)
	if sep < 0 && next > 0 && next <= len(payload) {
		// next is the offset after the committer line's newline, so the
		// separator's first byte can be that newline itself.
		if j := indexBlankLine(payload[next-1:]); j >= 0 {
			sep = next - 1 + j
		}
	}
	var msg []byte
	if sep >= 0 {
		msg = payload[sep+2:]
	}
	if line == nil {
		return AuthorInfo{}, nil, ErrAuthorLineNotFound
	}
	ai, err := parseAuthorLine(line)
	if err != nil {
		return AuthorInfo{}, nil, err
	}
	return ai, msg, nil
}

// walkCommitHeader scans header lines and returns the identity line to
// parse (the last "author " line, else the first "committer " line, else
// nil) without its key. The committer line closes the identity section of
// the object format (gpgsig, mergetag, and encoding follow it, and their
// continuation lines are space-prefixed), so the walk ends there with next
// set to the offset after that line's newline. A blank line also ends the
// walk: sep is the offset of the "\n\n" separator, or -1 when the walk saw
// none. The pre-payload header reader truncated at the committer line too.
func walkCommitHeader(b []byte) (line []byte, next int, sep int) {
	var author []byte
	i := 0
	for i < len(b) {
		lineEnd := len(b)
		if nl := bytes.IndexByte(b[i:], '\n'); nl >= 0 {
			lineEnd = i + nl
		}
		if lineEnd == i && i > 0 {
			return author, i, i - 1
		}
		cur := b[i:lineEnd]
		if bytes.HasPrefix(cur, []byte("author ")) {
			author = cur[7:]
		} else if bytes.HasPrefix(cur, []byte("committer ")) {
			return pick(author, cur[10:]), lineEnd + 1, -1
		}
		i = lineEnd + 1
	}
	return author, i, -1
}

// pick prefers the author line and falls back to the committer line, because
// some tooling (e.g. filter-branch, BFG) strips the author line while the
// committer line survives.
func pick(author, committer []byte) []byte {
	if author != nil {
		return author
	}
	return committer
}

// parseAuthorLine parses the remainder of an identity line:
// "<name> <email> <timestamp> <tz>".
func parseAuthorLine(line []byte) (AuthorInfo, error) {

	// Locate the terminating '>' of the email address (scan from the end
	// because the author's name can contain '>').
	emailEnd := -1
	for i := len(line) - 1; i >= 0; i-- {
		if line[i] == '>' {
			emailEnd = i
			break
		}
	}
	if emailEnd < 0 {
		return AuthorInfo{}, ErrMalformedAuthorLine
	}

	// Find the opening '<' for the email address.
	emailStart := -1
	for i := emailEnd - 1; i >= 0; i-- {
		if line[i] == '<' {
			emailStart = i
			break
		}
	}
	if emailStart < 0 {
		return AuthorInfo{}, ErrMissingEmail
	}

	name := bytes.TrimSpace(line[:emailStart])
	email := line[emailStart+1 : emailEnd]

	// Skip spaces after '>' to reach the start of the timestamp.
	tsStart := emailEnd + 1
	for tsStart < len(line) && line[tsStart] == ' ' {
		tsStart++
	}
	if tsStart >= len(line) {
		return AuthorInfo{}, ErrMissingTimestamp
	}

	// Timestamp ends at first space or tab.
	tsEnd := tsStart
	for tsEnd < len(line) && line[tsEnd] != ' ' && line[tsEnd] != '\t' {
		tsEnd++
	}

	tsBytes := line[tsStart:tsEnd]
	sec, err := strconv.ParseInt(string(tsBytes), 10, 64)
	if err != nil {
		return AuthorInfo{}, fmt.Errorf("invalid timestamp: %w", err)
	}

	return AuthorInfo{
		Name:  btostr(name),
		Email: btostr(email),
		When:  time.Unix(sec, 0).UTC(),
	}, nil
}
