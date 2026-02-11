// commit_attribution.go
//
// Efficient extraction and caching of Git commit author metadata.
//
// Every secret finding needs to be attributed to a commit author (name, email,
// timestamp). Parsing the raw commit header each time is expensive, so this
// file provides metaCache -- a concurrency-safe, read-through cache that
// stores AuthorInfo keyed by commit OID. When a commit-graph file is
// available, timestamps are served from the precomputed graph slice instead
// of re-parsing the header.
package objstore

import (
	"bytes"
	"errors"
	"fmt"
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

// commitHeaderReader is an interface for reading commit headers
// commitHeaderReader provides access to raw Git commit header data.
// It abstracts the storage and retrieval of commit metadata
// to support different backing stores and caching strategies.
type commitHeaderReader interface {
	// readCommitHeader retrieves the raw header bytes for a commit.
	// It returns the uncompressed header data or an error if the commit cannot be found
	// or the header is malformed.
	readCommitHeader(oid Hash) ([]byte, error)
}

// metaCache provides efficient access to Git commit metadata by caching author information
// and timestamps. It coordinates with a commit graph for fast lookups and uses a
// reader interface to load raw commit data only when needed.
type metaCache struct {
	// graph holds the commit graph structure used for traversal and lookups.
	graph *commitGraphData

	// store provides access to raw commit header data when cache misses occur.
	store commitHeaderReader

	// ts contains commit timestamps from the graph for quick access.
	// This is an alias to graph.Timestamps - no data is copied.
	ts []int64

	// mu guards concurrent access to the cache map.
	mu sync.RWMutex

	// m caches AuthorInfo by commit hash to avoid repeated parsing of commit headers.
	m map[Hash]AuthorInfo
}

// newMetaCache constructs a metaCache with the given commit graph (may be nil)
// and commit header reader. It is called once during NewHistoryScanner
// initialization. The initial map capacity (1024) is a heuristic that avoids
// early rehashing for typical repository sizes without over-allocating for
// very small repos.
func newMetaCache(g *commitGraphData, s commitHeaderReader) *metaCache {
	const cacheSize = 1024
	var ts []int64
	if g != nil {
		ts = g.Timestamps
	}

	return &metaCache{
		graph: g,
		store: s,
		ts:    ts,
		m:     make(map[Hash]AuthorInfo, cacheSize),
	}
}

// attachGraph replaces the current commit-graph reference. This is used when
// the scanner discovers a commit-graph file after initial construction, or
// when the graph is invalidated. Passing nil clears both the graph and the
// timestamp slice so subsequent lookups fall back to header parsing.
func (c *metaCache) attachGraph(g *commitGraphData) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if g == nil {
		c.graph = nil
		c.ts = nil
		return
	}
	c.graph = g
	c.ts = g.Timestamps
}

// get returns the CommitMetadata for the given OID, using the cache when
// possible and falling back to header parsing on a miss.
//
// Concurrency protocol:
//  1. Acquire RLock, probe the map. (fast path -- no allocation)
//  2. On miss: release RLock, parse header (potentially expensive I/O),
//     acquire write Lock, insert into map, release Lock.
//  3. Read graph pointer and timestamp slice under RLock, then look up
//     the precomputed timestamp.
//
// This two-phase locking pattern means the same OID may be parsed twice if
// two goroutines miss concurrently, but that is safe because AuthorInfo is
// an immutable value type and the second write simply overwrites with an
// identical value.
func (c *metaCache) get(oid Hash) (CommitMetadata, error) {
	// Fast read-only path for author info.
	c.mu.RLock()
	ai, ok := c.m[oid]
	c.mu.RUnlock()

	if !ok {
		// Slow path -- inflate and parse the commit header once.
		hdr, err := c.store.readCommitHeader(oid)
		if err != nil {
			return CommitMetadata{}, err
		}
		ai, err = parseAuthorHeader(hdr)
		if err != nil {
			return CommitMetadata{}, err
		}

		// Promote to cache.
		c.mu.Lock()
		c.m[oid] = ai
		c.mu.Unlock()
	}

	// Prefer the commit-graph timestamp when available because it avoids
	// reparsing the header and is authoritative for the committer date.
	var ts int64
	c.mu.RLock()
	graph := c.graph
	tsSlice := c.ts
	c.mu.RUnlock()
	if graph != nil {
		if idx, ok := graph.OIDToIndex[oid]; ok && idx < len(tsSlice) {
			ts = tsSlice[idx]
		}
	}
	// Fallback: ts == 0 can mean the commit is not in the graph, or the
	// graph timestamp is genuinely the Unix epoch (1970-01-01T00:00:00Z).
	// The epoch case is astronomically unlikely for real commits, so we
	// treat 0 as "not available" and fall back to the parsed author
	// timestamp.
	if ts == 0 {
		ts = ai.When.Unix()
	}

	return CommitMetadata{
		Author:    ai,
		Timestamp: ts,
	}, nil
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
// The function scans the input line-by-line looking for the first "author "
// header. If no author line exists it falls back to the first "committer "
// line, because some tooling (e.g. filter-branch, BFG) can produce commits
// where the author line is stripped but the committer line survives.
//
// It does zero-allocation substring slicing wherever possible and returns a
// descriptive error when the header is missing or malformed.
//
// Lifetime note: the returned AuthorInfo.Name and AuthorInfo.Email are
// produced via btostr and therefore alias the backing array of hdr. If the
// caller's hdr slice is pooled or reused, the strings must be copied before
// the slice is returned to the pool.
func parseAuthorHeader(hdr []byte) (AuthorInfo, error) {
	authorStart := -1
	authorEnd := -1
	isAuthor := false

	i := 0
	for i < len(hdr) {
		// We are at the beginning of a line if i == 0
		// or the previous byte is a newline.
		if i == 0 || (i > 0 && hdr[i-1] == '\n') {
			lineStart := i

			// Find the end of the current line.
			lineEnd := i
			for lineEnd < len(hdr) && hdr[lineEnd] != '\n' {
				lineEnd++
			}

			line := hdr[lineStart:lineEnd]

			switch {
			case len(line) >= 7 && bytes.Equal(line[:7], []byte("author ")):
				authorStart = lineStart + 7
				authorEnd = lineEnd
				isAuthor = true
			case !isAuthor && len(line) >= 10 && bytes.Equal(line[:10], []byte("committer ")):
				// Fallback to "committer " only if no "author " was seen yet.
				authorStart = lineStart + 10
				authorEnd = lineEnd
				// Continue scanning – there might be a genuine author line later.
			}

			i = lineEnd + 1
		} else {
			i++
		}
	}

	if authorStart == -1 {
		return AuthorInfo{}, ErrAuthorLineNotFound
	}

	// Slice that contains everything after "author " / "committer " up to
	// the newline.  Format: "<name> <email> <timestamp> <tz>".
	line := hdr[authorStart:authorEnd]

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
