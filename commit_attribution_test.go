// commit_attribution_test.go tests the commit attribution subsystem, including
// the metaCache (which maps commit OIDs to author metadata and timestamps),
// the parseAuthorHeader parser, and concurrent-access safety of the cache.

package objstore

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockCommitPayloadReader is an in-memory implementation of commitPayloadReader
// used by tests to supply pre-configured commit payloads keyed by OID.
// It is safe for concurrent use via an internal RWMutex.
type mockCommitPayloadReader struct {
	payloads map[Hash][]byte
	mu       sync.RWMutex
}

func newMockCommitPayloadReader() *mockCommitPayloadReader {
	return &mockCommitPayloadReader{
		payloads: make(map[Hash][]byte),
	}
}

func (m *mockCommitPayloadReader) readCommitPayload(oid Hash) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if payload, ok := m.payloads[oid]; ok {
		return payload, nil
	}
	return nil, fmt.Errorf("object %x not found", oid)
}

// mockCommitMessage is the message body addCommit embeds in every generated
// payload, so tests can assert Message extraction against a known value.
const mockCommitMessage = "Test commit message\n"

func (m *mockCommitPayloadReader) addCommit(oid Hash, author, email string, timestamp int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	payload := fmt.Sprintf(
		"tree 1234567890abcdef1234567890abcdef12345678\n"+
			"author %s <%s> %d +0000\n"+
			"committer %s <%s> %d +0000\n\n"+
			mockCommitMessage,
		author, email, timestamp,
		author, email, timestamp,
	)
	m.payloads[oid] = []byte(payload)
}

func (m *mockCommitPayloadReader) addRawPayload(oid Hash, payload []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.payloads[oid] = payload
}

// TestNewMetaCache validates that newMetaCache correctly initializes a metaCache
// with the provided commit graph data and reader, and starts with an empty map.
func TestNewMetaCache(t *testing.T) {
	graph := &commitGraphData{
		Timestamps: []int64{1000, 2000, 3000, 4000, 5000},
		OIDToIndex: map[Hash]int{
			{1}: 0,
			{2}: 1,
			{3}: 2,
			{4}: 3,
			{5}: 4,
		},
	}

	mockReader := newMockCommitPayloadReader()
	cache := newMetaCache(graph, mockReader)

	assert.NotNil(t, cache)
	assert.Equal(t, graph, cache.graph)
	assert.Equal(t, commitPayloadReader(mockReader), cache.store)
	assert.Equal(t, graph.Timestamps, cache.ts)
	assert.NotNil(t, cache.m)
	assert.Equal(t, 0, len(cache.m))
}

// TestMetaCacheTimestamp validates that metaCache.get returns the correct
// timestamp for known OIDs, returns errors for unknown OIDs, and handles
// out-of-bounds index conditions gracefully.
func TestMetaCacheTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		oid       Hash
		wantTS    int64
		wantFound bool
		setup     func(*commitGraphData)
	}{
		{
			name:      "existing first element",
			oid:       Hash{1},
			wantTS:    1000,
			wantFound: true,
		},
		{
			name:      "existing middle element",
			oid:       Hash{3},
			wantTS:    3000,
			wantFound: true,
		},
		{
			name:      "existing last element",
			oid:       Hash{5},
			wantTS:    5000,
			wantFound: true,
		},
		{
			name:      "non-existing oid",
			oid:       Hash{99},
			wantTS:    0,
			wantFound: false,
		},
		{
			name:      "index out of bounds",
			oid:       Hash{10},
			wantTS:    0,
			wantFound: false,
			setup: func(g *commitGraphData) {
				g.OIDToIndex[Hash{10}] = 100 // Beyond ts length
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph := &commitGraphData{
				Timestamps: []int64{1000, 2000, 3000, 4000, 5000},
				OIDToIndex: map[Hash]int{
					{1}: 0,
					{2}: 1,
					{3}: 2,
					{4}: 3,
					{5}: 4,
				},
			}

			if tt.setup != nil {
				tt.setup(graph)
			}

			mockReader := newMockCommitPayloadReader()
			// Add commit data for all test OIDs that should be found
			if tt.wantFound {
				mockReader.addCommit(tt.oid, "Test Author", "test@example.com", tt.wantTS)
			}

			cache := newMetaCache(graph, mockReader)

			metadata, err := cache.get(tt.oid)
			if tt.wantFound {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantTS, metadata.Timestamp)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// TestParseAuthorHeader exercises the parseAuthorHeader function against a wide
// range of inputs including standard headers, edge cases (empty names, special
// characters, missing fields), and error conditions (malformed timestamps, missing
// author/committer lines).
func TestParseAuthorHeader(t *testing.T) {
	tests := []struct {
		name    string
		header  []byte
		want    AuthorInfo
		wantErr bool
	}{
		{
			name: "standard author line",
			header: []byte("tree abc123\n" +
				"author John Doe <john@example.com> 1234567890 +0000\n" +
				"committer Jane Doe <jane@example.com> 1234567890 +0000\n"),
			want: AuthorInfo{
				Name:  "John Doe",
				Email: "john@example.com",
				When:  time.Unix(1234567890, 0).UTC(),
			},
		},
		{
			name:   "author with extra spaces",
			header: []byte("author   John   Doe   <john@example.com>   1234567890   +0000\n"),
			want: AuthorInfo{
				Name:  "John   Doe",
				Email: "john@example.com",
				When:  time.Unix(1234567890, 0).UTC(),
			},
		},
		{
			name: "fallback to committer when no author",
			header: []byte("tree abc123\n" +
				"committer Bob Smith <bob@example.com> 9876543210 +0000\n"),
			want: AuthorInfo{
				Name:  "Bob Smith",
				Email: "bob@example.com",
				When:  time.Unix(9876543210, 0).UTC(),
			},
		},
		{
			name: "prefer author over committer",
			header: []byte("author Alice <alice@example.com> 1500000000 +0000\n" +
				"committer Bob <bob@example.com> 1600000000 +0000\n"),
			want: AuthorInfo{
				Name:  "Alice",
				Email: "alice@example.com",
				When:  time.Unix(1500000000, 0).UTC(),
			},
		},
		{
			name:   "empty name",
			header: []byte("author  <noname@example.com> 1234567890 +0000"),
			want: AuthorInfo{
				Name:  "",
				Email: "noname@example.com",
				When:  time.Unix(1234567890, 0).UTC(),
			},
		},
		{
			name:   "complex name with special chars",
			header: []byte("author O'Neil, Jr. (The 3rd) <oneil@example.com> 1234567890 +0000"),
			want: AuthorInfo{
				Name:  "O'Neil, Jr. (The 3rd)",
				Email: "oneil@example.com",
				When:  time.Unix(1234567890, 0).UTC(),
			},
		},
		{
			name:   "name with < character",
			header: []byte("author Test <user> Name <test@example.com> 1234567890 +0000"),
			want: AuthorInfo{
				Name:  "Test <user> Name",
				Email: "test@example.com",
				When:  time.Unix(1234567890, 0).UTC(),
			},
		},
		{
			name:    "missing closing bracket",
			header:  []byte("author John Doe <john@example.com 1234567890 +0000"),
			wantErr: true,
		},
		{
			name:    "missing opening bracket",
			header:  []byte("author John Doe john@example.com> 1234567890 +0000"),
			wantErr: true,
		},
		{
			name:    "missing timestamp",
			header:  []byte("author John Doe <john@example.com>"),
			wantErr: true,
		},
		{
			name:    "no author or committer",
			header:  []byte("tree abc123\nparent def456\n\nCommit message"),
			wantErr: true,
		},
		{
			name:    "empty header",
			header:  []byte(""),
			wantErr: true,
		},
		{
			name:    "only newlines",
			header:  []byte("\n\n\n"),
			wantErr: true,
		},
		{
			name:    "invalid timestamp format",
			header:  []byte("author John Doe <john@example.com> notanumber +0000"),
			wantErr: true,
		},
		{
			name:   "negative timestamp",
			header: []byte("author John Doe <john@example.com> -1234567890 +0000"),
			want: AuthorInfo{
				Name:  "John Doe",
				Email: "john@example.com",
				When:  time.Unix(-1234567890, 0).UTC(),
			},
		},
		{
			name:   "timestamp with timezone",
			header: []byte("author John Doe <john@example.com> 1234567890 -0800"),
			want: AuthorInfo{
				Name:  "John Doe",
				Email: "john@example.com",
				When:  time.Unix(1234567890, 0).UTC(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAuthorHeader(tt.header)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// TestMetaCacheAuthor validates successful retrieval, caching behaviour, and
// error paths (missing object, malformed header) of metaCache.get for author info.
func TestMetaCacheAuthor(t *testing.T) {
	mockReader := newMockCommitPayloadReader()

	graph := &commitGraphData{
		Timestamps: []int64{1000, 2000, 3000},
		OIDToIndex: map[Hash]int{
			{1, 2, 3}: 0,
			{4, 5, 6}: 1,
		},
	}

	metaCache := newMetaCache(graph, mockReader)

	t.Run("successful retrieval and caching", func(t *testing.T) {
		oid := Hash{1, 2, 3}
		mockReader.addCommit(oid, "John Doe", "john@example.com", 1234567890)

		metadata1, err := metaCache.get(oid)
		assert.NoError(t, err)
		assert.Equal(t, "John Doe", metadata1.Author.Name)
		assert.Equal(t, "john@example.com", metadata1.Author.Email)
		assert.Equal(t, time.Unix(1234567890, 0).UTC(), metadata1.Author.When)
		assert.Equal(t, mockCommitMessage, metadata1.Message)

		// Check caching
		metaCache.mu.RLock()
		_, cached := metaCache.m[oid]
		metaCache.mu.RUnlock()
		assert.True(t, cached)

		metadata2, err := metaCache.get(oid)
		assert.NoError(t, err)
		assert.Equal(t, metadata1.Author, metadata2.Author)
		assert.Equal(t, metadata1.Message, metadata2.Message)
	})

	t.Run("object not found", func(t *testing.T) {
		oid := Hash{99, 99, 99}
		_, err := metaCache.get(oid)
		assert.Error(t, err)
	})

	t.Run("malformed header", func(t *testing.T) {
		oid := Hash{4, 5, 6}
		mockReader.addRawPayload(oid, []byte("invalid header"))

		_, err := metaCache.get(oid)
		assert.Error(t, err)
	})
}

// TestMetaCacheConcurrentAccess verifies that metaCache is safe for concurrent
// reads from multiple goroutines. It spawns 20 goroutines each performing 100
// reads across 50 distinct OIDs and asserts no data races or incorrect results.
func TestMetaCacheConcurrentAccess(t *testing.T) {
	mockReader := newMockCommitPayloadReader()

	graph := &commitGraphData{
		Timestamps: make([]int64, 50),
		OIDToIndex: make(map[Hash]int),
	}

	// Setup graph data
	for i := range 50 {
		oid := Hash{byte(i)}
		graph.OIDToIndex[oid] = i
		graph.Timestamps[i] = int64(1500000000 + i)
	}

	metaCache := newMetaCache(graph, mockReader)

	numObjects := 50
	for i := range numObjects {
		oid := Hash{byte(i)}
		mockReader.addCommit(oid, fmt.Sprintf("User%d", i), fmt.Sprintf("user%d@example.com", i), int64(1500000000+i))
	}

	var wg sync.WaitGroup
	numGoroutines := 20
	numReadsPerGoroutine := 100
	errors := make(chan error, numGoroutines*numReadsPerGoroutine)

	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutine int) {
			defer wg.Done()

			for i := range numReadsPerGoroutine {
				oid := Hash{byte(i % numObjects)}
				metadata, err := metaCache.get(oid)

				if err != nil {
					errors <- fmt.Errorf("goroutine %d: %w", goroutine, err)
					continue
				}

				expectedName := fmt.Sprintf("User%d", oid[0])
				if metadata.Author.Name != expectedName {
					errors <- fmt.Errorf("goroutine %d: expected name %s, got %s",
						goroutine, expectedName, metadata.Author.Name)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for err := range errors {
		t.Error(err)
		errorCount++
		if errorCount > 10 {
			t.Fatal("Too many errors, stopping")
		}
	}
}

// TODO: TestMetaCacheEdgeCases is disabled because it depends on synctest.Run
// (from the Go sync/synctest experiment) which is not yet available in the
// standard library. Re-enable once synctest graduates or replace with a
// compatible concurrency-testing approach.
//
// func TestMetaCacheEdgeCases(t *testing.T) { ... }

// TestSplitCommitPayload pins the header/message split rule: the payload
// divides at the first "\n\n", the message keeps its raw bytes (encoding,
// NULs, trailing newlines), and a payload without a separator yields an
// empty message. The gpgsig and mergetag cases matter because their
// multi-line values embed blank-looking lines that are actually
// space-prefixed continuations — the first true "\n\n" is still the
// header/message boundary.
func TestSplitCommitPayload(t *testing.T) {
	const (
		treeLine      = "tree 1234567890abcdef1234567890abcdef12345678\n"
		authorLine    = "author John Doe <john@example.com> 1500000000 +0000\n"
		committerLine = "committer Jane Doe <jane@example.com> 1600000000 +0000\n"
	)
	baseHeader := treeLine + authorLine + committerLine

	tests := []struct {
		name        string
		payload     string
		wantHeader  string
		wantMessage string
	}{
		{
			name:        "plain single-line message",
			payload:     baseHeader + "\n" + "Fix the bug\n",
			wantHeader:  treeLine + authorLine + strings.TrimSuffix(committerLine, "\n"),
			wantMessage: "Fix the bug\n",
		},
		{
			name:        "multiline message with internal blank line",
			payload:     baseHeader + "\n" + "Title\n\nBody paragraph.\n",
			wantHeader:  treeLine + authorLine + strings.TrimSuffix(committerLine, "\n"),
			wantMessage: "Title\n\nBody paragraph.\n",
		},
		{
			name: "gpgsig-signed commit",
			payload: baseHeader +
				"gpgsig -----BEGIN PGP SIGNATURE-----\n" +
				" \n" + // continuation of the sig's blank line: space-prefixed, NOT a separator
				" iQEzBAABCAAdFiEEfakefakefakefakefake\n" +
				" -----END PGP SIGNATURE-----\n" +
				"\n" +
				"Signed message\n",
			wantHeader: baseHeader +
				"gpgsig -----BEGIN PGP SIGNATURE-----\n" +
				" \n" +
				" iQEzBAABCAAdFiEEfakefakefakefakefake\n" +
				" -----END PGP SIGNATURE-----",
			wantMessage: "Signed message\n",
		},
		{
			name: "mergetag commit",
			payload: treeLine +
				"parent 1111111111111111111111111111111111111111\n" +
				"parent 2222222222222222222222222222222222222222\n" +
				authorLine + committerLine +
				"mergetag object 2222222222222222222222222222222222222222\n" +
				" type commit\n" +
				" tag v1.0\n" +
				" tagger Tag Ger <tag@example.com> 1400000000 +0000\n" +
				" \n" +
				" Tag annotation body\n" +
				"\n" +
				"Merge tag 'v1.0'\n",
			wantHeader: treeLine +
				"parent 1111111111111111111111111111111111111111\n" +
				"parent 2222222222222222222222222222222222222222\n" +
				authorLine + committerLine +
				"mergetag object 2222222222222222222222222222222222222222\n" +
				" type commit\n" +
				" tag v1.0\n" +
				" tagger Tag Ger <tag@example.com> 1400000000 +0000\n" +
				" \n" +
				" Tag annotation body",
			wantMessage: "Merge tag 'v1.0'\n",
		},
		{
			name:        "encoding header keeps raw message bytes",
			payload:     treeLine + authorLine + committerLine + "encoding ISO-8859-1\n" + "\n" + "caf\xe9 au lait\n",
			wantHeader:  treeLine + authorLine + committerLine + "encoding ISO-8859-1",
			wantMessage: "caf\xe9 au lait\n",
		},
		{
			name:        "embedded NUL kept",
			payload:     baseHeader + "\n" + "before\x00after\n",
			wantHeader:  treeLine + authorLine + strings.TrimSuffix(committerLine, "\n"),
			wantMessage: "before\x00after\n",
		},
		{
			name:        "empty message after separator",
			payload:     baseHeader + "\n",
			wantHeader:  treeLine + authorLine + strings.TrimSuffix(committerLine, "\n"),
			wantMessage: "",
		},
		{
			name:        "header-only no separator",
			payload:     strings.TrimSuffix(baseHeader, "\n"),
			wantHeader:  strings.TrimSuffix(baseHeader, "\n"),
			wantMessage: "",
		},
		{
			name:        "empty payload",
			payload:     "",
			wantHeader:  "",
			wantMessage: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header, message := splitCommitPayload([]byte(tt.payload))
			assert.Equal(t, tt.wantHeader, string(header), "header half")
			assert.Equal(t, tt.wantMessage, string(message), "message half")
		})
	}
}

// TestMetaCacheMessage drives raw payloads through metaCache.get and checks
// the Message field plus the split-before-parse hazard: a message line
// starting with "author " at column 0 must not alter the parsed author.
func TestMetaCacheMessage(t *testing.T) {
	const (
		treeLine      = "tree 1234567890abcdef1234567890abcdef12345678\n"
		authorLine    = "author Real Author <real@example.com> 1500000000 +0000\n"
		committerLine = "committer Real Committer <rc@example.com> 1600000000 +0000\n"
	)

	tests := []struct {
		name        string
		payload     string
		wantName    string
		wantEmail   string
		wantMessage string
	}{
		{
			name:        "plain message",
			payload:     treeLine + authorLine + committerLine + "\nFix parser\n",
			wantName:    "Real Author",
			wantEmail:   "real@example.com",
			wantMessage: "Fix parser\n",
		},
		{
			name: "message containing author line at column 0 must not corrupt parse",
			payload: treeLine + authorLine + committerLine + "\n" +
				"Quoting a header:\n" +
				"author Evil Impostor <evil@example.com> 999 +0000\n" +
				"committer Evil Committer <evil2@example.com> 999 +0000\n",
			wantName:  "Real Author",
			wantEmail: "real@example.com",
			wantMessage: "Quoting a header:\n" +
				"author Evil Impostor <evil@example.com> 999 +0000\n" +
				"committer Evil Committer <evil2@example.com> 999 +0000\n",
		},
		{
			name:        "empty message",
			payload:     treeLine + authorLine + committerLine + "\n",
			wantName:    "Real Author",
			wantEmail:   "real@example.com",
			wantMessage: "",
		},
		{
			name:        "no separator",
			payload:     treeLine + authorLine + strings.TrimSuffix(committerLine, "\n"),
			wantName:    "Real Author",
			wantEmail:   "real@example.com",
			wantMessage: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockReader := newMockCommitPayloadReader()
			oid := Hash{0xAB}
			mockReader.addRawPayload(oid, []byte(tt.payload))

			cache := newMetaCache(nil, mockReader)
			meta, err := cache.get(oid)
			require.NoError(t, err)

			assert.Equal(t, tt.wantName, meta.Author.Name)
			assert.Equal(t, tt.wantEmail, meta.Author.Email)
			assert.Equal(t, tt.wantMessage, meta.Message)

			// The cached entry must serve the identical message on a hit.
			again, err := cache.get(oid)
			require.NoError(t, err)
			assert.Equal(t, meta.Message, again.Message)
		})
	}
}

// slowCommitPayloadReader wraps a mockCommitPayloadReader and injects an
// artificial delay before each read. It is used to test cache-miss coalescing
// and slow-path behaviour in the metaCache.
type slowCommitPayloadReader struct {
	delegate *mockCommitPayloadReader
	delay    time.Duration
}

func (s *slowCommitPayloadReader) readCommitPayload(oid Hash) ([]byte, error) {
	time.Sleep(s.delay)
	return s.delegate.readCommitPayload(oid)
}

// BenchmarkParseAuthorHeader measures the throughput of parseAuthorHeader across
// several representative header formats (short, multi-line, long name).
func BenchmarkParseAuthorHeader(b *testing.B) {
	headers := [][]byte{
		[]byte("author John Doe <john@example.com> 1234567890 +0000"),
		[]byte("tree abc\nauthor Jane Smith <jane@example.com> 9876543210 +0000\ncommitter Bob <bob@example.com> 1111111111 +0000"),
		[]byte("author Very Long Name With Many Words <verylongemail@example.com> 1500000000 +0000"),
	}

	for i := 0; b.Loop(); i++ {
		header := headers[i%len(headers)]
		_, _ = parseAuthorHeader(header)
	}
}

// BenchmarkMetaCacheAuthor measures the fast-path (cache-hit) performance of
// metaCache.get by pre-populating the cache with 1000 entries and reading from
// a rotating set of 10 OIDs.
func BenchmarkMetaCacheAuthor(b *testing.B) {
	mockReader := newMockCommitPayloadReader()

	graph := &commitGraphData{
		Timestamps: make([]int64, 1000),
		OIDToIndex: make(map[Hash]int),
	}

	metaCache := newMetaCache(graph, mockReader)

	for i := range 1000 {
		oid := Hash{byte(i)}
		graph.OIDToIndex[oid] = i
		graph.Timestamps[i] = int64(1500000000 + i)
		metaCache.m[oid] = metaEntry{ai: AuthorInfo{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
			When:  time.Unix(int64(1500000000+i), 0).UTC(),
		}, msg: mockCommitMessage}
	}

	oids := make([]Hash, 10)
	for i := range oids {
		oids[i] = Hash{byte(i)}
	}

	b.ResetTimer()
	b.Run("cached", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			oid := oids[i%len(oids)]
			_, _ = metaCache.get(oid)
		}
	})
}

// BenchmarkMetaCacheConcurrent measures the throughput of metaCache.get under
// parallel access from multiple goroutines, all reading from a pre-populated
// cache of 100 entries.
func BenchmarkMetaCacheConcurrent(b *testing.B) {
	mockReader := newMockCommitPayloadReader()

	graph := &commitGraphData{
		Timestamps: make([]int64, 100),
		OIDToIndex: make(map[Hash]int),
	}

	metaCache := newMetaCache(graph, mockReader)

	// Pre-populate
	for i := range 100 {
		oid := Hash{byte(i)}
		graph.OIDToIndex[oid] = i
		graph.Timestamps[i] = int64(1500000000 + i)
		metaCache.m[oid] = metaEntry{ai: AuthorInfo{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
			When:  time.Unix(int64(1500000000+i), 0).UTC(),
		}, msg: mockCommitMessage}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			oid := Hash{byte(i % 100)}
			_, _ = metaCache.get(oid)
			i++
		}
	})
}

// BenchmarkMetaCacheAuthorSlow measures slow-path (cache-miss) performance by
// periodically clearing the cache, forcing metaCache.get to re-parse commit
// headers from the mock reader across a wide range of 10,000 OIDs.
func BenchmarkMetaCacheAuthorSlow(b *testing.B) {
	mockReader := newMockCommitPayloadReader()

	graph := &commitGraphData{
		Timestamps: make([]int64, 10000),
		OIDToIndex: make(map[Hash]int),
	}

	// Pre-populate the mock reader with many more headers to avoid cache hits.
	for i := range 10_000 {
		oid := Hash{byte(i), byte(i >> 8), byte(i >> 16)}
		graph.OIDToIndex[oid] = i
		graph.Timestamps[i] = int64(1500000000 + i)
		mockReader.addCommit(oid, fmt.Sprintf("User%d", i), fmt.Sprintf("user%d@example.com", i), int64(1500000000+i))
	}

	metaCache := newMetaCache(graph, mockReader)

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Clear cache every 100 iterations to ensure we hit the slow path.
		if i%100 == 0 {
			metaCache.mu.Lock()
			metaCache.m = make(map[Hash]metaEntry, 1024)
			metaCache.mu.Unlock()
		}

		// Use a wide range of OIDs to minimize cache hits.
		oid := Hash{byte(i), byte(i >> 8), byte(i >> 16)}
		_, _ = metaCache.get(oid)
	}
}

// TestMetaCacheAuthorSlowPath verifies the cache-miss (slow) path: when an OID
// is not in the cache, metaCache.get fetches the header from the store, parses
// the author info, and populates the cache for subsequent lookups.
func TestMetaCacheAuthorSlowPath(t *testing.T) {
	mockReader := newMockCommitPayloadReader()

	graph := &commitGraphData{
		Timestamps: []int64{1500000000, 1600000000, 1700000000},
		OIDToIndex: map[Hash]int{
			{0x01, 0x00, 0x00}: 0,
			{0x02, 0x00, 0x00}: 1,
			{0x03, 0x00, 0x00}: 2,
		},
	}

	testOID1 := Hash{0x01, 0x00, 0x00}
	testOID2 := Hash{0x02, 0x00, 0x00}
	testOID3 := Hash{0x03, 0x00, 0x00}

	mockReader.addCommit(testOID1, "Alice Johnson", "alice@example.com", 1500000000)
	mockReader.addCommit(testOID2, "Bob Smith", "bob@test.org", 1600000000)
	mockReader.addCommit(testOID3, "Carol Williams", "carol@company.net", 1700000000)

	metaCache := newMetaCache(graph, mockReader)

	// Ensure cache miss by clearing the cache
	metaCache.mu.Lock()
	delete(metaCache.m, testOID1)
	metaCache.mu.Unlock()

	result1, err := metaCache.get(testOID1)
	require.NoError(t, err)

	assert.Equal(t, "Alice Johnson", result1.Author.Name)
	assert.Equal(t, "alice@example.com", result1.Author.Email)
	assert.True(t, result1.Author.When.Equal(time.Unix(1500000000, 0).UTC()))
	assert.Equal(t, mockCommitMessage, result1.Message)

	// Check that result is now cached
	metaCache.mu.RLock()
	cached1, exists1 := metaCache.m[testOID1]
	metaCache.mu.RUnlock()

	assert.True(t, exists1, "Expected result to be cached after slow path execution")
	assert.Equal(t, result1.Author, cached1.ai, "Cached result should match returned result")
	assert.Equal(t, result1.Message, cached1.msg, "Cached message should match returned message")
}

// TestMetaCacheAuthorFastPathAfterCache verifies that after an initial fetch
// populates the cache, a second get for the same OID returns identical results
// from the fast (cached) path without re-reading from the store.
func TestMetaCacheAuthorFastPathAfterCache(t *testing.T) {
	mockReader := newMockCommitPayloadReader()

	graph := &commitGraphData{
		Timestamps: []int64{1500000000},
		OIDToIndex: map[Hash]int{
			{0x01, 0x00, 0x00}: 0,
		},
	}

	testOID := Hash{0x01, 0x00, 0x00}
	mockReader.addCommit(testOID, "Alice Johnson", "alice@example.com", 1500000000)

	metaCache := newMetaCache(graph, mockReader)

	result1, err := metaCache.get(testOID)
	require.NoError(t, err)

	result2, err := metaCache.get(testOID)
	require.NoError(t, err)

	assert.Equal(t, result1.Author, result2.Author, "Cache hit should return identical result")
	assert.Equal(t, result1.Message, result2.Message, "Cache hit should return identical message")
}

// TestMetaCacheAuthorSlowPathError verifies that metaCache.get returns an error
// when the requested OID does not exist in the graph or the underlying store.
func TestMetaCacheAuthorSlowPathError(t *testing.T) {
	mockReader := newMockCommitPayloadReader()

	graph := &commitGraphData{
		Timestamps: []int64{},
		OIDToIndex: map[Hash]int{},
	}

	nonExistentOID := Hash{0xFF, 0xFF, 0xFF}

	metaCache := newMetaCache(graph, mockReader)

	// Ensure cache miss
	metaCache.mu.Lock()
	delete(metaCache.m, nonExistentOID)
	metaCache.mu.Unlock()

	_, err := metaCache.get(nonExistentOID)
	assert.Error(t, err, "Expected error for non-existent commit")
}
