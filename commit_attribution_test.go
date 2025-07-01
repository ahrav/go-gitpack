package objstore

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCommitHeaderReader struct {
	headers map[Hash][]byte
	mu      sync.RWMutex
}

func newMockCommitHeaderReader() *mockCommitHeaderReader {
	return &mockCommitHeaderReader{
		headers: make(map[Hash][]byte),
	}
}

func (m *mockCommitHeaderReader) readCommitHeader(oid Hash) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if header, ok := m.headers[oid]; ok {
		return header, nil
	}
	return nil, fmt.Errorf("object %x not found", oid)
}

func (m *mockCommitHeaderReader) addCommit(oid Hash, author, email string, timestamp int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	header := fmt.Sprintf(
		"tree 1234567890abcdef1234567890abcdef12345678\n"+
			"author %s <%s> %d +0000\n"+
			"committer %s <%s> %d +0000\n\n"+
			"Test commit message\n",
		author, email, timestamp,
		author, email, timestamp,
	)
	m.headers[oid] = []byte(header)
}

func (m *mockCommitHeaderReader) addRawHeader(oid Hash, header []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.headers[oid] = header
}

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

	mockReader := newMockCommitHeaderReader()
	cache := newMetaCache(graph, mockReader)

	assert.NotNil(t, cache)
	assert.Equal(t, graph, cache.graph)
	assert.Equal(t, commitHeaderReader(mockReader), cache.store)
	assert.Equal(t, graph.Timestamps, cache.ts)
	assert.NotNil(t, cache.m)
	assert.Equal(t, 0, len(cache.m))
}

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

			cache := &metaCache{
				graph: graph,
				ts:    graph.Timestamps,
			}

			ts, found := cache.timestamp(tt.oid)
			assert.Equal(t, tt.wantTS, ts)
			assert.Equal(t, tt.wantFound, found)
		})
	}
}

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

func TestMetaCacheAuthor(t *testing.T) {
	mockReader := newMockCommitHeaderReader()

	metaCache := &metaCache{
		store: mockReader,
		m:     make(map[Hash]AuthorInfo),
	}

	t.Run("successful retrieval and caching", func(t *testing.T) {
		oid := Hash{1, 2, 3}
		mockReader.addCommit(oid, "John Doe", "john@example.com", 1234567890)

		ai1, err := metaCache.author(oid)
		assert.NoError(t, err)
		assert.Equal(t, "John Doe", ai1.Name)
		assert.Equal(t, "john@example.com", ai1.Email)
		assert.Equal(t, time.Unix(1234567890, 0).UTC(), ai1.When)

		assert.Contains(t, metaCache.m, oid)

		ai2, err := metaCache.author(oid)
		assert.NoError(t, err)
		assert.Equal(t, ai1, ai2)
	})

	t.Run("object not found", func(t *testing.T) {
		oid := Hash{99, 99, 99}
		_, err := metaCache.author(oid)
		assert.Error(t, err)
	})

	t.Run("malformed header", func(t *testing.T) {
		oid := Hash{4, 5, 6}
		mockReader.addRawHeader(oid, []byte("invalid header"))

		_, err := metaCache.author(oid)
		assert.Error(t, err)
	})
}

func TestMetaCacheConcurrentAccess(t *testing.T) {
	mockReader := newMockCommitHeaderReader()

	metaCache := &metaCache{
		store: mockReader,
		m:     make(map[Hash]AuthorInfo),
	}

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
				ai, err := metaCache.author(oid)

				if err != nil {
					errors <- fmt.Errorf("goroutine %d: %w", goroutine, err)
					continue
				}

				expectedName := fmt.Sprintf("User%d", oid[0])
				if ai.Name != expectedName {
					errors <- fmt.Errorf("goroutine %d: expected name %s, got %s",
						goroutine, expectedName, ai.Name)
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

// func TestMetaCacheEdgeCases(t *testing.T) {
// 	synctest.Run(func() {
// 		t.Run("concurrent reads of same oid", func(t *testing.T) {
// 			mockReader := newMockCommitHeaderReader()
// 			oid := Hash{1, 2, 3}
// 			mockReader.addCommit(oid, "John Doe", "john@example.com", 1234567890)

// 			metaCache := &metaCache{
// 				store: mockReader,
// 				m:     make(map[Hash]AuthorInfo),
// 			}

// 			// Multiple goroutines trying to read the same OID concurrently
// 			var wg sync.WaitGroup
// 			numGoroutines := 50
// 			results := make([]AuthorInfo, numGoroutines)
// 			errors := make([]error, numGoroutines)

// 			for i := 0; i < numGoroutines; i++ {
// 				wg.Add(1)
// 				go func(idx int) {
// 					defer wg.Done()
// 					results[idx], errors[idx] = metaCache.author(oid)
// 				}(i)
// 			}

// 			wg.Wait()

// 			// All reads should succeed with the same result
// 			for i := 0; i < numGoroutines; i++ {
// 				assert.NoError(t, errors[i])
// 				if i > 0 {
// 					assert.Equal(t, results[0], results[i])
// 				}
// 			}

// 			// Should be cached after first read
// 			assert.Contains(t, metaCache.m, oid)
// 		})

// 		t.Run("cache miss followed by concurrent reads", func(t *testing.T) {
// 			mockReader := newMockCommitHeaderReader()
// 			oid := Hash{4, 5, 6}

// 			// Simulate slow header read
// 			slowReader := &slowCommitHeaderReader{
// 				delegate: mockReader,
// 				delay:    50 * time.Millisecond,
// 			}
// 			slowReader.delegate.addCommit(oid, "Jane Doe", "jane@example.com", 1234567890)

// 			metaCache := &metaCache{
// 				store: slowReader,
// 				m:     make(map[Hash]AuthorInfo),
// 			}

// 			// Start multiple readers simultaneously
// 			var wg sync.WaitGroup
// 			results := make([]AuthorInfo, 10)

// 			start := time.Now()
// 			for i := 0; i < 10; i++ {
// 				wg.Add(1)
// 				go func(idx int) {
// 					defer wg.Done()
// 					ai, err := metaCache.author(oid)
// 					assert.NoError(t, err)
// 					results[idx] = ai
// 				}(i)
// 			}

// 			wg.Wait()
// 			elapsed := time.Since(start)

// 			// All results should be identical
// 			for i := 1; i < 10; i++ {
// 				assert.Equal(t, results[0], results[i])
// 			}

// 			// Should have taken roughly the delay time, not 10x the delay
// 			// (meaning requests were coalesced, not serialized)
// 			assert.Less(t, elapsed, 100*time.Millisecond)
// 		})
// 	})
// }

type slowCommitHeaderReader struct {
	delegate *mockCommitHeaderReader
	delay    time.Duration
}

func (s *slowCommitHeaderReader) readCommitHeader(oid Hash) ([]byte, error) {
	time.Sleep(s.delay)
	return s.delegate.readCommitHeader(oid)
}

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

func BenchmarkMetaCacheAuthor(b *testing.B) {
	mockReader := newMockCommitHeaderReader()

	metaCache := &metaCache{
		store: mockReader,
		m:     make(map[Hash]AuthorInfo),
	}

	for i := range 1000 {
		oid := Hash{byte(i)}
		metaCache.m[oid] = AuthorInfo{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
			When:  time.Unix(int64(1500000000+i), 0).UTC(),
		}
	}

	oids := make([]Hash, 10)
	for i := range oids {
		oids[i] = Hash{byte(i)}
	}

	b.ResetTimer()
	b.Run("cached", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			oid := oids[i%len(oids)]
			_, _ = metaCache.author(oid)
		}
	})
}

func BenchmarkMetaCacheConcurrent(b *testing.B) {
	mockReader := newMockCommitHeaderReader()

	metaCache := &metaCache{
		store: mockReader,
		m:     make(map[Hash]AuthorInfo),
	}

	// Pre-populate
	for i := range 100 {
		oid := Hash{byte(i)}
		metaCache.m[oid] = AuthorInfo{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
			When:  time.Unix(int64(1500000000+i), 0).UTC(),
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			oid := Hash{byte(i % 100)}
			_, _ = metaCache.author(oid)
			i++
		}
	})
}

func BenchmarkMetaCacheAuthorSlow(b *testing.B) {
	mockReader := newMockCommitHeaderReader()

	// Pre-populate the mock reader with many more headers to avoid cache hits.
	for i := range 10_000 {
		oid := Hash{byte(i), byte(i >> 8), byte(i >> 16)}
		mockReader.addCommit(oid, fmt.Sprintf("User%d", i), fmt.Sprintf("user%d@example.com", i), int64(1500000000+i))
	}

	metaCache := &metaCache{
		store: mockReader,
		m:     make(map[Hash]AuthorInfo, 1024), // Start with empty cache
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Clear cache every 100 iterations to ensure we hit the slow path.
		if i%100 == 0 {
			metaCache.mu.Lock()
			metaCache.m = make(map[Hash]AuthorInfo, 1024)
			metaCache.mu.Unlock()
		}

		// Use a wide range of OIDs to minimize cache hits.
		oid := Hash{byte(i), byte(i >> 8), byte(i >> 16)}
		_, _ = metaCache.author(oid)
	}
}

func TestMetaCacheAuthorSlowPath(t *testing.T) {
	mockReader := newMockCommitHeaderReader()

	testOID1 := Hash{0x01, 0x00, 0x00}
	testOID2 := Hash{0x02, 0x00, 0x00}
	testOID3 := Hash{0x03, 0x00, 0x00}

	mockReader.addCommit(testOID1, "Alice Johnson", "alice@example.com", 1500000000)
	mockReader.addCommit(testOID2, "Bob Smith", "bob@test.org", 1600000000)
	mockReader.addCommit(testOID3, "Carol Williams", "carol@company.net", 1700000000)

	metaCache := &metaCache{
		store: mockReader,
		m:     make(map[Hash]AuthorInfo, 1024),
	}

	metaCache.mu.Lock()
	delete(metaCache.m, testOID1)
	metaCache.mu.Unlock()

	result1, err := metaCache.author(testOID1)
	require.NoError(t, err)

	assert.Equal(t, "Alice Johnson", result1.Name)
	assert.Equal(t, "alice@example.com", result1.Email)
	assert.True(t, result1.When.Equal(time.Unix(1500000000, 0).UTC()))

	metaCache.mu.RLock()
	cached1, exists1 := metaCache.m[testOID1]
	metaCache.mu.RUnlock()

	assert.True(t, exists1, "Expected result to be cached after slow path execution")
	assert.Equal(t, result1, cached1, "Cached result should match returned result")
}

func TestMetaCacheAuthorFastPathAfterCache(t *testing.T) {
	mockReader := newMockCommitHeaderReader()
	testOID := Hash{0x01, 0x00, 0x00}
	mockReader.addCommit(testOID, "Alice Johnson", "alice@example.com", 1500000000)

	metaCache := &metaCache{
		store: mockReader,
		m:     make(map[Hash]AuthorInfo, 1024),
	}

	result1, err := metaCache.author(testOID)
	require.NoError(t, err)

	result2, err := metaCache.author(testOID)
	require.NoError(t, err)

	assert.Equal(t, result1, result2, "Cache hit should return identical result")
}

func TestMetaCacheAuthorSlowPathError(t *testing.T) {
	mockReader := newMockCommitHeaderReader()
	nonExistentOID := Hash{0xFF, 0xFF, 0xFF}

	metaCache := &metaCache{
		store: mockReader,
		m:     make(map[Hash]AuthorInfo, 1024),
	}

	metaCache.mu.Lock()
	delete(metaCache.m, nonExistentOID)
	metaCache.mu.Unlock()

	_, err := metaCache.author(nonExistentOID)
	assert.Error(t, err, "Expected error for non-existent commit")
}
