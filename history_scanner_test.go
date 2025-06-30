package objstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	if _, err := os.Stat("testdata/repos"); os.IsNotExist(err) {
		panic("Test data not found. Run ./generate_testdata.sh first")
	}
}

func TestLoadAllCommits_EmptyRepository(t *testing.T) {
	_, err := createScannerForRepoWithError(t, "empty-repo")
	assert.Error(t, err, "Empty repositories don't have commit graphs")
}

func TestLoadAllCommits_LinearHistory(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	commits, err := scanner.LoadAllCommits()
	require.NoError(t, err)
	assert.Len(t, commits, 3)

	for _, c := range commits {
		assert.NotEqual(t, Hash{}, c.OID)
		assert.NotEqual(t, Hash{}, c.TreeOID)
		// assert.Greater(t, c.Timestamp, int64(0))
	}
}

func TestLoadAllCommits_WithMergeCommits(t *testing.T) {
	scanner := createScannerForRepo(t, "with-merges")
	defer scanner.Close()

	commits, err := scanner.LoadAllCommits()
	require.NoError(t, err)
	assert.Len(t, commits, 5)

	var mergeCommit commitInfo
	for _, c := range commits {
		if len(c.ParentOIDs) == 2 {
			mergeCommit = c
			break
		}
	}
	require.NotNil(t, mergeCommit, "Should have a merge commit")
	assert.Len(t, mergeCommit.ParentOIDs, 2)

	hasMerge := false
	for _, c := range commits {
		if len(c.ParentOIDs) > 1 {
			hasMerge = true
			break
		}
	}
	assert.True(t, hasMerge, "Expected to find merge commits")
}

func TestLoadAllCommits_WithCommitGraph(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear") // This repo has commit-graph
	defer scanner.Close()

	commits, err := scanner.LoadAllCommits()
	require.NoError(t, err)
	assert.Len(t, commits, 3)
}

func TestLoadAllCommits_LargeRepo(t *testing.T) {
	scanner := createScannerForRepo(t, "large-repo")
	defer scanner.Close()

	commits, err := scanner.LoadAllCommits()
	require.NoError(t, err)

	assert.Len(t, commits, 100)

	rootCommitCount := 0
	for i, c := range commits {
		assert.NotEqual(t, Hash{}, c.OID, "Commit %d should have valid OID", i)
		assert.NotEqual(t, Hash{}, c.TreeOID, "Commit %d should have valid TreeOID", i)
		// assert.Greater(t, c.Timestamp, int64(0), "Commit %d should have valid timestamp", i)

		// Count root commits (no parents).
		if len(c.ParentOIDs) == 0 {
			rootCommitCount++
		}
	}

	// Should have exactly 1 root commit in a linear history.
	assert.Equal(t, 1, rootCommitCount, "Should have exactly one root commit")

	// Build a set of all commit OIDs for parent validation.
	commitSet := make(map[Hash]bool)
	for _, c := range commits {
		commitSet[c.OID] = true
	}

	// Verify all parent references are valid.
	for i, c := range commits {
		for _, parentOID := range c.ParentOIDs {
			assert.True(t, commitSet[parentOID],
				"Parent %x of commit %d should exist in the commit set",
				parentOID[:8], i)
		}
	}
}

func TestLoadAllCommits_WithoutCommitGraph_ShouldFail(t *testing.T) {
	_, err := createScannerForRepoWithError(t, "no-commit-graph")
	assert.Error(t, err, "Should fail when commit-graph is not available")
}

func TestDiffHistory(t *testing.T) {
	tests := []struct {
		name          string
		repo          string
		expectedFiles []string
		minAdditions  int
		maxAdditions  int
	}{
		{
			name:          "simple additions",
			repo:          "simple-linear",
			expectedFiles: []string{"README.md", "main.go", "test.txt"},
			minAdditions:  3, // At least one line per file
		},
		{
			name:          "with merges",
			repo:          "with-merges",
			expectedFiles: []string{"main.go", "feature.go", "after_merge.txt"},
			minAdditions:  5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scanner := createScannerForRepo(t, tt.repo)
			defer scanner.Close()

			additions, errC := scanner.DiffHistory()

			filesFound := make(map[string]bool)
			additionCount := 0

			for add := range additions {
				filesFound[add.Path] = true
				additionCount += len(add.Lines)
			}

			err := <-errC
			require.NoError(t, err)

			for _, expectedFile := range tt.expectedFiles {
				assert.True(t, filesFound[expectedFile],
					"Expected file %s not found in additions", expectedFile)
			}

			assert.GreaterOrEqual(t, additionCount, tt.minAdditions)
			if tt.maxAdditions > 0 {
				assert.LessOrEqual(t, additionCount, tt.maxAdditions)
			}
		})
	}
}

func createScannerForRepo(t testing.TB, repoName string) *HistoryScanner {
	repoPath := filepath.Join("testdata", "repos", repoName)
	scanner, err := NewHistoryScanner(repoPath)
	require.NoError(t, err)
	return scanner
}

func createScannerForRepoWithError(t testing.TB, repoName string) (*HistoryScanner, error) {
	repoPath := filepath.Join("testdata", "repos", repoName)
	return NewHistoryScanner(repoPath)
}

func BenchmarkLoadAllCommits(b *testing.B) {
	tests := []struct {
		name     string
		repo     string
		expected int
	}{
		{"SimpleLinear", "simple-linear", 3},
		{"WithMerges", "with-merges", 5},
		{"LargeRepo", "large-repo", 100},
		{"VeryLargeRepo1k", "very-large-repo-1k", 1000},
		{"SuperLargeRepo10k", "super-large-repo-10k", 10000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			scanner := createScannerForRepo(b, tt.repo)
			defer scanner.Close()

			for b.Loop() {
				commits, err := scanner.LoadAllCommits()
				require.NoError(b, err)
				require.Len(b, commits, tt.expected)
			}
		})
	}
}

func BenchmarkDiffHistory(b *testing.B) {
	tests := []struct {
		name         string
		repo         string
		minAdditions int
	}{
		{"SimpleLinear", "simple-linear", 3},
		{"WithMerges", "with-merges", 5},
		{"LargeRepo", "large-repo", 100},
		{"VeryLargeRepo1k", "very-large-repo-1k", 1000},
		// {"SuperLargeRepo10k", "super-large-repo-10k", 10000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			scanner := createScannerForRepo(b, tt.repo)
			defer scanner.Close()

			for b.Loop() {
				additions, errC := scanner.DiffHistory()

				// Consume all additions.
				count := 0
				for range additions {
					count++
				}

				err := <-errC
				require.NoError(b, err)
				require.GreaterOrEqual(b, count, tt.minAdditions)
			}
		})
	}
}

func TestDiffHistory_LargeRepo(t *testing.T) {
	scanner := createScannerForRepo(t, "large-repo")
	defer scanner.Close()

	startTime := time.Now()
	additions, errC := scanner.DiffHistory()

	filesFound := make(map[string]bool)
	additionCount := 0
	linesByFile := make(map[string]int)

	for add := range additions {
		filesFound[add.Path] = true
		additionCount += len(add.Lines)
		linesByFile[add.Path] += len(add.Lines)
	}

	err := <-errC
	require.NoError(t, err)

	duration := time.Since(startTime)
	t.Logf("Processed 100 commits in %v", duration)

	assert.Equal(t, 100, len(filesFound), "Should find 100 files")
	assert.True(t, filesFound["README.md"], "Should find README.md")

	for i := 2; i <= 100; i++ {
		filename := fmt.Sprintf("file_%d.txt", i)
		assert.True(t, filesFound[filename], "Should find %s", filename)
		assert.Greater(t, linesByFile[filename], 0,
			"File %s should have additions", filename)
	}

	assert.GreaterOrEqual(t, additionCount, 100)
}

func TestDiffHistory_VeryLargeRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping very large repo test in short mode")
	}

	scanner := createScannerForRepo(t, "very-large-repo-1k")
	defer scanner.Close()

	startTime := time.Now()
	additions, errC := scanner.DiffHistory()

	filesFound := make(map[string]bool)
	additionCount := 0
	linesByFile := make(map[string]int)

	for add := range additions {
		filesFound[add.Path] = true
		additionCount += len(add.Lines)
		linesByFile[add.Path] += len(add.Lines)
	}

	err := <-errC
	require.NoError(t, err)

	duration := time.Since(startTime)
	t.Logf("Processed 1,000 commits in %v", duration)

	assert.Equal(t, 1000, len(filesFound), "Should find 1,000 files")
	assert.True(t, filesFound["README.md"], "Should find README.md")

	// Check a sampling of files
	for i := 2; i <= 1000; i += 100 { // Check every 100th file
		filename := fmt.Sprintf("file_%d.txt", i)
		assert.True(t, filesFound[filename], "Should find %s", filename)
		assert.Greater(t, linesByFile[filename], 0,
			"File %s should have additions", filename)
	}

	assert.GreaterOrEqual(t, additionCount, 1000)
}

func TestDiffHistory_SuperLargeRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping super large repo test in short mode")
	}

	scanner := createScannerForRepo(t, "super-large-repo-10k")
	defer scanner.Close()

	startTime := time.Now()
	additions, errC := scanner.DiffHistory()

	filesFound := make(map[string]bool)
	additionCount := 0

	for add := range additions {
		filesFound[add.Path] = true
		additionCount += len(add.Lines)
	}

	err := <-errC
	require.NoError(t, err)

	duration := time.Since(startTime)
	t.Logf("Processed 10,000 commits in %v", duration)

	assert.Equal(t, 10000, len(filesFound), "Should find 10,000 files")
	assert.True(t, filesFound["README.md"], "Should find README.md")

	// Check a sampling of files to avoid excessive assertions
	for i := 2; i <= 10000; i += 1000 { // Check every 1000th file
		filename := fmt.Sprintf("file_%d.txt", i)
		assert.True(t, filesFound[filename], "Should find %s", filename)
	}

	assert.GreaterOrEqual(t, additionCount, 10000)
}

func TestLoadAllCommits_VeryLargeRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping very large repo test in short mode")
	}

	scanner := createScannerForRepo(t, "very-large-repo-1k")
	defer scanner.Close()

	startTime := time.Now()
	commits, err := scanner.LoadAllCommits()
	duration := time.Since(startTime)

	require.NoError(t, err)
	assert.Len(t, commits, 1000)

	t.Logf("Loaded 1,000 commits in %v", duration)

	// Validate structure
	rootCommitCount := 0
	for _, c := range commits {
		assert.NotEqual(t, Hash{}, c.OID)
		assert.NotEqual(t, Hash{}, c.TreeOID)
		// assert.Greater(t, c.Timestamp, int64(0))

		if len(c.ParentOIDs) == 0 {
			rootCommitCount++
		}
	}

	assert.Equal(t, 1, rootCommitCount, "Should have exactly one root commit")
}

func TestLoadAllCommits_SuperLargeRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping super large repo test in short mode")
	}

	scanner := createScannerForRepo(t, "super-large-repo-10k")
	defer scanner.Close()

	startTime := time.Now()
	commits, err := scanner.LoadAllCommits()
	duration := time.Since(startTime)

	require.NoError(t, err)
	assert.Len(t, commits, 10000)

	t.Logf("Loaded 10,000 commits in %v", duration)

	// Validate structure
	rootCommitCount := 0
	for _, c := range commits {
		assert.NotEqual(t, Hash{}, c.OID)
		assert.NotEqual(t, Hash{}, c.TreeOID)
		// assert.Greater(t, c.Timestamp, int64(0))

		if len(c.ParentOIDs) == 0 {
			rootCommitCount++
		}
	}

	assert.Equal(t, 1, rootCommitCount, "Should have exactly one root commit")
}
