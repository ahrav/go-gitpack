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

func TestDiffHistoryHunks(t *testing.T) {
	tests := []struct {
		name          string
		repo          string
		expectedFiles []string
		minHunks      int
		maxHunks      int
	}{
		{
			name:          "simple additions - hunks",
			repo:          "simple-linear",
			expectedFiles: []string{"README.md", "main.go", "test.txt"},
			minHunks:      3, // At least one hunk per file
		},
		{
			name:          "with merges - hunks",
			repo:          "with-merges",
			expectedFiles: []string{"main.go", "feature.go", "after_merge.txt"},
			minHunks:      3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scanner := createScannerForRepo(t, tt.repo)
			defer scanner.Close()

			hunks, errC := scanner.DiffHistoryHunks()

			filesFound := make(map[string]bool)
			hunkCount := 0
			totalLines := 0

			for hunk := range hunks {
				filesFound[hunk.Path()] = true
				hunkCount++
				totalLines += len(hunk.Lines())

				// Validate hunk properties
				assert.Greater(t, hunk.StartLine(), 0, "StartLine should be positive")
				assert.GreaterOrEqual(t, hunk.EndLine(), hunk.StartLine(), "EndLine should be >= StartLine")
				assert.NotEmpty(t, hunk.Lines(), "Hunk should have at least one line")
				assert.NotEmpty(t, hunk.Path(), "Hunk should have a path")
				assert.NotEqual(t, Hash{}, hunk.Commit(), "Hunk should have a valid commit")

				// Verify EndLine calculation
				expectedEndLine := hunk.StartLine() + len(hunk.Lines()) - 1
				assert.Equal(t, expectedEndLine, hunk.EndLine(), "EndLine calculation should be correct")
			}

			err := <-errC
			require.NoError(t, err)

			for _, expectedFile := range tt.expectedFiles {
				assert.True(t, filesFound[expectedFile],
					"Expected file %s not found in hunks", expectedFile)
			}

			assert.GreaterOrEqual(t, hunkCount, tt.minHunks)
			if tt.maxHunks > 0 {
				assert.LessOrEqual(t, hunkCount, tt.maxHunks)
			}

			// Verify we have some lines in total
			assert.Greater(t, totalLines, 0, "Should have some lines in hunks")

			t.Logf("Found %d hunks with %d total lines", hunkCount, totalLines)
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

func BenchmarkDiffHistoryHunks(b *testing.B) {
	tests := []struct {
		name     string
		repo     string
		minHunks int
	}{
		{"SimpleLinear", "simple-linear", 3},
		{"WithMerges", "with-merges", 3},
		{"LargeRepo", "large-repo", 50},
		{"VeryLargeRepo1k", "very-large-repo-1k", 500},
		// {"SuperLargeRepo10k", "super-large-repo-10k", 5000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			scanner := createScannerForRepo(b, tt.repo)
			defer scanner.Close()

			for b.Loop() {
				hunks, errC := scanner.DiffHistoryHunks()

				// Consume all hunks.
				count := 0
				for range hunks {
					count++
				}

				err := <-errC
				require.NoError(b, err)
				require.GreaterOrEqual(b, count, tt.minHunks)
			}
		})
	}
}

func TestDiffHistoryHunks_LargeRepo(t *testing.T) {
	scanner := createScannerForRepo(t, "large-repo")
	defer scanner.Close()

	startTime := time.Now()
	hunks, errC := scanner.DiffHistoryHunks()

	filesFound := make(map[string]bool)
	hunkCount := 0
	totalLines := 0
	linesByFile := make(map[string]int)

	for hunk := range hunks {
		filesFound[hunk.Path()] = true
		hunkCount++
		totalLines += len(hunk.Lines())
		linesByFile[hunk.Path()] += len(hunk.Lines())

		// Validate hunk properties
		assert.Greater(t, hunk.StartLine(), 0, "StartLine should be positive")
		assert.GreaterOrEqual(t, hunk.EndLine(), hunk.StartLine(), "EndLine should be >= StartLine")
		assert.NotEmpty(t, hunk.Lines(), "Hunk should have at least one line")
	}

	err := <-errC
	require.NoError(t, err)

	duration := time.Since(startTime)
	t.Logf("Processed 100 commits as hunks in %v", duration)

	assert.Equal(t, 100, len(filesFound), "Should find 100 files")
	assert.True(t, filesFound["README.md"], "Should find README.md")

	for i := 2; i <= 100; i++ {
		filename := fmt.Sprintf("file_%d.txt", i)
		assert.True(t, filesFound[filename], "Should find %s", filename)
		assert.Greater(t, linesByFile[filename], 0,
			"File %s should have lines in hunks", filename)
	}

	// Hunks should be fewer than individual line additions since consecutive lines get grouped
	assert.GreaterOrEqual(t, hunkCount, 50)
	assert.LessOrEqual(t, hunkCount, 200) // Should be significantly fewer than line count
	assert.GreaterOrEqual(t, totalLines, 100)

	t.Logf("Found %d hunks with %d total lines (avg %.1f lines per hunk)",
		hunkCount, totalLines, float64(totalLines)/float64(hunkCount))
}

func TestDiffHistoryHunks_VeryLargeRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping very large repo test in short mode")
	}

	scanner := createScannerForRepo(t, "very-large-repo-1k")
	defer scanner.Close()

	startTime := time.Now()
	hunks, errC := scanner.DiffHistoryHunks()

	filesFound := make(map[string]bool)
	hunkCount := 0
	totalLines := 0

	for hunk := range hunks {
		filesFound[hunk.Path()] = true
		hunkCount++
		totalLines += len(hunk.Lines())

		// Validate properties
		assert.Greater(t, hunk.StartLine(), 0)
		assert.GreaterOrEqual(t, hunk.EndLine(), hunk.StartLine())
		assert.NotEmpty(t, hunk.Lines())
	}

	err := <-errC
	require.NoError(t, err)

	duration := time.Since(startTime)
	t.Logf("Processed 1,000 commits as hunks in %v", duration)

	assert.Equal(t, 1000, len(filesFound), "Should find 1,000 files")
	assert.True(t, filesFound["README.md"], "Should find README.md")

	// Check a sampling of files
	for i := 2; i <= 1000; i += 100 { // Check every 100th file
		filename := fmt.Sprintf("file_%d.txt", i)
		assert.True(t, filesFound[filename], "Should find %s", filename)
	}

	assert.GreaterOrEqual(t, hunkCount, 500)
	assert.GreaterOrEqual(t, totalLines, 1000)

	t.Logf("Found %d hunks with %d total lines (avg %.1f lines per hunk)",
		hunkCount, totalLines, float64(totalLines)/float64(hunkCount))
}

func TestDiffHistoryHunks_SuperLargeRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping super large repo test in short mode")
	}

	scanner := createScannerForRepo(t, "super-large-repo-10k")
	defer scanner.Close()

	startTime := time.Now()
	hunks, errC := scanner.DiffHistoryHunks()

	filesFound := make(map[string]bool)
	hunkCount := 0
	totalLines := 0

	for hunk := range hunks {
		filesFound[hunk.Path()] = true
		hunkCount++
		totalLines += len(hunk.Lines())
	}

	err := <-errC
	require.NoError(t, err)

	duration := time.Since(startTime)
	t.Logf("Processed 10,000 commits as hunks in %v", duration)

	assert.Equal(t, 10000, len(filesFound), "Should find 10,000 files")
	assert.True(t, filesFound["README.md"], "Should find README.md")

	// Check a sampling of files to avoid excessive assertions
	for i := 2; i <= 10000; i += 1000 { // Check every 1000th file
		filename := fmt.Sprintf("file_%d.txt", i)
		assert.True(t, filesFound[filename], "Should find %s", filename)
	}

	assert.GreaterOrEqual(t, hunkCount, 5000)
	assert.GreaterOrEqual(t, totalLines, 10000)

	t.Logf("Found %d hunks with %d total lines (avg %.1f lines per hunk)",
		hunkCount, totalLines, float64(totalLines)/float64(hunkCount))
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
