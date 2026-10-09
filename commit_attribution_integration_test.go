// commit_attribution_integration_test.go is an integration-level test that
// opens pre-generated test repositories via HistoryScanner, loads commits,
// retrieves each commit object, and verifies that GetCommitMetadata returns
// non-empty author names, emails, and timestamps. It covers repositories
// with linear history, merge commits, many commits, the fallback walker
// (no commit-graph), and the empty-repository edge case.
package objstore

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCommitAttributionIntegration iterates over three test repositories
// (simple-linear, with-merges, large-repo) and, for each, loads commits and
// tests author metadata on up to the first 5 commits. The 5-commit limit
// keeps the test fast while still exercising the full retrieval-and-parse
// path for each repository layout.
func TestCommitAttributionIntegration(t *testing.T) {
	repos := []string{
		"simple-linear",
		"with-merges",
		"large-repo",
	}

	for _, repoName := range repos {
		t.Run(repoName, func(t *testing.T) {
			repoPath := filepath.Join("testdata", "repos", repoName)

			// Open the repository using HistoryScanner
			scanner, err := NewHistoryScanner(repoPath)
			require.NoError(t, err)
			defer scanner.Close()

			// Load all commits from the repository
			commits, err := scanner.loadAllCommits()
			require.NoError(t, err)
			require.NotEmpty(t, commits, "Repository should have commits")

			// Test author retrieval for the first few commits. The limit of 5
			// keeps the test fast for the large-repo case (100 commits) while
			// still validating metadata parsing for a representative sample.
			testCount := len(commits)
			if testCount > 5 {
				testCount = 5
			}

			validCommitsTested := 0
			for i := 0; i < testCount; i++ {
				commit := commits[i]

				// First verify this is actually a commit object
				data, objType, err := scanner.get(commit.OID)
				if err != nil {
					t.Logf("Skipping object %x: could not retrieve: %v", commit.OID, err)
					continue
				}

				// Skip non-commit objects (this indicates a bug in commit-graph loading)
				if objType != ObjCommit {
					t.Logf("Skipping object %x: type %v is not a commit", commit.OID, objType)
					continue
				}

				// Note: Empty data is expected for commit objects in this implementation
				t.Logf("Testing commit %x (data len: %d)", commit.OID, len(data))

				// Test GetCommitMetadata method.
				meta, err := scanner.GetCommitMetadata(commit.OID)
				if err != nil {
					t.Logf("Skipping commit %x: could not get metadata: %v", commit.OID, err)
					continue
				}

				assert.NotEmpty(t, meta.Author.Name, "Author name should not be empty for commit %x", commit.OID)
				assert.NotEmpty(t, meta.Author.Email, "Author email should not be empty for commit %x", commit.OID)
				assert.False(t, meta.Author.When.IsZero(), "Author timestamp should not be zero for commit %x", commit.OID)
				assert.Greater(t, meta.Timestamp, int64(0), "Timestamp should be positive for commit %x", commit.OID)

				// Verify timestamp consistency.
				assert.Equal(t, meta.Author.When.Unix(), meta.Timestamp, "Timestamp from AuthorInfo and CommitMetadata should match for commit %x", commit.OID)

				validCommitsTested++
			}

			// Ensure we tested at least one valid commit
			assert.Greater(t, validCommitsTested, 0, "Should have tested at least one valid commit in %s", repoName)
		})
	}
}

// TestCommitAttributionWithoutCommitGraph verifies that commit discovery
// works through the fallback walker when no commit-graph file is present.
func TestCommitAttributionWithoutCommitGraph(t *testing.T) {
	repoPath := filepath.Join("testdata", "repos", "no-commit-graph")

	scanner, err := NewHistoryScanner(repoPath)
	require.NoError(t, err)
	defer scanner.Close()

	commits, err := scanner.loadAllCommits()
	require.NoError(t, err)
	assert.NotEmpty(t, commits, "Fallback loader should discover commits without commit-graph")
}

// TestCommitAttributionEmptyRepo confirms that an empty repository (no
// commits at all) returns an empty slice without error.
func TestCommitAttributionEmptyRepo(t *testing.T) {
	repoPath := filepath.Join("testdata", "repos", "empty-repo")

	scanner, err := NewHistoryScanner(repoPath)
	require.NoError(t, err)
	defer scanner.Close()

	commits, err := scanner.loadAllCommits()
	require.NoError(t, err)
	assert.Empty(t, commits)
}

// TestCommitMessageAgainstGitOracle verifies, for EVERY commit in each
// generated fixture repository, that GetCommitMetadata.Message byte-equals
// the bytes after the first "\n\n" of `git cat-file commit` output.
//
// The oracle is deliberately `git cat-file commit` and NOT `git log
// --format=%B`: %B re-encodes to the log output encoding and truncates at
// embedded NULs, both presentation behaviors of git-log rather than object
// format. Author fields are additionally asserted non-empty so a payload
// split that ate the header would not pass silently.
func TestCommitMessageAgainstGitOracle(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repos := []string{
		"simple-linear",
		"with-merges",
		"large-repo",
	}

	for _, repoName := range repos {
		t.Run(repoName, func(t *testing.T) {
			repoPath := filepath.Join("testdata", "repos", repoName)

			scanner, err := NewHistoryScanner(repoPath)
			require.NoError(t, err)
			defer scanner.Close()

			commits, err := scanner.loadAllCommits()
			require.NoError(t, err)
			require.NotEmpty(t, commits)

			for _, commit := range commits {
				payload := gitCatFile(t, repoPath, "commit", commit.OID)

				var wantMessage []byte
				if i := bytes.Index(payload, []byte("\n\n")); i >= 0 {
					wantMessage = payload[i+2:]
				}

				meta, err := scanner.GetCommitMetadata(commit.OID)
				require.NoErrorf(t, err, "GetCommitMetadata %s", commit.OID)

				assert.Equalf(t, string(wantMessage), meta.Message,
					"message mismatch for commit %s", commit.OID)
				assert.NotEmptyf(t, meta.Author.Name, "author name for %s", commit.OID)
				assert.NotEmptyf(t, meta.Author.Email, "author email for %s", commit.OID)
			}
		})
	}
}

// TestCommitMetadataConcurrentDuplicateOIDs exercises the metaCache's
// double-parse-on-concurrent-miss tolerance against a real store: many
// goroutines request the same small OID set simultaneously (all missing at
// start), and every result must be identical. Run under -race, this covers
// the RLock/parse/Lock insert protocol with genuine concurrent misses.
func TestCommitMetadataConcurrentDuplicateOIDs(t *testing.T) {
	repoPath := filepath.Join("testdata", "repos", "large-repo")

	scanner, err := NewHistoryScanner(repoPath)
	require.NoError(t, err)
	defer scanner.Close()

	commits, err := scanner.loadAllCommits()
	require.NoError(t, err)
	require.NotEmpty(t, commits)

	// A small OID set maximizes duplicate-OID collisions across goroutines.
	oids := make([]Hash, 0, 8)
	for i, c := range commits {
		if i >= 8 {
			break
		}
		oids = append(oids, c.OID)
	}

	// Reference results, computed serially.
	want := make(map[Hash]CommitMetadata, len(oids))
	for _, oid := range oids {
		meta, err := scanner.GetCommitMetadata(oid)
		require.NoError(t, err)
		want[oid] = meta
	}

	// Reset the cache so every goroutine races the miss path.
	scanner.meta.mu.Lock()
	clear(scanner.meta.m)
	scanner.meta.mu.Unlock()

	const goroutines = 16
	const iterations = 200
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	// Every worker's first request is the same missing OID, released by one
	// close, so the duplicate-miss path runs with genuine overlap.
	start := make(chan struct{})
	for g := range goroutines {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			<-start
			for i := range iterations {
				oid := oids[(seed+i)%len(oids)]
				if i == 0 {
					oid = oids[0]
				}
				meta, err := scanner.GetCommitMetadata(oid)
				if err != nil {
					errs <- err
					return
				}
				if meta != want[oid] {
					errs <- assert.AnError
					return
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent GetCommitMetadata: %v", err)
	}
}

// TestHistoryScannerCloseReleasesMetaCache pins that Close drops the
// attribution cache along with the pair cache: a scanner value retained after
// Close holds no cached entries and no metadata slabs.
func TestHistoryScannerCloseReleasesMetaCache(t *testing.T) {
	scanner := createScannerForRepo(t, "large-repo")

	commits, err := scanner.loadAllCommits()
	require.NoError(t, err)
	require.NotEmpty(t, commits)
	_, err = scanner.GetCommitMetadata(commits[0].OID)
	require.NoError(t, err)
	require.NotEmpty(t, scanner.meta.m)
	require.NotZero(t, scanner.meta.slabBytes)

	require.NoError(t, scanner.Close())
	require.Empty(t, scanner.meta.m)
	require.Nil(t, scanner.meta.slab)
	require.Zero(t, scanner.meta.slabBytes)
}
