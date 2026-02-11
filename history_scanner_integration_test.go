// history_scanner_integration_test.go is an oracle-based integration test that
// validates DiffHistoryHunks output against `git show <commit>:<path>`. For
// each hunk emitted by the scanner, the test fetches the authoritative file
// snapshot from Git and asserts that every hunk line matches the corresponding
// line in the real file. This ensures the diff and history-walking pipeline
// produces byte-accurate results for a known test repository.

package objstore

import (
	"bufio"
	"bytes"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiffHistoryHunks_AdditionIntegrity walks the full commit history of the
// "simple-linear" test repository via DiffHistoryHunks, then cross-checks
// every emitted hunk against `git show <commit>:<path>`.
//
// Strategy:
//  1. Scan all diff hunks from the history scanner.
//  2. Group hunks by (commit, path).
//  3. For each group, invoke `git show` to get the authoritative file snapshot.
//  4. Assert every hunk line matches the corresponding line number in the file.
//  5. Verify EndLine = StartLine + len(Lines) - 1.
//
// Prerequisites:
//   - The testdata/repos/simple-linear repository must exist and be a valid
//     Git repository. It is typically created by a test-fixture setup script.
//   - The `git` CLI must be available on PATH.
func TestDiffHistoryHunks_AdditionIntegrity(t *testing.T) {
	repo := "simple-linear"
	repoPath := filepath.Join("testdata", "repos", repo)
	scanner := createScannerForRepo(t, repo)
	defer scanner.Close()

	hunks, errC := scanner.DiffHistoryHunks()

	// Group by commit+path => []*HunkAddition for easier post‑processing.
	type key struct{ commit, path string }
	group := make(map[key][]HunkAddition)

	for h := range hunks {
		k := key{commit: h.commit.String(), path: h.Path()}
		group[k] = append(group[k], h)
	}
	require.NoError(t, <-errC)

	for k, hunkList := range group {
		// Fetch authoritative file snapshot via `git show <commit>:<path>`.
		out, err := exec.Command("git", "-C", repoPath,
			"show", k.commit+":"+k.path).Output()
		require.NoErrorf(t, err, "git show failed for %s:%s", k.commit[:8], k.path)

		// Build a map lineNo -> lineText for constant‑time lookup.
		lines := map[int][]byte{}
		scanner := bufio.NewScanner(bytes.NewReader(out))
		for i := 1; scanner.Scan(); i++ {
			// NOTE: Scanner strips trailing '\n'; matches hunk line semantics.
			lines[i] = append([]byte(nil), scanner.Bytes()...)
		}
		require.NoError(t, scanner.Err())

		for _, hunk := range hunkList {
			// Validate hunk properties.
			assert.Greater(t, hunk.StartLine(), 0, "StartLine should be positive")
			assert.GreaterOrEqual(t, hunk.EndLine(), hunk.StartLine(), "EndLine should be >= StartLine")
			assert.NotEmpty(t, hunk.Lines(), "Hunk should have at least one line")

			// Verify each line in the hunk matches the authoritative file.
			for i, hunkLine := range hunk.Lines() {
				lineNo := hunk.StartLine() + i
				want, ok := lines[lineNo]
				assert.True(t, ok, "commit %s file %s: claimed line %d does not exist in hunk starting at %d",
					k.commit[:8], k.path, lineNo, hunk.StartLine())
				assert.Equal(t, want, []byte(hunkLine), "commit %s file %s line %d: text mismatch in hunk starting at %d",
					k.commit[:8], k.path, lineNo, hunk.StartLine())
			}

			// Verify EndLine calculation.
			expectedEndLine := hunk.StartLine() + len(hunk.Lines()) - 1
			assert.Equal(t, expectedEndLine, hunk.EndLine(),
				"EndLine calculation incorrect for hunk in %s:%s starting at line %d",
				k.commit[:8], k.path, hunk.StartLine())
		}
	}
}
