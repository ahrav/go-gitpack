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

func TestDiffHistoryHunks_AdditionIntegrity(t *testing.T) {
	// Keep it small for focused testing
	repo := "simple-linear"
	repoPath := filepath.Join("testdata", "repos", repo)
	scanner := createScannerForRepo(t, repo)
	defer scanner.Close()

	hunks, errC := scanner.DiffHistoryHunks()

	// Group by commit+path => []*HunkAddition for easier post‑processing
	type key struct{ commit, path string }
	group := make(map[key][]HunkAddition)

	for h := range hunks {
		k := key{commit: h.commit.String(), path: h.Path()}
		group[k] = append(group[k], h)
	}
	require.NoError(t, <-errC)

	for k, hunkList := range group {
		// Fetch authoritative file snapshot via `git show <commit>:<path>`
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
			// Validate hunk properties
			assert.Greater(t, hunk.StartLine(), 0, "StartLine should be positive")
			assert.GreaterOrEqual(t, hunk.EndLine(), hunk.StartLine(), "EndLine should be >= StartLine")
			assert.NotEmpty(t, hunk.Lines(), "Hunk should have at least one line")

			// Verify each line in the hunk matches the authoritative file
			for i, hunkLine := range hunk.Lines() {
				lineNo := hunk.StartLine() + i
				want, ok := lines[lineNo]
				assert.True(t, ok, "commit %s file %s: claimed line %d does not exist in hunk starting at %d",
					k.commit[:8], k.path, lineNo, hunk.StartLine())
				assert.Equal(t, want, hunkLine, "commit %s file %s line %d: text mismatch in hunk starting at %d",
					k.commit[:8], k.path, lineNo, hunk.StartLine())
			}

			// Verify EndLine calculation
			expectedEndLine := hunk.StartLine() + len(hunk.Lines()) - 1
			assert.Equal(t, expectedEndLine, hunk.EndLine(),
				"EndLine calculation incorrect for hunk in %s:%s starting at line %d",
				k.commit[:8], k.path, hunk.StartLine())
		}
	}
}
