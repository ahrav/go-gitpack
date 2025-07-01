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

func TestDiffHistory_AdditionIntegrity(t *testing.T) {
	// Keep it small so we don't blow up CI time; the 3‑commit repo is perfect.
	repo := "simple-linear"
	repoPath := filepath.Join("testdata", "repos", repo)
	scanner := createScannerForRepo(t, repo)
	defer scanner.Close()

	additions, errC := scanner.DiffHistory()

	// Group by commit+path => []*Addition for easier post‑processing
	type key struct{ commit, path string }
	group := make(map[key][]Addition)

	for a := range additions {
		k := key{commit: a.commit.String(), path: a.Path()}
		group[k] = append(group[k], a)
	}
	require.NoError(t, <-errC)

	for k, adds := range group {
		// Fetch authoritative file snapshot via `git show <commit>:<path>`
		out, err := exec.Command("git", "-C", repoPath,
			"show", k.commit+":"+k.path).Output()
		require.NoErrorf(t, err, "git show failed for %s:%s", k.commit[:8], k.path)

		// Build a map lineNo -> lineText for constant‑time lookup.
		lines := map[int][]byte{}
		scanner := bufio.NewScanner(bytes.NewReader(out))
		for i := 1; scanner.Scan(); i++ {
			// NOTE: Scanner strips trailing '\n'; matches AddedLine.Text semantics.
			lines[i] = append([]byte(nil), scanner.Bytes()...)
		}
		require.NoError(t, scanner.Err())

		for _, a := range adds {
			want, ok := lines[a.Line()]
			assert.True(t, ok, "commit %s file %s: claimed line %d does not exist",
				k.commit[:8], k.path, a.Line())
			assert.Equal(t, want, a.Text(), "commit %s file %s line %d: text mismatch",
				k.commit[:8], k.path, a.Line())
		}
	}
}
