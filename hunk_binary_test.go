package objstore

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanHunks_BinaryPayloadIsPassedIntact(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repo := t.TempDir()
	want := []byte("PK\x03\x04\x00binary\x00payload")
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "payload.zip"), want, 0o644))
	runGit(t, repo, "add", "payload.zip")
	runGit(t, repo, "commit", "-m", "archive", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"), WithScanMode(ScanModeHunks))
	require.NoError(t, err)
	defer scanner.Close()

	rec := &capturingBlobScanner{}
	require.NoError(t, scanner.Scan(nil, rec))
	require.Len(t, rec.items, 1)
	assert.Equal(t, "payload.zip", rec.items[0].meta.Path)
	assert.True(t, bytes.Equal(want, rec.items[0].data), "binary hunk payload changed")
}

func runGit(t *testing.T, repo string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=t",
		"GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t",
		"GIT_COMMITTER_EMAIL=t@example.com",
	)
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "git %v failed: %s", args, string(out))
}

type capturingBlobScanner struct {
	items []capturedBlob
}

type capturedBlob struct {
	meta ScanMeta
	data []byte
}

func (s *capturingBlobScanner) ScanBlob(r io.Reader, meta ScanMeta) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.items = append(s.items, capturedBlob{
		meta: meta,
		data: append([]byte(nil), data...),
	})
	return nil
}
