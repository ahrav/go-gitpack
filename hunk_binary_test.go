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

func TestRunGitIgnoresHostileGlobalCommitSigning(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	home := t.TempDir()
	missingGPG := filepath.Join(home, "missing-gpg")
	globalConfig := []byte("[commit]\n\tgpgsign = true\n[gpg]\n\tprogram = " + missingGPG + "\n")
	require.NoError(t, os.WriteFile(filepath.Join(home, ".gitconfig"), globalConfig, 0o644))
	hostileEnv := []string{"HOME=" + home, "XDG_CONFIG_HOME=" + home}

	repo := t.TempDir()
	runGitEnv(t, repo, hostileEnv, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "file.txt"), []byte("content\n"), 0o644))
	runGitEnv(t, repo, hostileEnv, "add", "file.txt")
	runGitEnv(t, repo, hostileEnv, "commit", "-m", "must not sign", "--quiet")
}

func runGit(t *testing.T, repo string, args ...string) {
	t.Helper()
	runGitEnv(t, repo, nil, args...)
}

func runGitEnv(t *testing.T, repo string, extraEnv []string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	cmd.Env = append(gitFixtureEnv(), extraEnv...)
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
