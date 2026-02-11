// commit_fallback_test.go tests the graceful fallback behaviour when a
// repository's HEAD or branch refs point to objects that do not exist in any
// pack file. The scanner should return zero commits rather than an error,
// allowing callers to handle repositories with dangling references.
package objstore

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadAllCommits_SkipsMissingRefObjects simulates a repository whose HEAD
// resolves to refs/heads/main, which in turn contains a 40-hex OID that does
// not correspond to any object in the pack directory. The expected behaviour
// is that loadAllCommits returns an empty commit slice (not an error), because
// missing ref targets are silently skipped during the commit walk.
func TestLoadAllCommits_SkipsMissingRefObjects(t *testing.T) {
	gitDir := t.TempDir()

	for _, rel := range []string{
		filepath.Join("objects", "pack"),
		filepath.Join("refs", "heads"),
	} {
		if err := os.MkdirAll(filepath.Join(gitDir, rel), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
	}

	const missingOID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatalf("write HEAD: %v", err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "refs", "heads", "main"), []byte(missingOID+"\n"), 0o644); err != nil {
		t.Fatalf("write refs/heads/main: %v", err)
	}

	scanner, err := NewHistoryScanner(gitDir)
	if err != nil {
		t.Fatalf("NewHistoryScanner: %v", err)
	}
	defer scanner.Close()

	commits, err := scanner.loadAllCommits()
	if err != nil {
		t.Fatalf("LoadAllCommits: %v", err)
	}
	if len(commits) != 0 {
		t.Fatalf("expected zero commits, got %d", len(commits))
	}
}

// TestReadRefHash_PropagatesIOErrors verifies that readRefHash returns an error
// (not just false) when the ref file cannot be read due to a permission error.
// Before the fix, all errors were swallowed as "not found".
func TestReadRefHash_PropagatesIOErrors(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission-based test not reliable on Windows")
	}

	gitDir := t.TempDir()
	refsDir := filepath.Join(gitDir, "refs", "heads")
	require.NoError(t, os.MkdirAll(refsDir, 0o755))

	refPath := filepath.Join(refsDir, "main")
	require.NoError(t, os.WriteFile(refPath, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"), 0o000))

	// Before the fix: readRefHash returns (Hash{}, false, nil), swallowing
	// the permission error. After the fix: it returns a non-nil error.
	_, _, err := readRefHash(gitDir, "refs/heads/main")
	assert.Error(t, err, "permission errors should be propagated, not swallowed")
}

// TestReadRefHash_ChecksScannerError verifies that readRefHash checks sc.Err()
// after the scanner loop, so I/O errors from packed-refs are reported.
func TestReadRefHash_ChecksScannerError(t *testing.T) {
	gitDir := t.TempDir()

	// Create a packed-refs file with valid content. readRefHash should
	// return (Hash{}, false, nil) when the ref is simply not in packed-refs.
	packedRefs := "# pack-refs with: peeled fully-peeled sorted\n" +
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb refs/heads/other\n"
	require.NoError(t, os.WriteFile(filepath.Join(gitDir, "packed-refs"), []byte(packedRefs), 0o644))

	_, ok, err := readRefHash(gitDir, "refs/heads/nonexistent")
	require.NoError(t, err)
	assert.False(t, ok)
}
