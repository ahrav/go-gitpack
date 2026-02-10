// commit_fallback_test.go tests the graceful fallback behaviour when a
// repository's HEAD or branch refs point to objects that do not exist in any
// pack file. The scanner should return zero commits rather than an error,
// allowing callers to handle repositories with dangling references.
package objstore

import (
	"os"
	"path/filepath"
	"testing"
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
