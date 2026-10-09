// Package main demonstrates streaming blob scanning from a Git repository
// using the go-gitpack library. It opens the nearest .git directory, creates a
// HistoryScanner, and scans every unique blob reachable from the commit history,
// printing detailed metadata for each blob.
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	objstore "github.com/ahrav/go-gitpack"
)

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current directory: %v", err)
	}

	gitDir := findGitDir(cwd)
	if gitDir == "" {
		log.Fatal("Could not find .git directory. Please run from within the go-gitpack repository.")
	}

	fmt.Printf("Opening Git repository at: %s\n", gitDir)

	scanner, err := objstore.NewHistoryScanner(gitDir)
	if err != nil {
		log.Fatalf("Failed to create history scanner: %v", err)
	}
	defer scanner.Close()

	fmt.Println("Scanning all unique blobs from repository history...")

	s := &detailedScanner{scanner: scanner}
	if err := scanner.Scan(nil, s); err != nil {
		log.Fatalf("Error during scan: %v", err)
	}

	fmt.Printf("\nProcessed %d blobs (%d bytes total)\n",
		s.blobs.Load(), s.totalBytes.Load())
}

// detailedScanner implements objstore.BlobScanner by draining each blob and
// printing per-blob metadata plus the author and title of meta.Commit, the
// commit Scan selected for the blob (the first commit in its ordered walk
// whose first-parent diff added the blob). ScanBlob is called concurrently
// from multiple decode workers, so counters use atomic operations, each
// record is written in one call under out, and GetCommitMetadata is safe for
// concurrent use and caches per-commit results internally.
type detailedScanner struct {
	scanner    *objstore.HistoryScanner
	blobs      atomic.Int64
	totalBytes atomic.Int64
	out        sync.Mutex
}

func (s *detailedScanner) ScanBlob(r io.Reader, meta objstore.ScanMeta) error {
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		return err
	}
	s.totalBytes.Add(n)
	count := s.blobs.Add(1)

	cm, err := s.scanner.GetCommitMetadata(meta.Commit)
	if err != nil {
		return fmt.Errorf("commit metadata for %s: %w", meta.Commit, err)
	}
	title, _, _ := strings.Cut(cm.Message, "\n")

	var rec strings.Builder
	fmt.Fprintf(&rec, "Blob #%d:\n", count)
	fmt.Fprintf(&rec, "  OID:    %s\n", meta.Blob)
	fmt.Fprintf(&rec, "  Commit: %s\n", meta.Commit)
	fmt.Fprintf(&rec, "  Path:   %s\n", meta.Path)
	fmt.Fprintf(&rec, "  Size:   %d bytes\n", n)
	fmt.Fprintf(&rec, "  Author: %s <%s>\n", cm.Author.Name, cm.Author.Email)
	fmt.Fprintf(&rec, "  Title:  %s\n", title)
	rec.WriteString("  ---\n")

	s.out.Lock()
	defer s.out.Unlock()
	_, err = os.Stdout.WriteString(rec.String())
	return err
}

// findGitDir walks up the directory tree to find a .git directory.
func findGitDir(startDir string) string {
	dir := startDir
	for {
		gitPath := filepath.Join(dir, ".git")
		if info, err := os.Stat(gitPath); err == nil && info.IsDir() {
			return gitPath
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}
