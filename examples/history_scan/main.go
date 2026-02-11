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

	s := &detailedScanner{}
	if err := scanner.Scan(nil, s); err != nil {
		log.Fatalf("Error during scan: %v", err)
	}

	fmt.Printf("\nProcessed %d blobs (%d bytes total)\n",
		s.blobs.Load(), s.totalBytes.Load())
}

// detailedScanner implements objstore.BlobScanner by draining each blob and
// printing per-blob metadata. ScanBlob is called concurrently from multiple
// decode workers, so counters use atomic operations.
type detailedScanner struct {
	blobs      atomic.Int64
	totalBytes atomic.Int64
}

func (s *detailedScanner) ScanBlob(r io.Reader, meta objstore.ScanMeta) error {
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		return err
	}
	s.totalBytes.Add(n)
	count := s.blobs.Add(1)

	fmt.Printf("Blob #%d:\n", count)
	fmt.Printf("  OID:    %s\n", meta.Blob)
	fmt.Printf("  Commit: %s\n", meta.Commit)
	fmt.Printf("  Path:   %s\n", meta.Path)
	fmt.Printf("  Size:   %d bytes\n", n)
	fmt.Println("  ---")

	return nil
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
