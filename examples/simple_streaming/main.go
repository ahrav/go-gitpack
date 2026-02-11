// Package main demonstrates simple streaming blob scanning using the go-gitpack
// library. It locates the nearest .git directory, opens a HistoryScanner, and
// scans every unique blob reachable from the commit history, printing a rolling
// summary as blobs are processed.
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

// findGitDir traverses the directory tree upwards from startDir to locate the ".git" directory.
// It returns the path to the ".git" directory if found, otherwise an empty string.
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

// summaryScanner implements objstore.BlobScanner by draining each blob and
// printing periodic progress. ScanBlob is called concurrently from multiple
// decode workers, so the counters use atomic operations.
type summaryScanner struct {
	blobs      atomic.Int64
	totalBytes atomic.Int64
}

func (s *summaryScanner) ScanBlob(r io.Reader, meta objstore.ScanMeta) error {
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		return err
	}
	s.totalBytes.Add(n)
	count := s.blobs.Add(1)

	if count%50 == 0 || count <= 10 {
		fmt.Printf("[%d] blob %s  commit %s  path %s  (%d bytes)\n",
			count,
			meta.Blob.String()[:8],
			meta.Commit.String()[:8],
			meta.Path,
			n)
	}
	return nil
}

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current directory: %v", err)
	}

	gitDir := findGitDir(cwd)
	if gitDir == "" {
		log.Fatal("Could not find .git directory. Please run from within the go-gitpack repository.")
	}

	fmt.Printf("Found .git directory at: %s\n", gitDir)

	scanner, err := objstore.NewHistoryScanner(gitDir)
	if err != nil {
		log.Fatalf("Failed to open repository: %v", err)
	}
	defer scanner.Close()

	fmt.Println("Scanning all unique blobs from repository history...")

	s := &summaryScanner{}
	if err := scanner.Scan(nil, s); err != nil {
		log.Fatalf("Error during scan: %v", err)
	}

	fmt.Printf("\nScan completed. Total blobs: %d, total bytes: %d\n",
		s.blobs.Load(), s.totalBytes.Load())
}
