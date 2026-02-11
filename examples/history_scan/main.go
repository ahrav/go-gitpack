// Package main demonstrates streaming commit-history hunk additions from a
// Git repository using the go-gitpack library. It opens the nearest .git
// directory, calls DiffHistoryHunks to stream per-commit file additions, and
// prints each hunk with its commit, path, line range, and a content preview.
package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"

	objstore "github.com/ahrav/go-gitpack"
)

func main() {
	// Get the current working directory.
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current directory: %v", err)
	}

	// Find the .git directory by traversing up the directory tree.
	gitDir := findGitDir(cwd)
	if gitDir == "" {
		log.Fatal("Could not find .git directory. Please run from within the go-gitpack repository.")
	}

	fmt.Printf("Opening Git repository at: %s\n", gitDir)

	// Create a new HistoryScanner to scan the git history.
	scanner, err := objstore.NewHistoryScanner(gitDir)
	if err != nil {
		log.Fatalf("Failed to create history scanner: %v", err)
	}
	defer scanner.Close()

	fmt.Println("Starting to stream commit history hunks...")
	fmt.Println("Press Ctrl+C to stop")

	// DiffHistoryHunks returns two channels: a stream of hunk additions and
	// an error channel. Drain hunks completely in a goroutine, then check the
	// error channel. This avoids a select race where the error channel becomes
	// readable before the hunk channel is closed.
	hunks, errs := scanner.DiffHistoryHunks()

	hunkCount := 0
	// maxHunks controls how many hunks to process before stopping.
	// Set to a small value (e.g. 50) for demo/testing purposes, or
	// math.MaxInt to process the entire history.
	const maxHunks = math.MaxInt

	go func() {
		for hunkAddition := range hunks {
			if hunkCount >= maxHunks {
				fmt.Printf("... (stopping after %d hunks for demo purposes)\n", maxHunks)
				return
			}

			hunkCount++
			fmt.Printf("Hunk #%d:\n", hunkCount)
			fmt.Printf("  Commit:     %s\n", hunkAddition.Commit())
			fmt.Printf("  File:       %s\n", hunkAddition.Path())
			fmt.Printf("  Start Line: %d\n", hunkAddition.StartLine())
			fmt.Printf("  End Line:   %d\n", hunkAddition.EndLine())
			fmt.Printf("  Lines in hunk: %d\n", len(hunkAddition.Lines()))

			lines := hunkAddition.Lines()
			showLines := min(3, len(lines))
			fmt.Printf("  Content (first %d lines):\n", showLines)
			for i := range showLines {
				content := lines[i]
				if len(content) > 80 {
					content = content[:77] + "..."
				}
				fmt.Printf("    Line %d: %s\n", hunkAddition.StartLine()+i, content)
			}
			if len(lines) > showLines {
				fmt.Printf("    ... and %d more lines\n", len(lines)-showLines)
			}
			fmt.Println("  ---")

			if hunkCount >= maxHunks {
				return
			}
		}
	}()

	if err := <-errs; err != nil {
		log.Fatalf("Error during history scan: %v", err)
	}
	fmt.Printf("Processed %d hunks\n", hunkCount)
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
			// Reached filesystem root
			break
		}
		dir = parent
	}
	return ""
}
