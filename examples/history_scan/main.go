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

	// DiffHistoryHunks returns streaming channels for additions and errors.
	hunkAdditions, errors := scanner.DiffHistoryHunks()

	hunkCount := 0
	// const maxHunks = 50 // Limit output for demo purposes.
	const maxHunks = math.MaxInt // Limit output for demo purposes.

	// Launch a goroutine to process hunk additions from the stream.
	done := make(chan bool)
	go func() {
		defer close(done)
		for hunkAddition := range hunkAdditions {
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
				content := string(lines[i])
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

	// Wait for either the completion of hunk processing or an error.
	select {
	case err := <-errors:
		if err != nil {
			log.Fatalf("Error during history scan: %v", err)
		}
		fmt.Println("✅ History scan completed successfully!")
	case <-done:
		fmt.Printf("✅ Processed %d hunks\n", hunkCount)
	}

	fmt.Println("\n--- Additional Scanner Capabilities ---")

	// LoadAllCommits demonstrates loading all commits.
	commits, err := scanner.LoadAllCommits()
	if err != nil {
		log.Printf("Failed to load commits: %v", err)
	} else {
		fmt.Printf("📊 Total commits in repository: %d\n", len(commits))

		showCount := min(5, len(commits))
		fmt.Printf("📝 First %d commits:\n", showCount)
		for i := range showCount {
			commit := commits[i]
			metadata, err := scanner.GetCommitMetadata(commit.OID)
			if err != nil {
				log.Printf("Failed to get commit metadata: %v", err)
				continue
			}

			fmt.Printf("  %s (tree: %s, parents: %d, time: %d)\n",
				commit.OID, commit.TreeOID, len(commit.ParentOIDs), metadata.Timestamp)
		}
	}
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
