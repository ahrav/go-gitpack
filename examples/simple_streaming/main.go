package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

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
			// Reached filesystem root
			break
		}
		dir = parent
	}
	return ""
}

// main is the entry point of the simple streaming example.
// It demonstrates how to use the go-gitpack library to stream commit hunks from a Git repository.
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

	fmt.Println("🚀 Streaming commit hunks from repository history...")

	hunkAdditions, errors := scanner.DiffHistoryHunks()

	count := 0
	totalLines := 0
	for {
		select {
		case hunkAddition, ok := <-hunkAdditions:
			if !ok {
				fmt.Printf("\n✅ Finished! Processed %d hunks with %d total lines.\n", count, totalLines)
				return
			}

			count++
			hunkLineCount := len(hunkAddition.Lines())
			totalLines += hunkLineCount
			if count%50 == 0 || count <= 10 {
				fmt.Printf("[%d] %s added hunk in %s (lines %d-%d, %d lines)\n",
					count,
					hunkAddition.Commit().String()[:8],
					hunkAddition.Path(),
					hunkAddition.StartLine(),
					hunkAddition.EndLine(),
					hunkLineCount)

				if count <= 5 {
					lines := hunkAddition.Lines()

					showLines := min(2, len(lines))
					for i := 0; i < showLines; i++ {
						content := string(lines[i])
						if len(content) > 80 {
							content = content[:77] + "..."
						}
						fmt.Printf("     Line %d: %s\n", hunkAddition.StartLine()+i, content)
					}
					if len(lines) > showLines {
						fmt.Printf("     ... and %d more lines\n", len(lines)-showLines)
					}
				}
			}

		case err := <-errors:
			if err != nil {
				log.Fatalf("❌ Error during streaming: %v", err)
			}
			fmt.Printf("\n✅ Streaming completed successfully! Total hunks: %d, total lines: %d\n", count, totalLines)
			return
		}
	}
}
