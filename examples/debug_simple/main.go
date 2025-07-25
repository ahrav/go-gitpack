package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	objstore "github.com/ahrav/go-gitpack"
)

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

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current directory: %v", err)
	}
	fmt.Printf("Current working directory: %s\n", cwd)

	gitDir := findGitDir(cwd)
	if gitDir == "" {
		log.Fatal("Could not find .git directory. Please run from within the go-gitpack repository.")
	}

	fmt.Printf("Found .git directory at: %s\n", gitDir)

	commitGraphPath := filepath.Join(gitDir, "objects", "info", "commit-graph")
	fmt.Printf("Looking for commit-graph at: %s\n", commitGraphPath)

	if info, err := os.Stat(commitGraphPath); err != nil {
		fmt.Printf("Commit-graph file check failed: %v\n", err)
	} else {
		fmt.Printf("Commit-graph file found: %d bytes, mode: %s\n", info.Size(), info.Mode())
	}

	objectsDir := filepath.Join(gitDir, "objects")
	fmt.Printf("Objects directory: %s\n", objectsDir)
	if info, err := os.Stat(objectsDir); err != nil {
		fmt.Printf("Objects directory check failed: %v\n", err)
	} else {
		fmt.Printf("Objects directory found, mode: %s\n", info.Mode())
	}

	fmt.Println("Attempting to create HistoryScanner...")
	scanner, err := objstore.NewHistoryScanner(gitDir)
	if err != nil {
		fmt.Printf("Failed to create scanner: %v\n", err)
		fmt.Printf("Error type: %T\n", err)

		if err == objstore.ErrCommitGraphRequired {
			fmt.Println("Error is ErrCommitGraphRequired")
		}

		log.Fatalf("Cannot proceed: %v", err)
	}
	defer scanner.Close()

	fmt.Println("✅ Scanner created successfully!")

	commits, err := scanner.LoadAllCommits()
	if err != nil {
		log.Fatalf("Failed to load commits: %v", err)
	}

	fmt.Printf("✅ Successfully loaded %d commits\n", len(commits))
}
