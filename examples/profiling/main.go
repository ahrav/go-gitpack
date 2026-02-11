// Package main demonstrates how to use profiling with go-gitpack to diagnose
// memory and CPU performance issues when scanning large repositories.
//
// The profiling server exposes pprof endpoints that can be accessed with:
//
//	curl http://localhost:6060/debug/pprof/heap > heap.prof
//	go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	objstore "github.com/ahrav/go-gitpack"
)

// findGitDir walks up the directory tree from startDir looking for a ".git"
// directory. It returns the full path to the first ".git" directory found, or
// an empty string if the filesystem root is reached without finding one.
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
	// Command-line flags for profiling configuration
	var (
		gitDir      = flag.String("repo", "", "Path to .git directory to scan")
		enableProf  = flag.Bool("profile", true, "Enable HTTP profiling server")
		profileAddr = flag.String("profile-addr", ":6060", "Address for profiling HTTP server")
		trace       = flag.Bool("trace", false, "Enable execution tracing")
		tracePath   = flag.String("trace-path", "./trace.out", "Path for trace output")
		maxHunks    = flag.Int("max-hunks", 0, "Maximum number of hunks to process (0 for all)")
	)
	flag.Parse()

	if *gitDir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			log.Fatalf("Failed to get current directory: %v", err)
		}
		*gitDir = findGitDir(cwd)
		fmt.Printf("No repository specified, using default: %s\n", *gitDir)
	}

	if _, err := os.Stat(*gitDir); err != nil {
		log.Fatalf("Git directory not found: %v", err)
	}

	fmt.Printf("Scanning repository: %s\n", *gitDir)
	fmt.Printf("Profiling server: %v (address: %s)\n", *enableProf, *profileAddr)
	fmt.Printf("Execution trace: %v\n", *trace)

	if *enableProf {
		fmt.Printf("\n📊 Profiling endpoints available at http://%s/debug/pprof/\n", *profileAddr)
		fmt.Println("Capture profiles during the scan with:")
		fmt.Printf("  CPU (30s):  curl http://%s/debug/pprof/profile?seconds=30 > cpu.prof\n", *profileAddr)
		fmt.Printf("  Heap:       curl http://%s/debug/pprof/heap > heap.prof\n", *profileAddr)
		fmt.Printf("  Goroutines: curl http://%s/debug/pprof/goroutine > goroutine.prof\n", *profileAddr)
		fmt.Printf("  Allocs:     curl http://%s/debug/pprof/allocs > allocs.prof\n", *profileAddr)
		fmt.Println()
	}

	// Create scanner with profiling configuration.
	// WithProfiling accepts a ProfilingConfig that controls the HTTP pprof
	// server and Go execution tracing. When EnableProfiling is true the
	// scanner starts an HTTP server on ProfileAddr exposing /debug/pprof/.
	// When Trace is true a runtime/trace file is written to TraceOutputPath.
	scanner, err := objstore.NewHistoryScanner(*gitDir,
		objstore.WithProfiling(&objstore.ProfilingConfig{
			EnableProfiling: *enableProf,
			ProfileAddr:     *profileAddr,
			Trace:           *trace,
			TraceOutputPath: *tracePath,
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create scanner: %v", err)
	}
	defer scanner.Close()

	startTime := time.Now()
	hunkCount := 0
	totalLines := 0
	commitSet := make(map[objstore.Hash]struct{})

	fmt.Println("\n🚀 Starting scan...")

	// DiffHistoryHunks returns two channels: hunks and a single error.
	// Drain hunks completely in a goroutine, then check the error channel.
	// This avoids a select race where the error channel becomes readable
	// before the hunk channel is closed (due to defer ordering inside
	// DiffHistoryHunks), which would cause a data race on the shared
	// counters and potentially incomplete statistics.
	hunks, errs := scanner.DiffHistoryHunks()

	go func() {
		for hunkAddition := range hunks {
			hunkCount++
			totalLines += len(hunkAddition.Lines())
			commitSet[hunkAddition.Commit()] = struct{}{}

			if hunkCount%1000 == 0 {
				elapsed := time.Since(startTime)
				rate := float64(hunkCount) / elapsed.Seconds()
				fmt.Printf("Progress: %d hunks, %d lines, %d commits, %.1f hunks/sec\n",
					hunkCount, totalLines, len(commitSet), rate)
			}

			if *maxHunks > 0 && hunkCount >= *maxHunks {
				fmt.Printf("\nReached max hunks limit (%d), stopping scan...\n", *maxHunks)
				return
			}
		}
	}()

	if err := <-errs; err != nil {
		log.Printf("Error during scan: %v", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("\nScan Statistics:\n")
	fmt.Printf("   Total hunks processed: %d\n", hunkCount)
	fmt.Printf("   Total lines analyzed: %d\n", totalLines)
	fmt.Printf("   Unique commits: %d\n", len(commitSet))
	fmt.Printf("   Time elapsed: %v\n", elapsed)
	fmt.Printf("   Processing rate: %.1f hunks/sec\n", float64(hunkCount)/elapsed.Seconds())

	if *trace {
		fmt.Printf("\n   Execution trace will be saved to: %s\n", *tracePath)
		fmt.Printf("   Analyze with: go tool trace %s\n", *tracePath)
	}
}
