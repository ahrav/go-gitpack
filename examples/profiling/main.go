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

	hunkAdditions, errors := scanner.DiffHistoryHunks()

	done := make(chan bool)
	go func() {
		defer close(done)
		for hunkAddition := range hunkAdditions {
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

	select {
	case err := <-errors:
		if err != nil {
			log.Printf("Error during scan: %v", err)
		}
	case <-done:
		fmt.Println("\n✅ Scan completed successfully!")
	}

	elapsed := time.Since(startTime)
	fmt.Printf("\n📊 Scan Statistics:\n")
	fmt.Printf("   Total hunks processed: %d\n", hunkCount)
	fmt.Printf("   Total lines analyzed: %d\n", totalLines)
	fmt.Printf("   Unique commits: %d\n", len(commitSet))
	fmt.Printf("   Time elapsed: %v\n", elapsed)
	fmt.Printf("   Processing rate: %.1f hunks/sec\n", float64(hunkCount)/elapsed.Seconds())

	if *enableProf {
		fmt.Printf("\n📈 Profile Analysis:\n")
		fmt.Println("While the scan is running, capture profiles from another terminal:")
		fmt.Printf("   # CPU profile (30 seconds):\n")
		fmt.Printf("   curl http://%s/debug/pprof/profile?seconds=30 > cpu.prof\n", *profileAddr)
		fmt.Printf("   go tool pprof cpu.prof\n\n")

		fmt.Printf("   # Memory profile:\n")
		fmt.Printf("   curl http://%s/debug/pprof/heap > heap.prof\n", *profileAddr)
		fmt.Printf("   go tool pprof heap.prof\n\n")

		fmt.Printf("   # Live profiling:\n")
		fmt.Printf("   go tool pprof http://%s/debug/pprof/heap\n", *profileAddr)
		fmt.Printf("   go tool pprof http://%s/debug/pprof/profile?seconds=30\n\n", *profileAddr)

		fmt.Printf("💡 Common pprof commands:\n")
		fmt.Printf("   top10          - Show top 10 functions by CPU/memory\n")
		fmt.Printf("   list <func>    - Show source code for a function\n")
		fmt.Printf("   web            - Open interactive graph in browser\n")
		fmt.Printf("   png > out.png  - Save graph as image\n")
	}

	if *trace {
		fmt.Printf("\n   Execution trace will be saved to: %s\n", *tracePath)
		fmt.Printf("   Analyze with: go tool trace %s\n", *tracePath)
	}
}
