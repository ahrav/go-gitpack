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
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
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
			break
		}
		dir = parent
	}
	return ""
}

func main() {
	var (
		gitDir      = flag.String("repo", "", "Path to .git directory to scan")
		enableProf  = flag.Bool("profile", true, "Enable HTTP profiling server")
		profileAddr = flag.String("profile-addr", ":6060", "Address for profiling HTTP server")
		trace       = flag.Bool("trace", false, "Enable execution tracing")
		tracePath   = flag.String("trace-path", "./trace.out", "Path for trace output")
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
		fmt.Printf("\nProfiling endpoints available at http://%s/debug/pprof/\n", *profileAddr)
		fmt.Println("Capture profiles during the scan with:")
		fmt.Printf("  CPU (30s):  curl http://%s/debug/pprof/profile?seconds=30 > cpu.prof\n", *profileAddr)
		fmt.Printf("  Heap:       curl http://%s/debug/pprof/heap > heap.prof\n", *profileAddr)
		fmt.Printf("  Goroutines: curl http://%s/debug/pprof/goroutine > goroutine.prof\n", *profileAddr)
		fmt.Printf("  Allocs:     curl http://%s/debug/pprof/allocs > allocs.prof\n", *profileAddr)
		fmt.Println()
	}

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
	fmt.Println("\nStarting blob scan...")

	s := &profilingScanner{startTime: startTime}
	if err := scanner.Scan(nil, s); err != nil {
		log.Printf("Error during scan: %v", err)
	}

	elapsed := time.Since(startTime)
	blobs := s.blobs.Load()
	totalBytes := s.totalBytes.Load()
	fmt.Printf("\nScan Statistics:\n")
	fmt.Printf("   Total blobs scanned:  %d\n", blobs)
	fmt.Printf("   Total bytes scanned:  %d\n", totalBytes)
	fmt.Printf("   Time elapsed:         %v\n", elapsed)
	if elapsed.Seconds() > 0 {
		fmt.Printf("   Processing rate:      %.1f blobs/sec\n", float64(blobs)/elapsed.Seconds())
		fmt.Printf("   Throughput:           %.1f MB/sec\n", float64(totalBytes)/elapsed.Seconds()/1024/1024)
	}

	if *trace {
		fmt.Printf("\n   Execution trace saved to: %s\n", *tracePath)
		fmt.Printf("   Analyze with: go tool trace %s\n", *tracePath)
	}
}

// profilingScanner implements objstore.BlobScanner, printing periodic progress
// during long-running scans. ScanBlob is called concurrently from multiple
// decode workers, so counters use atomic operations.
type profilingScanner struct {
	blobs      atomic.Int64
	totalBytes atomic.Int64
	startTime  time.Time
}

func (s *profilingScanner) ScanBlob(r io.Reader, _ objstore.ScanMeta) error {
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		return err
	}
	s.totalBytes.Add(n)
	count := s.blobs.Add(1)

	if count%1000 == 0 {
		elapsed := time.Since(s.startTime)
		rate := float64(count) / elapsed.Seconds()
		fmt.Printf("Progress: %d blobs, %.1f blobs/sec\n", count, rate)
	}
	return nil
}
