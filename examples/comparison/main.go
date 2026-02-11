// Package main compares go-gitpack's blob scanning against the git log
// commands used by gitleaks and trufflehog for full-history scanning.
//
// Usage:
//
//	go run examples/comparison/main.go /path/to/.git [/path/to/.git ...]
//
// If no arguments are given, it scans all .git directories one level up from cwd.
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	objstore "github.com/ahrav/go-gitpack"
)

type gitResult struct {
	label string
	dur   time.Duration
	bytes int64
}

type result struct {
	name    string
	commits int
	git     []gitResult

	gpDur   time.Duration
	gpBlobs int64
	gpBytes int64
	gpErr   error
}

func main() {
	repos := os.Args[1:]
	if len(repos) == 0 {
		repos = discoverRepos()
	}
	if len(repos) == 0 {
		log.Fatal("No repositories found. Pass .git paths as arguments or run from a directory with sibling repos.")
	}

	var results []result
	for _, gitDir := range repos {
		fmt.Fprintf(os.Stderr, "benchmarking %s ...\n", repoName(gitDir))
		results = append(results, bench(gitDir))
	}

	printReport(results)
}

func printReport(results []result) {
	line := strings.Repeat("─", 68)
	dblLine := strings.Repeat("═", 68)

	fmt.Println()
	fmt.Println(dblLine)
	fmt.Println("  go-gitpack vs git log  ·  Full-History Scan Comparison")
	fmt.Println(dblLine)
	fmt.Println()
	printMachineInfo()
	fmt.Println()
	fmt.Println("  How each method works:")
	fmt.Println("    gitleaks    git log -p -U0  → diff hunks (changed lines only)")
	fmt.Println("    trufflehog  git log -p      → diff patches (changes + context)")
	fmt.Println("    go-gitpack  HistoryScanner  → every unique full blob via mmap")

	for _, r := range results {
		fmt.Println()
		fmt.Println(line)
		fmt.Printf("  %s", r.name)
		if r.commits > 0 {
			fmt.Printf("  (%s commits)", fmtCount(r.commits))
		}
		fmt.Println()
		fmt.Println(line)

		if r.gpErr != nil {
			fmt.Printf("\n  ERROR: %v\n", r.gpErr)
			continue
		}

		fmt.Println()

		// Timing + data table.
		fmt.Printf("  %-22s %10s    %10s streamed\n",
			"gitleaks git log", fmtDur(r.git[0].dur), fmtBytes(r.git[0].bytes))
		fmt.Printf("  %-22s %10s    %10s streamed\n",
			"trufflehog git log", fmtDur(r.git[1].dur), fmtBytes(r.git[1].bytes))
		fmt.Printf("  %-22s %10s    %10s scanned  (%s blobs)\n",
			"go-gitpack", fmtDur(r.gpDur), fmtBytes(r.gpBytes), fmtCount(int(r.gpBlobs)))

		// Speedup summary.
		fmt.Println()
		if r.git[0].dur > 0 {
			glSpeedup := float64(r.git[0].dur) / float64(r.gpDur)
			fmt.Printf("  → %.1fx faster than gitleaks\n", glSpeedup)
		}
		if r.git[1].dur > 0 {
			thSpeedup := float64(r.git[1].dur) / float64(r.gpDur)
			fmt.Printf("  → %.1fx faster than trufflehog\n", thSpeedup)
		}

		// Data comparison (only if git commands produced output to compare against).
		if r.git[0].bytes > 0 && r.gpBytes > r.git[0].bytes {
			dataRatio := float64(r.gpBytes) / float64(r.git[0].bytes)
			fmt.Printf("\n  Note: go-gitpack read %.1fx MORE data (full blobs vs diff hunks)\n", dataRatio)
			fmt.Println("  yet still finished faster.")
		}

		// Throughput.
		if r.gpDur > 0 && r.gpBytes > 0 {
			throughput := float64(r.gpBytes) / r.gpDur.Seconds()
			fmt.Printf("\n  Throughput: %s/s\n", fmtBytes(int64(throughput)))
		}
	}

	fmt.Println()
	fmt.Println(dblLine)
}

func printMachineInfo() {
	fmt.Println("## Environment")
	fmt.Println()

	cpu := trimCmd("sysctl", "-n", "machdep.cpu.brand_string")
	mem := trimCmd("sysctl", "-n", "hw.memsize")
	cores := trimCmd("sysctl", "-n", "hw.ncpu")
	os := trimCmd("uname", "-mrs")
	gitVer := trimCmd("git", "--version")
	goVer := trimCmd("go", "version")

	var memGB string
	var memBytes int64
	fmt.Sscanf(mem, "%d", &memBytes)
	if memBytes > 0 {
		memGB = fmt.Sprintf("%.0f GB", float64(memBytes)/1024/1024/1024)
	}

	fmt.Printf("- **CPU:** %s\n", cpu)
	fmt.Printf("- **RAM:** %s\n", memGB)
	fmt.Printf("- **Cores:** %s\n", cores)
	fmt.Printf("- **OS:** %s\n", os)
	fmt.Printf("- **Git:** %s\n", gitVer)
	fmt.Printf("- **Go:** %s\n", goVer)
}

func trimCmd(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func gitCommands(repoDir string) []struct {
	label string
	args  []string
} {
	return []struct {
		label string
		args  []string
	}{
		{
			label: "gitleaks",
			args:  []string{"-C", repoDir, "log", "-p", "-U0", "--full-history", "--all", "--diff-filter=tuxdb"},
		},
		{
			label: "trufflehog",
			args:  []string{"-C", repoDir, "log", "-p", "--full-history", "--all", "--date=iso-strict", "--pretty=fuller", "--notes"},
		},
	}
}

func bench(gitDir string) result {
	repoDir := filepath.Dir(gitDir)
	r := result{name: repoName(gitDir)}

	r.commits = commitCount(repoDir)

	for _, cmd := range gitCommands(repoDir) {
		b, dur := runGit(cmd.args)
		r.git = append(r.git, gitResult{label: cmd.label, dur: dur, bytes: b})
	}

	r.gpBlobs, r.gpBytes, r.gpDur, r.gpErr = gitpackScan(gitDir)
	return r
}

func commitCount(repoDir string) int {
	out, err := exec.Command("git", "-C", repoDir, "rev-list", "--all", "--count").Output()
	if err != nil {
		return 0
	}
	var n int
	fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &n)
	return n
}

func runGit(args []string) (int64, time.Duration) {
	start := time.Now()
	cmd := exec.Command("git", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, time.Since(start)
	}
	if err := cmd.Start(); err != nil {
		return 0, time.Since(start)
	}
	n, _ := io.Copy(io.Discard, stdout)
	cmd.Wait()
	return n, time.Since(start)
}

func gitpackScan(gitDir string) (blobs, totalBytes int64, dur time.Duration, err error) {
	start := time.Now()

	scanner, err := objstore.NewHistoryScanner(gitDir)
	if err != nil {
		return 0, 0, time.Since(start), err
	}
	defer scanner.Close()

	s := &countingScanner{}
	if scanErr := scanner.Scan(nil, s); scanErr != nil {
		return s.blobs.Load(), s.totalBytes.Load(), time.Since(start), scanErr
	}

	return s.blobs.Load(), s.totalBytes.Load(), time.Since(start), nil
}

type countingScanner struct {
	blobs      atomic.Int64
	totalBytes atomic.Int64
}

func (s *countingScanner) ScanBlob(r io.Reader, _ objstore.ScanMeta) error {
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		return err
	}
	s.totalBytes.Add(n)
	s.blobs.Add(1)
	return nil
}

func discoverRepos() []string {
	cwd, err := os.Getwd()
	if err != nil {
		return nil
	}
	parent := filepath.Dir(cwd)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil
	}
	var repos []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		gitDir := filepath.Join(parent, e.Name(), ".git")
		if info, err := os.Stat(gitDir); err == nil && info.IsDir() {
			packDir := filepath.Join(gitDir, "objects", "pack")
			if packs, _ := filepath.Glob(filepath.Join(packDir, "*.pack")); len(packs) > 0 {
				repos = append(repos, gitDir)
			}
		}
	}
	return repos
}

func repoName(gitDir string) string {
	// If the repo is a well-known project, use a cleaner name.
	name := filepath.Base(filepath.Dir(gitDir))
	// Try to get remote URL for display.
	out, err := exec.Command("git", "-C", filepath.Dir(gitDir), "remote", "get-url", "origin").Output()
	if err == nil {
		url := strings.TrimSpace(string(out))
		// Extract owner/repo from URL.
		if idx := strings.Index(url, "github.com"); idx >= 0 {
			parts := strings.TrimSuffix(url[idx+len("github.com")+1:], ".git")
			if parts != "" {
				return parts
			}
		}
	}
	return name
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%.0fus", float64(d.Microseconds()))
	case d < time.Second:
		return fmt.Sprintf("%.1fms", float64(d.Milliseconds()))
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

func fmtCount(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%dk", n/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func fmtBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b < kb:
		return fmt.Sprintf("%dB", b)
	case b < mb:
		return fmt.Sprintf("%.1fKB", float64(b)/kb)
	case b < gb:
		return fmt.Sprintf("%.1fMB", float64(b)/mb)
	default:
		return fmt.Sprintf("%.2fGB", float64(b)/gb)
	}
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Suppress git advice/hints that pollute stderr.
	os.Setenv("GIT_TERMINAL_PROMPT", "0")
}
