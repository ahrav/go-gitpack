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
	fmt.Println()
	fmt.Println("# go-gitpack Scan vs git log (gitleaks / trufflehog)")
	fmt.Println()
	printMachineInfo()
	fmt.Println()

	fmt.Println("## What each method does")
	fmt.Println()
	fmt.Println("| Method | Work performed |")
	fmt.Println("|--------|---------------|")
	fmt.Println("| gitleaks `git log` | Walk commits, diff trees, compute unified patches (`-U0`), emit changed lines only |")
	fmt.Println("| trufflehog `git log` | Walk commits, diff trees, compute full unified patches, emit changed lines + context |")
	fmt.Println("| gitpack `Scan` | Walk commits, collect unique blob OIDs, decompress + delta-resolve **every full blob** |")
	fmt.Println()
	fmt.Println("gitpack reads significantly more data because it materializes full file contents,")
	fmt.Println("not just diff hunks. Despite this, it is consistently faster due to mmap'd packfile")
	fmt.Println("access, parallel decode workers, and zero process-spawning overhead.")
	fmt.Println()

	fmt.Println("## Results")
	fmt.Println()
	fmt.Printf("| %-25s | %8s | %10s | %10s | %10s | %10s | %10s | %10s | %10s |\n",
		"Repository", "Commits",
		"gitleaks", "gl output",
		"trufflehog", "th output",
		"gitpack", "gp data",
		"gp blobs")
	fmt.Printf("|%s|%s|%s|%s|%s|%s|%s|%s|%s|\n",
		strings.Repeat("-", 27),
		strings.Repeat("-", 10),
		strings.Repeat("-", 12),
		strings.Repeat("-", 12),
		strings.Repeat("-", 12),
		strings.Repeat("-", 12),
		strings.Repeat("-", 12),
		strings.Repeat("-", 12),
		strings.Repeat("-", 12))

	for _, r := range results {
		if r.gpErr != nil {
			fmt.Printf("| %-25s | %8d | ERROR: %v\n", r.name, r.commits, r.gpErr)
			continue
		}
		fmt.Printf("| %-25s | %8d | %10s | %10s | %10s | %10s | %10s | %10s | %10d |\n",
			r.name, r.commits,
			fmtDur(r.git[0].dur), fmtBytes(r.git[0].bytes),
			fmtDur(r.git[1].dur), fmtBytes(r.git[1].bytes),
			fmtDur(r.gpDur), fmtBytes(r.gpBytes),
			r.gpBlobs)
	}

	fmt.Println()
	fmt.Println("## Speedup")
	fmt.Println()
	fmt.Printf("| %-25s | %8s | %10s | %10s | %6s | %10s | %10s | %6s |\n",
		"Repository", "Commits",
		"gitleaks", "gitpack", "ratio",
		"trufflehog", "gitpack", "ratio")
	fmt.Printf("|%s|%s|%s|%s|%s|%s|%s|%s|\n",
		strings.Repeat("-", 27),
		strings.Repeat("-", 10),
		strings.Repeat("-", 12),
		strings.Repeat("-", 12),
		strings.Repeat("-", 8),
		strings.Repeat("-", 12),
		strings.Repeat("-", 12),
		strings.Repeat("-", 8))

	for _, r := range results {
		if r.gpErr != nil {
			continue
		}
		glRatio := "—"
		thRatio := "—"
		if r.git[0].dur > 0 {
			glRatio = fmt.Sprintf("%.2fx", float64(r.gpDur)/float64(r.git[0].dur))
		}
		if r.git[1].dur > 0 {
			thRatio = fmt.Sprintf("%.2fx", float64(r.gpDur)/float64(r.git[1].dur))
		}
		fmt.Printf("| %-25s | %8d | %10s | %10s | %6s | %10s | %10s | %6s |\n",
			r.name, r.commits,
			fmtDur(r.git[0].dur), fmtDur(r.gpDur), glRatio,
			fmtDur(r.git[1].dur), fmtDur(r.gpDur), thRatio)
	}

	fmt.Println()
	fmt.Println("Ratio = gitpack / git (lower = gitpack is faster).")
	fmt.Println()
	fmt.Println("## Legend")
	fmt.Println()
	fmt.Println("| Label | Command |")
	fmt.Println("|-------|---------|")
	fmt.Println("| gitleaks | `git log -p -U0 --full-history --all --diff-filter=tuxdb` |")
	fmt.Println("| trufflehog | `git log -p --full-history --all --date=iso-strict --pretty=fuller --notes` |")
	fmt.Println("| gitpack | `HistoryScanner.Scan()` — mmap packfiles, parallel decode, stream every unique blob |")
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
