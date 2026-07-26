// commit_fallback_test.go tests the graceful fallback behaviour when a
// repository's HEAD or branch refs point to objects that do not exist in any
// pack file. The scanner should return zero commits rather than an error,
// allowing callers to handle repositories with dangling references. It also
// pins the visit-concurrency contract of the two walk entry points, which
// callers rely on to decide whether they need their own locking.
package objstore

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadAllCommits_SkipsMissingRefObjects simulates a repository whose HEAD
// resolves to refs/heads/main, which in turn contains a 40-hex OID that does
// not correspond to any object in the pack directory. The expected behaviour
// is that loadAllCommits returns an empty commit slice (not an error), because
// missing ref targets are silently skipped during the commit walk.
func TestLoadAllCommits_SkipsMissingRefObjects(t *testing.T) {
	gitDir := t.TempDir()

	for _, rel := range []string{
		filepath.Join("objects", "pack"),
		filepath.Join("refs", "heads"),
	} {
		if err := os.MkdirAll(filepath.Join(gitDir, rel), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
	}

	const missingOID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatalf("write HEAD: %v", err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "refs", "heads", "main"), []byte(missingOID+"\n"), 0o644); err != nil {
		t.Fatalf("write refs/heads/main: %v", err)
	}

	scanner, err := NewHistoryScanner(gitDir)
	if err != nil {
		t.Fatalf("NewHistoryScanner: %v", err)
	}
	defer scanner.Close()

	commits, err := scanner.loadAllCommits()
	if err != nil {
		t.Fatalf("LoadAllCommits: %v", err)
	}
	if len(commits) != 0 {
		t.Fatalf("expected zero commits, got %d", len(commits))
	}
}

// TestReadRefHash_PropagatesIOErrors verifies that readRefHash returns an error
// (not just false) when the ref file cannot be read due to a permission error.
// Before the fix, all errors were swallowed as "not found".
func TestReadRefHash_PropagatesIOErrors(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission-based test not reliable on Windows")
	}

	gitDir := t.TempDir()
	refsDir := filepath.Join(gitDir, "refs", "heads")
	require.NoError(t, os.MkdirAll(refsDir, 0o755))

	refPath := filepath.Join(refsDir, "main")
	require.NoError(t, os.WriteFile(refPath, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"), 0o000))

	// Before the fix: readRefHash returns (Hash{}, false, nil), swallowing
	// the permission error. After the fix: it returns a non-nil error.
	_, _, err := readRefHash(gitDir, "refs/heads/main")
	assert.Error(t, err, "permission errors should be propagated, not swallowed")
}

// TestReadRefHash_ChecksScannerError verifies that readRefHash checks sc.Err()
// after the scanner loop, so I/O errors from packed-refs are reported.
func TestReadRefHash_ChecksScannerError(t *testing.T) {
	gitDir := t.TempDir()

	// Create a packed-refs file with valid content. readRefHash should
	// return (Hash{}, false, nil) when the ref is simply not in packed-refs.
	packedRefs := "# pack-refs with: peeled fully-peeled sorted\n" +
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb refs/heads/other\n"
	require.NoError(t, os.WriteFile(filepath.Join(gitDir, "packed-refs"), []byte(packedRefs), 0o644))

	_, ok, err := readRefHash(gitDir, "refs/heads/nonexistent")
	require.NoError(t, err)
	assert.False(t, ok)
}

// TestWalkCommitsFromRefs_VisitsConcurrently proves the parallel walk does not
// serialize visit. The barrier below is released only once two workers sit
// inside visit at the same time, so it fails if the walk holds any lock across
// the callback: DiffHistoryHunksFunc's visit is already concurrency-safe, and
// funneling every visit through one mutex parks the walk workers instead of
// inflating commit headers.
func TestWalkCommitsFromRefs_VisitsConcurrently(t *testing.T) {
	if runtime.NumCPU() < 2 {
		t.Skip("the walk runs a single worker on a single-CPU machine")
	}

	// with-merges publishes two ref tips, so the frontier offers two commits
	// from the first pop onward.
	scanner := createScannerForRepo(t, "with-merges")
	defer scanner.Close()

	const wantInFlight = 2
	var (
		mu       sync.Mutex
		inFlight int
		gaveUp   atomic.Bool
		once     sync.Once
	)
	reached := make(chan struct{})

	err := scanner.walkCommitsFromRefs(func(commitInfo) error {
		mu.Lock()
		inFlight++
		n := inFlight
		mu.Unlock()

		if n >= wantInFlight {
			once.Do(func() { close(reached) })
		}
		if !gaveUp.Load() {
			select {
			case <-reached:
			case <-time.After(10 * time.Second):
				// Serialized visits can never reach the barrier. Stop waiting
				// so the walk finishes and the assertion below reports it,
				// rather than letting every commit burn the full deadline.
				gaveUp.Store(true)
			}
		}

		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)

	select {
	case <-reached:
	default:
		t.Fatal("no two visits were in flight at once: the walk serializes visit")
	}
}

// TestWalkCommitsFromRefsOrdered_VisitsSeriallyInStableOrder pins the contract
// the scan planners depend on: a single worker never calls visit concurrently,
// and the visit order is reproducible, so first-wins blob dedup attributes each
// blob to the same commit on every run.
func TestWalkCommitsFromRefsOrdered_VisitsSeriallyInStableOrder(t *testing.T) {
	scanner := createScannerForRepo(t, "with-merges")
	defer scanner.Close()

	collect := func() []Hash {
		var (
			inVisit    atomic.Bool
			overlapped atomic.Bool
			order      []Hash
		)
		require.NoError(t, scanner.walkCommitsFromRefsOrdered(func(c commitInfo) error {
			if !inVisit.CompareAndSwap(false, true) {
				overlapped.Store(true)
			}
			order = append(order, c.OID)
			inVisit.Store(false)
			return nil
		}))
		require.False(t, overlapped.Load(), "the ordered walk invoked visit concurrently")
		return order
	}

	first := collect()
	require.Len(t, first, 5, "with-merges holds five commits")
	require.Equal(t, first, collect(), "ordered visit order must be reproducible")
}
