// These tests reduce dedupLimits.inFlightPairs to exercise window
// boundaries with small repositories.

package objstore

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func uniqueText(tag string, n int) string {
	var b strings.Builder
	for i := range n {
		h := fnv.New64a()
		fmt.Fprintf(h, "%s/%d", tag, i)
		fmt.Fprintf(&b, "%s line %05d %016x %s\n", tag, i, h.Sum64(), strings.Repeat("x", 24))
	}
	return b.String()
}

func scanDedupWithLimits(t *testing.T, gitDir string, timeout time.Duration, tune func(*dedupLimits), probe *dedupProbe) []string {
	t.Helper()
	s, err := NewHistoryScanner(gitDir, WithHunkLineDedup(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	tune(&s.dedupLimits)
	s.dedupProbe = probe

	var (
		mu  sync.Mutex
		out []string
	)
	done := make(chan error, 1)
	go func() {
		done <- s.DiffHistoryHunksFunc(func(h HunkAddition) error {
			mu.Lock()
			out = append(out, canonicalHunkAddition(h))
			mu.Unlock()
			return nil
		})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(timeout):
		t.Fatalf("dedup scan did not finish within %v", timeout)
	}
	sort.Strings(out)
	return out
}

// Every line is unique, so dedup must emit every hunk of this history.
func buildWideCommitRepo(t *testing.T) string {
	t.Helper()
	b := newDedupRepoBuilder(t)
	for i := range 20 {
		lines := 4
		if i >= 8 {
			lines = 64
		}
		name := fmt.Sprintf("f%02d.txt", i)
		b.write(name, uniqueText(name, lines))
	}
	b.commit("wide root")
	b.write("tail.txt", uniqueText("tail", 3))
	b.commit("tail")
	return b.finish()
}

// The expensive pairs beyond the eight-pair window exercise early
// forwarding at the window boundary of a 20-file root commit.
func TestDiffHistoryHunksDedup_WideCommitEarlyForwardStaysInWindow(t *testing.T) {
	gitDir := buildWideCommitRepo(t)
	want := serialDedupReference(t, gitDir, false, nil)
	require.Len(t, want, 21)

	tune := func(l *dedupLimits) {
		l.inFlightPairs = 8
		l.expensivePairBytes = 2 << 10
	}
	for run := range 20 {
		got := scanDedupWithLimits(t, gitDir, 10*time.Second, tune, nil)
		require.Equalf(t, want, got, "run %d", run)
	}
}

// The root's only pair is forwarded early; stamping the next commit waits
// for that pair's decision. With one hunk worker, the worker parked on the
// window is the only one that can run the queued pair.
func TestDiffHistoryHunksDedup_FullWindowBehindQueuedExpensivePair(t *testing.T) {
	b := newDedupRepoBuilder(t)
	b.write("big.txt", uniqueText("big", 64))
	b.commit("expensive root")
	for i := range 8 {
		name := fmt.Sprintf("s%02d.txt", i)
		b.write(name, uniqueText(name, 2))
	}
	b.commit("fills the window")
	gitDir := b.finish()

	want := serialDedupReference(t, gitDir, false, nil)
	require.Len(t, want, 9)

	got := scanDedupWithLimits(t, gitDir, 10*time.Second, func(l *dedupLimits) {
		l.inFlightPairs = 8
		l.expensivePairBytes = 2 << 10
		l.workers = 1
	}, nil)
	require.Equal(t, want, got)
}

// A commit wider than the window waits for every earlier seq to be decided
// before stamping, and the preceding large diff delays that decision. The
// window-wait bound detects a sequencer polling through the delay.
func TestDiffHistoryHunksDedup_WideCommitParksUntilDrained(t *testing.T) {
	b := newDedupRepoBuilder(t)
	big := uniqueText("big", 60000)
	b.write("big.txt", big)
	b.commit("big")
	b.write("big.txt", "head line\n"+big+"tail line\n")
	b.commit("slow diff")
	for i := range 20 {
		name := fmt.Sprintf("w%02d.txt", i)
		b.write(name, uniqueText(name, 2))
	}
	b.commit("wide")
	gitDir := b.finish()

	want := serialDedupReference(t, gitDir, false, nil)
	probe := &dedupProbe{}
	got := scanDedupWithLimits(t, gitDir, 30*time.Second, func(l *dedupLimits) {
		l.inFlightPairs = 8
		l.expensivePairBytes = 1 << 40
		l.workers = 2
	}, probe)
	require.Equal(t, want, got)
	require.Lessf(t, probe.windowWaits.Load(), uint64(100),
		"the sequencer re-polled the window instead of parking")
}

// stalledScanPeaks runs a dedup scan on s whose consumer blocks until the
// probe peaks stop moving, then releases the consumer and returns the
// sorted emissions with the observed peak pending pairs and retained bytes.
func stalledScanPeaks(t *testing.T, s *HistoryScanner, probe *dedupProbe) (got []string, pairs, bytes int64) {
	t.Helper()
	release := make(chan struct{})
	var mu sync.Mutex
	done := make(chan error, 1)
	go func() {
		done <- s.DiffHistoryHunksFunc(func(h HunkAddition) error {
			<-release
			mu.Lock()
			got = append(got, canonicalHunkAddition(h))
			mu.Unlock()
			return nil
		})
	}()

	var lastPairs, lastBytes int64
	stableSince := time.Now()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
		p, by := probe.peakPendingPairs.Load(), probe.peakRetainedBytes.Load()
		if p != lastPairs || by != lastBytes {
			lastPairs, lastBytes = p, by
			stableSince = time.Now()
			continue
		}
		if time.Since(stableSince) > 300*time.Millisecond {
			break
		}
	}
	pairs, bytes = probe.peakPendingPairs.Load(), probe.peakRetainedBytes.Load()

	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("dedup scan did not finish after the consumer resumed")
	}
	sort.Strings(got)
	return got, pairs, bytes
}

// A blocked consumer retains materialized pair lists and hunk bytes. Both
// must stay near their caps while the consumer is stalled, and the scan
// must still emit the serial reference once it resumes.
func TestDiffHistoryHunksDedup_StalledConsumerBoundsRetention(t *testing.T) {
	const (
		commits = 60
		files   = 30
		lines   = 16
	)
	b := newDedupRepoBuilder(t)
	for c := range commits {
		for f := range files {
			name := fmt.Sprintf("f%02d.txt", f)
			b.write(name, uniqueText(fmt.Sprintf("c%02d%s", c, name), lines))
		}
		b.commit(fmt.Sprintf("rewrite %d", c))
	}
	gitDir := b.finish()
	want := serialDedupReference(t, gitDir, false, nil)

	s, err := NewHistoryScanner(gitDir, WithHunkLineDedup(true))
	require.NoError(t, err)
	defer s.Close()
	s.dedupLimits.inFlightPairs = 32
	s.dedupLimits.workers = 2
	s.dedupLimits.yieldChanCap = 1
	s.dedupLimits.pendingPairsCap = 40
	s.dedupLimits.retainedBytesCap = 8 << 10
	probe := &dedupProbe{}
	s.dedupProbe = probe

	got, pairs, bytes := stalledScanPeaks(t, s, probe)
	require.Equal(t, want, got)

	treeWorkers := int64(min(s.dedupLimits.workers, maxTreeDiffWorkers))
	maxPairs := s.dedupLimits.pendingPairsCap + dedupDispatchDepth*treeWorkers*files
	hunk := mkTestHunk(1, strings.Split(strings.TrimSuffix(uniqueText("c00f00.txt", lines), "\n"), "\n")...)
	maxBytes := s.dedupLimits.retainedBytesCap + int64(s.dedupLimits.inFlightPairs)*int64(toKiB(uint64(hunkRetainedBytes(&hunk))))<<10

	t.Logf("peak pending pairs %d (bound %d), peak retained bytes %d (bound %d)", pairs, maxPairs, bytes, maxBytes)
	require.LessOrEqualf(t, pairs, maxPairs, "pending pair lists outgrew the look-ahead budget")
	require.LessOrEqualf(t, bytes, maxBytes, "retained hunk bytes outgrew the budget")
}

// One commit wider than the in-flight window is stamped whole, so its
// pairs are admitted one window at a time as earlier pairs are decided.
// With the consumer stalled, admission must still stop once the retained
// bytes pass the cap: what is in flight when the cap is crossed is at most
// one batch per hunk worker plus the batches the yield stage holds.
func TestDiffHistoryHunksDedup_WideCommitStalledConsumerBoundsRetention(t *testing.T) {
	const (
		files = 2000
		lines = 16
	)
	b := newDedupRepoBuilder(t)
	for f := range files {
		name := fmt.Sprintf("f%04d.txt", f)
		b.write(name, uniqueText(name, lines))
	}
	b.commit("wide")
	gitDir := b.finish()
	want := serialDedupReference(t, gitDir, false, nil)
	require.Len(t, want, files)

	s, err := NewHistoryScanner(gitDir, WithHunkLineDedup(true))
	require.NoError(t, err)
	defer s.Close()
	s.dedupLimits.inFlightPairs = 512
	s.dedupLimits.workers = 2
	s.dedupLimits.yieldChanCap = 1
	s.dedupLimits.retainedBytesCap = 2 << 10
	probe := &dedupProbe{}
	s.dedupProbe = probe

	got, _, bytes := stalledScanPeaks(t, s, probe)
	require.Equal(t, want, got)

	hunk := mkTestHunk(1, strings.Split(strings.TrimSuffix(uniqueText("f0000.txt", lines), "\n"), "\n")...)
	pairBytes := int64(toKiB(uint64(hunkRetainedBytes(&hunk)))) << 10
	// Hunk workers hold one batch each; the yield stage holds one batch per
	// yield worker, the channel buffer, and the decision stage's open batch.
	heldBatches := int64(s.dedupLimits.workers + s.dedupLimits.workers + s.dedupLimits.yieldChanCap + 1)
	maxBytes := s.dedupLimits.retainedBytesCap + heldBatches*int64(max(dedupPairBatchSize, dedupYieldBatchHunks))*pairBytes

	t.Logf("peak retained bytes %d (bound %d, window %d pairs = %d bytes)", bytes, maxBytes,
		s.dedupLimits.inFlightPairs, int64(s.dedupLimits.inFlightPairs)*pairBytes)
	require.LessOrEqualf(t, bytes, maxBytes, "a wide commit kept admitting pairs past the retained-byte cap")
}

// The reorder ring holds one dedupPairResult per in-flight pair and is
// zeroed on every scan, so a larger result shows up directly in short-scan
// latency.
func TestDedupPairResultSize(t *testing.T) {
	require.LessOrEqual(t, unsafe.Sizeof(dedupPairResult{}), uintptr(64))
}
