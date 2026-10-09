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
