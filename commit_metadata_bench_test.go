// commit_metadata_bench_test.go measures GetCommitMetadata against a real
// packed repository (not a mock reader), so the numbers include object
// lookup, zlib inflation, and header parsing exactly as a scan consumer
// pays them.
//
// The two benchmarks bracket the cache behavior:
//
//   - Cold: the metaCache is cleared each time the OID rotation wraps, so
//     (almost) every get takes the slow path: locate object, inflate,
//     parse, insert. This is the number the commit-message work must not
//     regress: it inflates the commit object on every miss.
//   - Warm: pure cache hits after a pre-warming pass; guards the fast-path
//     map probe against regressions from entry-shape changes.
//
// Both use the very-large-repo-1k fixture (1000 packed commits). The cold
// benchmark's per-wrap cache reset runs with the timer stopped.
package objstore

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// benchCommitOIDs opens the fixture repo and returns the scanner plus every
// commit OID in it. Shared setup for the metadata benchmarks.
func benchCommitOIDs(b *testing.B, repo string) (*HistoryScanner, []Hash) {
	b.Helper()
	scanner := createScannerForRepo(b, repo)
	b.Cleanup(func() { scanner.Close() })

	commits, err := scanner.loadAllCommits()
	require.NoError(b, err)
	require.NotEmpty(b, commits)

	oids := make([]Hash, len(commits))
	for i, c := range commits {
		oids[i] = c.OID
	}
	return scanner, oids
}

// BenchmarkGetCommitMetadataCold measures the slow (cache-miss) path against
// real packed commits: object lookup + inflation + parse per call.
func BenchmarkGetCommitMetadataCold(b *testing.B) {
	scanner, oids := benchCommitOIDs(b, "very-large-repo-1k")

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if i%len(oids) == 0 {
			// Clearing the map and dropping the slabs is O(entries); keep
			// that cache maintenance out of the per-lookup number.
			b.StopTimer()
			scanner.meta.attachGraph(scanner.graphData)
			b.StartTimer()
		}
		_, err := scanner.GetCommitMetadata(oids[i%len(oids)])
		if err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// BenchmarkGetCommitMetadataWarm measures the fast (cache-hit) path: a map
// probe plus the graph timestamp lookup.
func BenchmarkGetCommitMetadataWarm(b *testing.B) {
	scanner, oids := benchCommitOIDs(b, "very-large-repo-1k")

	// Pre-warm every entry.
	for _, oid := range oids {
		_, err := scanner.GetCommitMetadata(oid)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, err := scanner.GetCommitMetadata(oids[i%len(oids)])
		if err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// BenchmarkReadCommitHeader and BenchmarkReadCommitPayload compare the
// early-exit header read (commit-walk path) with the full-payload read
// (attribution path) on the same packed commits. They document the design
// premise that full inflation of typical commits costs about the same as
// stopping at the committer line, since the whole object usually fits the
// decompressor's first fill anyway.
func BenchmarkReadCommitHeader(b *testing.B) {
	scanner, oids := benchCommitOIDs(b, "very-large-repo-1k")

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, err := scanner.store.readCommitHeader(oids[i%len(oids)])
		if err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func BenchmarkReadCommitPayload(b *testing.B) {
	scanner, oids := benchCommitOIDs(b, "very-large-repo-1k")

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, err := scanner.store.readCommitPayload(oids[i%len(oids)])
		if err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// BenchmarkHunkScanWithAttribution is the consumer-shaped end-to-end
// measurement: a DiffHistoryHunksFunc scan whose callback attributes every
// hunk's commit via GetCommitMetadata, exactly like a secret-scanner joining
// findings to commits. It reports the attribution count and the process peak
// RSS alongside wall time so an unbounded-cache regression is visible, and
// runs against a real repository named by GITPACK_BENCH_REPO (skipped
// otherwise, keeping CI hermetic).
//
// VmHWM is a process-lifetime high-water mark, so the RSS figure is only
// attributable to this benchmark when it runs alone in its process:
//
//	GITPACK_BENCH_REPO=/path/.git go test -run '^$' -bench HunkScanWithAttribution
func BenchmarkHunkScanWithAttribution(b *testing.B) {
	repo := os.Getenv("GITPACK_BENCH_REPO")
	if repo == "" {
		b.Skip("set GITPACK_BENCH_REPO to a .git directory")
	}

	var hunks, metaCalls atomic.Int64
	for b.Loop() {
		scanner, err := NewHistoryScanner(repo)
		require.NoError(b, err)

		err = scanner.DiffHistoryHunksFunc(func(h HunkAddition) error {
			if _, err := scanner.GetCommitMetadata(h.Commit()); err != nil {
				return err
			}
			hunks.Add(1)
			metaCalls.Add(1)
			return nil
		})
		require.NoError(b, err)
		require.NoError(b, scanner.Close())
	}
	// A repository that yields no added hunks measures nothing; refuse the
	// number instead of reporting a fast empty run.
	require.Positive(b, metaCalls.Load(), "benchmark repository produced no attributions")
	b.ReportMetric(float64(hunks.Load())/float64(b.N), "hunks/op")
	b.ReportMetric(float64(metaCalls.Load())/float64(b.N), "metaCalls/op")
	b.ReportMetric(peakRSSMiB(), "procPeakRSS-MiB")
}

// peakRSSMiB reads VmHWM (process-lifetime peak resident set) from
// /proc/self/status. Linux only; returns 0 elsewhere, which benchstat simply
// reports as zero.
func peakRSSMiB() float64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if rest, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			kb, err := strconv.Atoi(strings.TrimSuffix(strings.TrimSpace(rest), " kB"))
			if err != nil {
				return 0
			}
			return float64(kb) / 1024
		}
	}
	return 0
}
