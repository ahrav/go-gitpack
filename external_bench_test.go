package objstore

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkDiffHistoryHunksExternal runs the hunk diff pipeline on a real bare
// repository. It is skipped unless GITPACK_BENCH_REPO points at a .git
// directory, so normal test and benchmark runs stay hermetic.
func BenchmarkDiffHistoryHunksExternal(b *testing.B) {
	repo := os.Getenv("GITPACK_BENCH_REPO")
	if repo == "" {
		b.Skip("set GITPACK_BENCH_REPO to a bare .git directory")
	}

	for b.Loop() {
		scanner, err := NewHistoryScanner(repo)
		require.NoError(b, err)

		hunks, errC := scanner.DiffHistoryHunks()
		count := 0
		for range hunks {
			count++
		}
		require.NoError(b, <-errC)
		require.Greater(b, count, 0)
		require.NoError(b, scanner.Close())
	}
}
