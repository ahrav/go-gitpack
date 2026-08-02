// rename_pairing_bench_test.go measures pairCommitRenames, the per-commit
// add/delete classification behind rename detection, under the adversarial
// commit shapes the feature targets:
//
//   - SameOID mass moves: every file in a moved tree has identical content
//     (empty __init__.py, .gitkeep, generated boilerplate), so one OID group
//     holds thousands of candidate deletes. Matching cost per add must stay
//     near-constant, not scale with the group size.
//   - Many directory-rename candidates: a partial mega-move produces
//     thousands of (oldDir,newDir) candidates while thousands of edited files
//     remain unmatched adds. Candidate lookup per add must scale with path
//     depth, not with the candidate count.
//
// The inputs are rebuilt outside the timed region each iteration because
// pairCommitRenames consumes them (used flags, map, backing array).
package objstore

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// benchHash returns a synthetic distinct Hash for fixture construction.
func benchHash(n uint64) Hash {
	var h Hash
	binary.BigEndian.PutUint64(h[:8], n+1) // +1 keeps the zero Hash reserved
	return h
}

// recordDelete mirrors emitCommitBlobPairs' delete bookkeeping for fixtures.
func recordDelete(byOID map[Hash]*deleteGroup, oid Hash, idx int) {
	g := byOID[oid]
	if g == nil {
		g = &deleteGroup{}
		byOID[oid] = g
	}
	g.indices = append(g.indices, idx)
}

// sameOIDMoveInput builds a commit shape where n deletes and n adds all share
// ONE blob OID (identical content). withBasenameMatch controls whether each
// add keeps its basename across the move (the realistic directory-rename
// shape) or every basename changes (the worst case: no early exit on
// basename, full-group fallback scans).
func sameOIDMoveInput(n int, withBasenameMatch bool) (adds, deletes []blobPairWork, byOID map[Hash]*deleteGroup) {
	shared := benchHash(1)
	adds = make([]blobPairWork, 0, n)
	deletes = make([]blobPairWork, 0, n)
	byOID = make(map[Hash]*deleteGroup, 1)
	for i := 0; i < n; i++ {
		deletes = append(deletes, blobPairWork{
			path:   fmt.Sprintf("old/file-%d.txt", i),
			oldOID: shared,
		})
		recordDelete(byOID, shared, i)

		base := fmt.Sprintf("file-%d.txt", i)
		if !withBasenameMatch {
			base = fmt.Sprintf("renamed-%d.txt", i)
		}
		adds = append(adds, blobPairWork{
			path:   "new/" + base,
			newOID: shared,
		})
	}
	return adds, deletes, byOID
}

// dirCandidateInput builds numDirs directory-rename candidates (two exact
// renames each) plus numAdds unmatched adds in unrelated directories, so
// every unmatched add exercises the candidate lookup without ever matching.
func dirCandidateInput(numDirs, numAdds int) (adds, deletes []blobPairWork, byOID map[Hash]*deleteGroup) {
	adds = make([]blobPairWork, 0, 2*numDirs+numAdds)
	deletes = make([]blobPairWork, 0, 2*numDirs)
	byOID = make(map[Hash]*deleteGroup, 2*numDirs)
	oid := uint64(0)
	for d := 0; d < numDirs; d++ {
		for f := 0; f < 2; f++ {
			oid++
			h := benchHash(oid)
			idx := len(deletes)
			deletes = append(deletes, blobPairWork{
				path:   fmt.Sprintf("src/dir-%d/anchor-%d.txt", d, f),
				oldOID: h,
			})
			recordDelete(byOID, h, idx)
			adds = append(adds, blobPairWork{
				path:   fmt.Sprintf("dst/dir-%d/anchor-%d.txt", d, f),
				newOID: h,
			})
		}
	}
	for a := 0; a < numAdds; a++ {
		oid++
		adds = append(adds, blobPairWork{
			path:   fmt.Sprintf("unrelated/deep/tree/added-%d.txt", a),
			newOID: benchHash(oid),
		})
	}
	return adds, deletes, byOID
}

func BenchmarkPairCommitRenames(b *testing.B) {
	run := func(b *testing.B, build func() (adds, deletes []blobPairWork, byOID map[Hash]*deleteGroup), wantEmitted int) {
		b.Helper()
		// Shape sanity check outside the timed region: the fixture must
		// actually exercise the intended matching path.
		{
			adds, deletes, byOID := build()
			got := pairCommitRenames(adds, deletes, byOID)
			if len(got) != wantEmitted {
				b.Fatalf("fixture emitted %d works, want %d", len(got), wantEmitted)
			}
		}
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			adds, deletes, byOID := build()
			b.StartTimer()
			pairCommitRenames(adds, deletes, byOID)
		}
	}

	// Every add is an exact rename: all suppressed, nothing emitted.
	b.Run("SameOID/BasenameMatch/1k", func(b *testing.B) {
		run(b, func() ([]blobPairWork, []blobPairWork, map[Hash]*deleteGroup) {
			return sameOIDMoveInput(1000, true)
		}, 0)
	})
	b.Run("SameOID/BasenameMatch/4k", func(b *testing.B) {
		run(b, func() ([]blobPairWork, []blobPairWork, map[Hash]*deleteGroup) {
			return sameOIDMoveInput(4000, true)
		}, 0)
	})
	b.Run("SameOID/BasenameMiss/1k", func(b *testing.B) {
		run(b, func() ([]blobPairWork, []blobPairWork, map[Hash]*deleteGroup) {
			return sameOIDMoveInput(1000, false)
		}, 0)
	})
	b.Run("SameOID/BasenameMiss/4k", func(b *testing.B) {
		run(b, func() ([]blobPairWork, []blobPairWork, map[Hash]*deleteGroup) {
			return sameOIDMoveInput(4000, false)
		}, 0)
	})

	// 1k candidate dirs, 4k unmatched adds: emitted = the 4k unmatched adds.
	b.Run("DirCandidates/1kx4k", func(b *testing.B) {
		run(b, func() ([]blobPairWork, []blobPairWork, map[Hash]*deleteGroup) {
			return dirCandidateInput(1000, 4000)
		}, 4000)
	})
}
