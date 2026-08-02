// rename_pairing_bench_test.go measures pairCommitRenames, the per-commit
// candidate/delete classification behind rename detection, under the
// adversarial commit shapes the feature targets:
//
//   - SameIdentity mass moves: every file in a moved tree has identical
//     content (empty __init__.py, .gitkeep, generated boilerplate), so one
//     identity group holds thousands of candidate deletes. Matching cost per
//     candidate must stay near-constant, not scale with the group size.
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

// pairingInput is one fixture: the buffered candidates, the deletion pool, and
// the per-delete consumption flags, in the exact shapes emitCommitBlobPairs
// hands to pairCommitRenames.
type pairingInput struct {
	candidates []blobPairCandidate
	deletes    []deletedEntry
	byIdentity map[blobIdentity]*deleteGroup
	used       []bool
}

// recordDelete mirrors emitCommitBlobPairs' delete bookkeeping for fixtures.
func recordDelete(in *pairingInput, path string, oid Hash) {
	entry := deletedEntry{path: path, oid: oid, kind: modeBlob}
	g := in.byIdentity[entry.identity()]
	if g == nil {
		g = &deleteGroup{}
		in.byIdentity[entry.identity()] = g
	}
	g.indices = append(g.indices, len(in.deletes))
	in.deletes = append(in.deletes, entry)
}

// recordAdd mirrors emitCommitBlobPairs' candidate bookkeeping for fixtures.
func recordAdd(in *pairingInput, path string, newOID Hash) {
	in.candidates = append(in.candidates, blobPairCandidate{
		work: blobPairWork{path: path, newOID: newOID},
		kind: modeBlob,
	})
}

// sameIdentityMoveInput builds a commit shape where n deletes and n adds all
// share ONE blob identity (identical content and entry type). withBasenameMatch
// controls whether each add keeps its basename across the move (the realistic
// directory-rename shape) or every basename changes (the worst case: no early
// exit on basename, full-group fallback scans).
func sameIdentityMoveInput(n int, withBasenameMatch bool) pairingInput {
	shared := benchHash(1)
	in := pairingInput{
		candidates: make([]blobPairCandidate, 0, n),
		deletes:    make([]deletedEntry, 0, n),
		byIdentity: make(map[blobIdentity]*deleteGroup, 1),
	}
	for i := 0; i < n; i++ {
		recordDelete(&in, fmt.Sprintf("old/file-%d.txt", i), shared)

		base := fmt.Sprintf("file-%d.txt", i)
		if !withBasenameMatch {
			base = fmt.Sprintf("renamed-%d.txt", i)
		}
		recordAdd(&in, "new/"+base, shared)
	}
	in.used = make([]bool, len(in.deletes))
	return in
}

// dirCandidateInput builds numDirs directory-rename candidates (two exact
// renames each) plus numAdds unmatched adds in unrelated directories, so
// every unmatched add exercises the candidate lookup without ever matching.
func dirCandidateInput(numDirs, numAdds int) pairingInput {
	in := pairingInput{
		candidates: make([]blobPairCandidate, 0, 2*numDirs+numAdds),
		deletes:    make([]deletedEntry, 0, 2*numDirs),
		byIdentity: make(map[blobIdentity]*deleteGroup, 2*numDirs),
	}
	oid := uint64(0)
	for d := 0; d < numDirs; d++ {
		for f := 0; f < 2; f++ {
			oid++
			h := benchHash(oid)
			recordDelete(&in, fmt.Sprintf("src/dir-%d/anchor-%d.txt", d, f), h)
			recordAdd(&in, fmt.Sprintf("dst/dir-%d/anchor-%d.txt", d, f), h)
		}
	}
	for a := 0; a < numAdds; a++ {
		oid++
		recordAdd(&in, fmt.Sprintf("unrelated/deep/tree/added-%d.txt", a), benchHash(oid))
	}
	in.used = make([]bool, len(in.deletes))
	return in
}

func BenchmarkPairCommitRenames(b *testing.B) {
	run := func(b *testing.B, build func() pairingInput, wantEmitted int) {
		b.Helper()
		// Shape sanity check outside the timed region: the fixture must
		// actually exercise the intended matching path.
		{
			in := build()
			got := pairCommitRenames(in.candidates, in.deletes, in.byIdentity, in.used)
			if len(got) != wantEmitted {
				b.Fatalf("fixture emitted %d works, want %d", len(got), wantEmitted)
			}
		}
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			in := build()
			b.StartTimer()
			pairCommitRenames(in.candidates, in.deletes, in.byIdentity, in.used)
		}
	}

	// Every add is an exact move: all suppressed, nothing emitted.
	b.Run("SameIdentity/BasenameMatch/1k", func(b *testing.B) {
		run(b, func() pairingInput { return sameIdentityMoveInput(1000, true) }, 0)
	})
	b.Run("SameIdentity/BasenameMatch/4k", func(b *testing.B) {
		run(b, func() pairingInput { return sameIdentityMoveInput(4000, true) }, 0)
	})
	b.Run("SameIdentity/BasenameMiss/1k", func(b *testing.B) {
		run(b, func() pairingInput { return sameIdentityMoveInput(1000, false) }, 0)
	})
	b.Run("SameIdentity/BasenameMiss/4k", func(b *testing.B) {
		run(b, func() pairingInput { return sameIdentityMoveInput(4000, false) }, 0)
	})

	// 1k candidate dirs, 4k unmatched adds: emitted = the 4k unmatched adds.
	b.Run("DirCandidates/1kx4k", func(b *testing.B) {
		run(b, func() pairingInput { return dirCandidateInput(1000, 4000) }, 4000)
	})
}
