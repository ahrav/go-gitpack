// diff_blob.go
//
// Diff analysis utilities for identifying blocks of added lines between two
// versions of a file. The implementation performs line-by-line comparison
// to find additions and groups consecutive added lines into "hunks" for
// efficient representation.
//
// The algorithm uses an adaptive, multi-tier optimization strategy selected
// by file size (see computeAddedHunks):
//
//   - <= SmallFileThreshold  (1 MB):   addedHunksWithPos — greedy forward scan
//     with optional hash-map acceleration for files with >50 lines. This is NOT
//     a standard diff algorithm (e.g., Myers); it is a greedy heuristic that
//     walks the old and new line sequences in order and reports lines in newB
//     that cannot be matched to a remaining line in oldB.
//   - <= MediumFileThreshold (50 MB):  addedHunksWithLineSet — set-membership
//     diff that discards positional information.
//   - <= LargeFileThreshold  (500 MB): addedHunksWithHashing — stores only
//     64-bit FarmHash digests per line to limit memory.
//   - > MaxDiffSize          (1 GB):   skipped entirely; a placeholder hunk is
//     returned instead. Enforced per side as each blob is lazily loaded; see
//     the MaxDiffSize doc comment for the short-circuit paths that never
//     consult the old blob.
//
// Tokenization is zero-copy: tokenize() creates string views into the original
// byte slices via btostr (see unsafe.go). This means the returned strings share
// the backing array of the input []byte. Callers must not mutate the input
// slices after tokenization, or the strings become invalid.
//
// Cross-file dependencies:
//   - btostr (unsafe.go): zero-copy []byte to string conversion.
//   - store.get (store.go): used by loadBlob to retrieve blob content.

package objstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"sync"

	"github.com/dgryski/go-farm"
)

// lineHashSeed seeds the runtime maphash used by lineIndex. One process-wide
// seed is fine: the index verifies string equality on every hit, so
// collisions cost time, never correctness.
var lineHashSeed = maphash.MakeSeed()

// lineIndex accelerates "first occurrence of this line at or after position
// p" queries over the old file's lines. Line contents are bucketed by 64-bit
// maphash into chains stored in one flat next slice. The bucket heads live
// in an open-addressed, epoch-stamped table rather than a Go map: bumping
// the epoch invalidates every slot in O(1), so a pooled index is reusable
// across diffs with no clear() pass, no per-line allocations, and no map
// runtime overhead on the hot lookup path. Hash collisions are harmless —
// lookups verify actual string equality before accepting a position.
type lineIndex struct {
	slots []uint64
	epoch uint32
	next  []int32
}

// Slot encoding: each open-addressing bucket is a single uint64 —
//
//	[ hash tag: 24 bits | pos: 32 bits | epoch: 8 bits ]
//
// Packing the previous 16-byte {hash, pos, epoch} struct into 8 bytes halves
// the table's cache footprint; profiling showed the random-index slot probe
// was the hottest load in the whole diff path. A slot is live only when its
// low epoch byte matches the index's current epoch (mod 256); the table is
// fully cleared on wraparound so stale epochs can never alias. The 24-bit
// tag only filters probes — chain entries are verified by string equality —
// so tag collisions cost time, never correctness.
const (
	lineSlotEpochBits = 8
	lineSlotEpochMask = 1<<lineSlotEpochBits - 1
	lineSlotPosShift  = lineSlotEpochBits
	lineSlotTagShift  = lineSlotEpochBits + 32
)

func packLineSlot(h uint64, pos int32, epoch uint32) uint64 {
	return (h>>40)<<lineSlotTagShift | uint64(uint32(pos))<<lineSlotPosShift | uint64(epoch)
}

var lineIndexPool = sync.Pool{
	New: func() any { return &lineIndex{} },
}

// maxPooledLineIndexBytes bounds the retained capacity of a lineIndex that
// may be returned to lineIndexPool. build() only ever grows slots and next,
// and sync.Pool holds objects across GC cycles, so without a cap every
// pooled index ratchets to the largest file it has ever indexed (~20 MiB
// for a 2^20-line file: 16 MiB of slots plus 4 MiB of next) — retained once
// per scan worker. Oversized indexes are dropped instead of pooled: the
// rebuild allocation is dwarfed by the cost of diffing a file that large,
// and re-pooling them would also make every subsequent small diff probe a
// huge, cache-cold table. 4 MiB keeps indexes for old sides up to ~200K
// lines pooled (a 120K-line file of 4-byte lines needs ~2.5 MiB; dropping
// at 2 MiB measurably regressed that shape) while excluding the degenerate
// one-byte-per-line ratchet.
const maxPooledLineIndexBytes = 4 << 20 // 4 MiB

// putLineIndex returns ix to lineIndexPool unless its retained capacity
// exceeds maxPooledLineIndexBytes, in which case it is dropped for the GC.
func putLineIndex(ix *lineIndex) {
	if cap(ix.slots)*8+cap(ix.next)*4 > maxPooledLineIndexBytes {
		return
	}
	lineIndexPool.Put(ix)
}

// build indexes lines[from:]. Chains are threaded in ascending position
// order by inserting from the highest index down.
func (ix *lineIndex) build(lines []string, from int) {
	n := len(lines) - from
	// Size for load factor <= 0.5, minimum 1024 slots.
	want := 1024
	for want < n*2 {
		want <<= 1
	}
	if len(ix.slots) < want {
		ix.slots = make([]uint64, want)
		ix.epoch = 0
	}
	ix.epoch++
	if ix.epoch&lineSlotEpochMask == 0 { // epoch byte wrapped: stale slots would read as live
		clear(ix.slots)
		ix.epoch = 1
	}
	if cap(ix.next) < len(lines) {
		ix.next = make([]int32, len(lines))
	}
	ix.next = ix.next[:len(lines)]

	epoch := ix.epoch & lineSlotEpochMask
	slots := ix.slots
	next := ix.next
	mask := uint64(len(slots) - 1)
	for i := len(lines) - 1; i >= from; i-- {
		h := maphash.String(lineHashSeed, lines[i])
		tagged := packLineSlot(h, int32(i), epoch)
		j := h & mask
		for {
			s := slots[j]
			if s&lineSlotEpochMask != uint64(epoch) { // empty slot: new bucket
				slots[j] = tagged
				next[i] = -1
				break
			}
			if s>>lineSlotTagShift == h>>40 { // existing bucket: prepend (i is smaller)
				next[i] = int32(uint32(s >> lineSlotPosShift))
				slots[j] = tagged
				break
			}
			j = (j + 1) & mask
		}
	}
}

// lookup returns the smallest position >= atLeast whose line equals s.
func (ix *lineIndex) lookup(lines []string, s string, atLeast int) (int, bool) {
	h := maphash.String(lineHashSeed, s)
	epoch := uint64(ix.epoch & lineSlotEpochMask)
	slots := ix.slots
	next := ix.next
	mask := uint64(len(slots) - 1)
	j := h & mask
	for {
		sl := slots[j]
		if sl&lineSlotEpochMask != epoch {
			return 0, false
		}
		if sl>>lineSlotTagShift == h>>40 {
			for pos := int32(uint32(sl >> lineSlotPosShift)); pos >= 0; pos = next[pos] {
				if int(pos) >= atLeast && lines[pos] == s {
					return int(pos), true
				}
			}
			return 0, false
		}
		j = (j + 1) & mask
	}
}

// le64 loads 8 bytes as a little-endian word for fast prefix comparison.
// binary.LittleEndian.Uint64 compiles to a single load on little-endian
// architectures.
func le64(b []byte) uint64 { return binary.LittleEndian.Uint64(b) }

// Size‑selection thresholds used by computeAddedHunks.
const (
	// SmallFileThreshold is 1 MB (1 << 20). Files at or below this size are
	// diffed with the position‑tracking algorithm, which preserves line
	// ordering information for the most accurate output.
	SmallFileThreshold = 1 << 20 // 1 MB

	// MediumFileThreshold is 50 MB (50 << 20). Files larger than
	// SmallFileThreshold and up to this limit are diffed with the memory‑
	// optimized line‑set algorithm.
	MediumFileThreshold = 50 << 20 // 50 MB

	// LargeFileThreshold is 500 MB (500 << 20). Files whose size exceeds
	// MediumFileThreshold and is at or below this limit trigger the hash‑
	// based algorithm, which stores only 64‑bit hashes of each line.
	LargeFileThreshold = 500 << 20 // 500 MB

	// MaxDiffSize is 1 GB (1 << 30). If a blob consulted by computeAddedHunks
	// is larger than this limit, the diff is skipped and a single placeholder
	// hunk is returned instead.
	//
	// Because blobs are loaded lazily (new side first, old side only when a
	// text diff is actually needed), the limit is enforced per side as each
	// blob is loaded. Two short-circuit paths never consult the old blob and
	// therefore never apply the limit to it:
	//
	//   - Pure deletions (zero new OID) return no hunks without loading
	//     either blob.
	//   - A binary new blob at or below the limit is returned as a single
	//     binary hunk without loading the old blob, even if the old version
	//     was larger than the limit.
	MaxDiffSize = 1 << 30 // 1 GB
)

// AddedHunk represents a contiguous block of added lines in a diff.
// The struct groups consecutive lines that were added to a file, tracking
// both the content and position of the additions.
type AddedHunk struct {
	// Lines contains the actual text content of the added lines.
	// Each string represents one line without its trailing newline character.
	// For binary files, this will contain a single element with the raw binary data.
	Lines []string

	// StartLine indicates the 1-based line number where this hunk begins in the new file.
	// Line numbers start at 1 to match standard diff output conventions.
	// For binary files, this is always 1.
	StartLine uint32

	// IsBinary indicates whether this hunk contains binary data.
	// When true, Lines contains the raw binary content as a single string.
	IsBinary bool
}

// EndLine returns the 1-based line number of the last line in this hunk.
// If the hunk contains no lines, EndLine returns the StartLine value.
// For a hunk starting at line 10 with 3 lines, EndLine returns 12.
func (h *AddedHunk) EndLine() uint32 {
	if len(h.Lines) == 0 {
		return h.StartLine
	}
	return h.StartLine + uint32(len(h.Lines)) - 1
}

// tokenize splits a byte slice into individual lines without copying the underlying data.
// The function recognizes '\n' as the line delimiter and excludes it from the results.
// Empty input returns nil rather than an empty slice.
//
// Shared-memory safety invariant: the returned strings are created via btostr
// (unsafe.go), which performs a zero-copy cast from []byte to string using
// unsafe.String. The returned strings alias the memory of src. This is safe
// only as long as src is not mutated after this call. If src's backing array is
// modified, the previously returned strings will silently reflect the mutation,
// violating Go's string immutability guarantee.
func tokenize(src []byte) []string {
	if len(src) == 0 {
		return nil
	}

	// bytes.Count and bytes.IndexByte dispatch to vectorized assembly
	// (NEON/AVX2), so both the counting pass and the split loop run at
	// multiple bytes per cycle instead of the one-byte-per-iteration range
	// loop this previously used.
	lineCount := bytes.Count(src, nlByte) + 1

	lines := make([]string, 0, lineCount)
	rest := src
	for {
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			break
		}
		lines = append(lines, btostr(rest[:i])) // Exclude the newline character.
		rest = rest[i+1:]
	}
	if len(rest) > 0 { // Handle the last line without a newline.
		lines = append(lines, btostr(rest))
	}
	return lines
}

// fuseHunks merges consecutive AddedHunks when the number of untouched lines
// between them is less than or equal to 2*ctx + inter.
//
// ctx specifies the amount of ordinary "context" you intend to display around
// each hunk, while inter represents an additional "inter-hunk" allowance
// (equivalent to Git's --inter-hunk-context flag).
// The function never inserts the untouched lines into the resulting hunks; it
// only extends the range metadata and concatenates the added lines.
//
// EndLine() semantics caveat: the gap between two hunks is computed as
//
//	hunks[i].StartLine - cur.EndLine() - 1
//
// Because fuseHunks concatenates Lines from the merged hunk into cur without
// inserting the skipped (untouched) lines, cur.EndLine() after a merge will be
// less than hunks[i+1].StartLine by the number of omitted lines. This means
// the merged hunk's EndLine() no longer reflects the true last line number in
// the new file -- it reflects StartLine + len(Lines) - 1, which undercounts
// when untouched lines were elided. Callers that need accurate final line
// numbers should recompute them from StartLine and the actual line count.
func fuseHunks(hunks []AddedHunk, ctx, inter int) []AddedHunk {
	if len(hunks) < 2 {
		return hunks
	}
	maxGap := 2*ctx + inter
	out := make([]AddedHunk, 0, len(hunks))
	cur := hunks[0]

	for i := 1; i < len(hunks); i++ {
		gap := int(hunks[i].StartLine) - int(cur.EndLine()) - 1
		if gap <= maxGap {
			// Merge – we *do not* insert the untouched lines into Lines,
			// we just extend the range & byte count.
			cur.Lines = append(cur.Lines, hunks[i].Lines...)
		} else {
			out = append(out, cur)
			cur = hunks[i]
		}
	}
	out = append(out, cur)
	return out
}

// isBinary reports whether the first 8 KiB of data contains a null byte,
// which is a strong indicator that the blob is a binary file.
//
// This matches Git's own heuristic (see buffer_is_binary() in xdiff-interface.c),
// which considers a file binary if a NUL byte appears within the first 8000
// bytes. Our threshold of 8192 is slightly larger but functionally equivalent
// for the overwhelming majority of real-world files.
func isBinary(data []byte) bool {
	checkSize := min(len(data), 8192)
	return bytes.IndexByte(data[:checkSize], 0) != -1
}

// computeAddedHunks returns the contiguous blocks of lines that are present in
// newOID but not in oldOID.
//
// The routine short-circuits in this order, loading each blob only when a
// later step needs it:
//
//  1. Return nil for a pure deletion (newOID zero) or a mode-only change
//     (oldOID == newOID); neither blob is loaded.
//  2. Load the new blob. Emit a placeholder if it exceeds MaxDiffSize, or a
//     single binary hunk if it is binary. In the binary case the old blob is
//     never loaded, so an old version above MaxDiffSize does not produce a
//     placeholder here — the size limit applies per side, as each blob is
//     loaded.
//  3. For a pure addition (oldOID zero), tokenize the new blob.
//  4. Otherwise load the old blob: a placeholder if it exceeds MaxDiffSize,
//     one binary hunk on a binary→text transition, else a size-selected
//     text diff.
//
// The returned slice is nil when there are no additions, or contains at least
// one hunk (possibly a single placeholder line) in every other case.
func computeAddedHunks(store *store, oldOID, newOID Hash) ([]AddedHunk, error) {
	// Pure deletion (or nothing at all): no added-line side exists, so the
	// blobs never need to be loaded. This matters for history walks where
	// large binaries get deleted — inflating a multi-MB blob only to
	// discard it dominated whole-scan tail latency.
	//
	// A deletion of a file larger than MaxDiffSize therefore yields no hunks
	// rather than the "[File too large to diff]" placeholder: an added-lines
	// diff of a pure deletion has no added side to show.
	if newOID.IsZero() {
		return nil, nil
	}
	// Identical content on both sides (mode-only change, e.g. chmod):
	// content addressing guarantees an empty diff without loading either
	// blob.
	if oldOID == newOID {
		return nil, nil
	}

	// Load only the NEW side first. Whenever the new blob is binary the
	// result is a single binary hunk carrying the new content — the old
	// blob's bytes are never consulted (content inequality is already
	// guaranteed by oldOID != newOID under content addressing). Deferring
	// the old-side load until it is actually needed means each version of
	// a large checked-in binary is inflated exactly once per scan (as the
	// "new" side), instead of again as the "old" side of the next
	// transition — that redundant multi-MB inflation dominated whole-scan
	// tail latency.
	newBytes, err := loadBlob(store, newOID)
	if err != nil {
		return nil, fmt.Errorf("getting new blob: %w", err)
	}
	newSize := int64(len(newBytes))

	// Hard size limit — users would rather see a placeholder than wait.
	if newSize > MaxDiffSize {
		placeholder := AddedHunk{
			StartLine: 1,
			Lines:     []string{fmt.Sprintf("[File too large to diff: new=%d bytes]", newSize)},
			IsBinary:  false,
		}
		return []AddedHunk{placeholder}, nil
	}

	if len(newBytes) > 0 && isBinary(newBytes) {
		// btostr avoids copying the blob (which may be tens of MB for
		// checked-in binaries); newBytes comes from the object store whose
		// buffers are immutable by contract, matching the zero-copy
		// convention tokenize already uses for text.
		//
		// The old blob is intentionally not loaded (or size-checked) on
		// this path: its bytes cannot change the output — the hunk carries
		// only the new content — and checking its size would require
		// inflating it, reinstating the redundant old-side inflation this
		// lazy-load ordering exists to avoid. Consequently a >MaxDiffSize
		// old version rewritten to a within-limit binary new version emits
		// the binary hunk, not the "[File too large to diff]" placeholder.
		hunk := AddedHunk{
			StartLine: 1,
			Lines:     []string{btostr(newBytes)},
			IsBinary:  true,
		}
		return []AddedHunk{hunk}, nil
	}

	// Pure addition of a text file: everything in newBytes is new.
	if oldOID.IsZero() {
		lines := tokenize(newBytes)
		if len(lines) == 0 {
			return nil, nil // Empty file added.
		}
		hunk := AddedHunk{StartLine: 1, Lines: lines, IsBinary: false}
		return []AddedHunk{hunk}, nil
	}

	// New side is text; the old side is now needed for the line diff (or
	// to detect a binary→text transition).
	oldBytes, err := loadBlob(store, oldOID)
	if err != nil {
		return nil, fmt.Errorf("getting old blob: %w", err)
	}
	oldSize := int64(len(oldBytes))

	if oldSize > MaxDiffSize {
		placeholder := AddedHunk{
			StartLine: 1,
			Lines:     []string{fmt.Sprintf("[File too large to diff: old=%d new=%d bytes]", oldSize, newSize)},
			IsBinary:  false,
		}
		return []AddedHunk{placeholder}, nil
	}

	if len(oldBytes) > 0 && isBinary(oldBytes) {
		// Binary → text transition: report the whole new content as one
		// binary-style hunk, matching the previous mixed-change behavior.
		hunk := AddedHunk{
			StartLine: 1,
			Lines:     []string{btostr(newBytes)},
			IsBinary:  true,
		}
		return []AddedHunk{hunk}, nil
	}

	// Both files are text — choose the diff algorithm by size.
	if oldSize <= SmallFileThreshold && newSize <= SmallFileThreshold {
		return addedHunksWithPos(oldBytes, newBytes), nil
	}

	return addedHunksForLargeFiles(oldBytes, newBytes), nil
}

// loadBlob retrieves the raw content of a single object ID from store.
// A zero Hash yields a nil slice without touching the store.
func loadBlob(s *store, oid Hash) ([]byte, error) {
	if oid.IsZero() {
		return nil, nil
	}
	b, _, err := s.get(oid)
	return b, err
}

// addedHunksWithPos compares two byte slices and identifies contiguous blocks of added lines.
// The function performs a line-by-line comparison between oldB and newB to find additions.
// It groups consecutive added lines into hunks for efficient diff representation.
//
// IMPORTANT: this is a greedy forward-matching heuristic, NOT a standard diff
// algorithm (e.g., Myers or patience diff). It walks the new lines in order and,
// for each new line, attempts to find the earliest matching line at or after the
// current position in the old file. Lines in newB that cannot be matched are
// reported as additions. This greedy strategy may over-report additions when
// lines are reordered rather than truly added, but it runs in O(N) or O(N log N)
// time without the quadratic worst case of a full edit-distance computation.
//
// The algorithm uses an optimized lookup strategy: for files with more than 50 lines,
// it builds a hash map for O(1) line lookups; for smaller files, it uses linear search.
// This adaptive approach balances memory usage and performance based on file size.
//
// Returns nil if the byte slices are identical.
// Returns a slice of AddedHunk structs representing all additions found in newB.
func addedHunksWithPos(oldB, newB []byte) []AddedHunk {
	// First, check for the trivial case where the files are identical.
	if bytes.Equal(oldB, newB) {
		return nil
	}

	// Skip the common byte prefix, snapped back to a line boundary, before
	// tokenizing. Consecutive versions of a file usually share a large
	// unchanged head; the greedy matcher below consumes identical prefixes
	// in lockstep without emitting hunks, so eliding them wholesale is
	// output-preserving and avoids tokenizing (and later map-indexing) the
	// bytes that dominate typical inputs. Line numbering is recovered by
	// counting the skipped newlines.
	var skippedLines int
	if p := commonPrefixLineBoundary(oldB, newB); p > 0 {
		skippedLines = bytes.Count(oldB[:p], nlByte)
		oldB, newB = oldB[p:], newB[p:]
	}

	// Tokenize the old and new byte slices into lines for comparison.
	// This is a zero-copy operation, creating string views into the original slices.
	oldLines, newLines := tokenize(oldB), tokenize(newB)

	// For larger files, a hash index over old lines provides O(1) lookups.
	// It is built lazily at the first mismatch: prefix-trimmed inputs
	// frequently diverge only near the tail, and eagerly indexing lines the
	// lockstep walk would consume anyway wastes the dominant cost of this
	// function. The pooled flat-chain structure avoids the per-line slice
	// allocations of the previous map[string][]uint32 design.
	const threshold = 50
	var oldIndex *lineIndex
	defer func() {
		if oldIndex != nil {
			putLineIndex(oldIndex)
		}
	}()

	var hunks []AddedHunk
	hunkStart := -1
	hunkStartLine := uint32(0)
	flushHunk := func(end int) {
		if hunkStart < 0 {
			return
		}
		hunks = append(hunks, AddedHunk{
			StartLine: hunkStartLine,
			Lines:     append([]string(nil), newLines[hunkStart:end]...),
		})
		hunkStart = -1
	}
	oldIdx := 0

	for newIdx, newLine := range newLines {
		lineNum := uint32(newIdx+skippedLines) + 1
		isAdded := false

		// If we've exhausted all lines in the old file,
		// any remaining lines in the new file are additions.
		if oldIdx >= len(oldLines) {
			isAdded = true
		} else if newLine != oldLines[oldIdx] {
			// The lines do not match. We need to determine if the new line is an addition
			// or if it's a line that was moved from later in the old file.
			foundLater := false

			if len(oldLines)-oldIdx > threshold {
				if oldIndex == nil {
					// Positions before oldIdx can never satisfy the
					// pos >= oldIdx filter below, so indexing from the
					// first mismatch position loses nothing.
					oldIndex = lineIndexPool.Get().(*lineIndex)
					oldIndex.build(oldLines, oldIdx)
				}
				if pos, ok := oldIndex.lookup(oldLines, newLine, oldIdx); ok {
					// Found at or after our current position: fast-forward,
					// treating the lines between oldIdx and pos as deleted.
					foundLater = true
					oldIdx = pos
				}
			} else {
				// For smaller remainders, perform a linear search to find the line.
				for j := oldIdx; j < len(oldLines); j++ {
					if newLine == oldLines[j] {
						foundLater = true
						oldIdx = j
						break
					}
				}
			}

			// If the line was not found later in the old file,
			// it's a genuine addition.
			if !foundLater {
				isAdded = true
			}
		}

		if isAdded {
			// Start a compact hunk view; clone its line headers once when
			// the contiguous added run ends.
			if hunkStart < 0 {
				hunkStart = newIdx
				hunkStartLine = lineNum
			}
		} else {
			// The line is not an addition;
			// it matches the corresponding line in the old file.
			// If we were building a hunk, it's now complete.
			flushHunk(newIdx)
			oldIdx++
		}
	}

	// After the loop, if there's still an open hunk, add it to the list.
	// This handles cases where the file ends with a block of added lines.
	flushHunk(len(newLines))

	return hunks
}

// nlByte is '\n' as a []byte, for the bytes.Count newline scans.
var nlByte = []byte{'\n'}

// commonPrefixLineBoundary returns the length of the longest common byte
// prefix of a and b that ends immediately after a '\n'. Returning a
// line-aligned boundary guarantees the remainders start at the beginning of
// a line on both sides, so line-based diffing of the remainders is
// equivalent to diffing the full inputs.
func commonPrefixLineBoundary(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	// Word-at-a-time comparison; falls back to byte steps near the end.
	for i+8 <= n && le64(a[i:]) == le64(b[i:]) {
		i += 8
	}
	for i < n && a[i] == b[i] {
		i++
	}
	if i == 0 {
		return 0
	}
	// Snap back to just after the last newline within the common prefix.
	if j := bytes.LastIndexByte(a[:i], '\n'); j >= 0 {
		return j + 1
	}
	return 0
}

// addedHunksForLargeFiles chooses between the line‑set and hash‑based
// algorithms based on the size of the input blobs.
func addedHunksForLargeFiles(oldBytes, newBytes []byte) []AddedHunk {
	oldSize, newSize := int64(len(oldBytes)), int64(len(newBytes))

	if oldSize > LargeFileThreshold || newSize > LargeFileThreshold {
		return addedHunksWithHashing(oldBytes, newBytes)
	}
	return addedHunksWithLineSet(oldBytes, newBytes)
}

// addedHunksWithHashing diff-computes added hunks by hashing each line with
// Farm Hash64 and storing only the hashes of the old file. This keeps memory
// usage proportional to the number of unique lines rather than their total
// length.
//
// Hash collision risk: because the comparison uses 64-bit FarmHash digests
// rather than full line content, there is a small but nonzero probability of
// hash collisions. A collision causes a genuinely added line to be mistakenly
// considered present in the old file, resulting in a false negative (the line
// is silently omitted from the diff output). With 64-bit hashes the birthday-
// bound collision probability reaches ~50% at roughly 2^32 (~4 billion) unique
// lines, which is far beyond typical inputs. For correctness-critical callers,
// use addedHunksWithLineSet or addedHunksWithPos instead.
func addedHunksWithHashing(oldBytes, newBytes []byte) []AddedHunk {
	oldLines := tokenize(oldBytes)
	oldLineHashes := make(map[uint64]struct{}, len(oldLines))
	for _, line := range oldLines {
		oldLineHashes[farm.Hash64([]byte(line))] = struct{}{}
	}
	oldLines = nil // Free memory.

	newLines := tokenize(newBytes)

	var hunks []AddedHunk
	var cur *AddedHunk

	for i, line := range newLines {
		lineNum := uint32(i) + 1
		if _, exists := oldLineHashes[farm.Hash64([]byte(line))]; !exists {
			if cur == nil || lineNum != cur.EndLine()+1 {
				if cur != nil {
					hunks = append(hunks, *cur)
				}
				cur = &AddedHunk{StartLine: lineNum}
			}
			cur.Lines = append(cur.Lines, line)
		} else if cur != nil {
			hunks = append(hunks, *cur)
			cur = nil
		}
	}

	if cur != nil {
		hunks = append(hunks, *cur)
	}
	return hunks
}

// addedHunksWithLineSet diff‑computes added hunks by placing every unique
// line of the old file in a map. The map provides fast existence checks while
// avoiding position tracking, making this strategy suitable for medium‑sized
// inputs that do not warrant full hashing.
func addedHunksWithLineSet(oldBytes, newBytes []byte) []AddedHunk {
	oldLines := tokenize(oldBytes)
	oldLineSet := make(map[string]struct{}, len(oldLines))
	for _, line := range oldLines {
		oldLineSet[line] = struct{}{}
	}
	oldLines = nil // Free memory.

	newLines := tokenize(newBytes)

	var hunks []AddedHunk
	var cur *AddedHunk

	for i, line := range newLines {
		lineNum := uint32(i) + 1
		if _, exists := oldLineSet[line]; !exists {
			if cur == nil || lineNum != cur.EndLine()+1 {
				if cur != nil {
					hunks = append(hunks, *cur)
				}
				cur = &AddedHunk{StartLine: lineNum}
			}
			cur.Lines = append(cur.Lines, line)
		} else if cur != nil {
			hunks = append(hunks, *cur)
			cur = nil
		}
	}

	if cur != nil {
		hunks = append(hunks, *cur)
	}
	return hunks
}
