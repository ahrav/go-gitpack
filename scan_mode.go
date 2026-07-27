// scan_mode.go
//
// Scanning strategy selection for HistoryScanner.
//
// Two modes are supported:
//   - ScanModeBlob  (default) -- iterates every unique blob introduced across
//     the commit history in pack-file offset order, yielding the full blob
//     body exactly once per OID. This is the recommended and fastest path.
//   - ScanModeHunks (legacy)  -- computes per-commit diffs and yields added-line
//     hunks, except for exact-OID moves whose bytes are unchanged. Retained for
//     backward compatibility with callers that require line-level attribution,
//     but significantly slower because it must diff every parent-child commit
//     pair.
package objstore

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
)

// ScanMode selects the high-level scanning strategy used by HistoryScanner.Scan.
type ScanMode uint8

const (
	// ScanModeBlob scans full blob objects, deduplicating by OID and visiting
	// them in pack-file offset order. Pack-sorted iteration minimizes random
	// I/O because entries stored contiguously in the pack are read
	// sequentially, which is especially beneficial on spinning disks and
	// over NFS.
	ScanModeBlob ScanMode = iota

	// ScanModeHunks is the legacy scanning mode that computes parent-child
	// diffs for every commit and yields added-line hunks. An entry's added
	// lines are suppressed when the same commit has an unmatched deletion of
	// the same blob identity -- the blob OID plus the tree entry's type, so a
	// regular file never pairs with a symlink; matching is one-for-one because
	// the content-addressed bytes are unchanged. This covers both a pure
	// addition and a move that overwrites a tracked destination, which git
	// reports as a modification whose resulting blob is the deleted one.
	// Consequently an exact-OID move has no hunk attributed to its destination
	// path or moving commit. When one deletion has several same-identity
	// candidates (a rename plus a copy of the same bytes), one is suppressed
	// and the rest are emitted; which path survives follows tree order, so the
	// bytes are reported once but the surviving path is not git's rename
	// heuristic.
	//
	// A type change at one path is a deletion plus an addition, and their
	// identities differ in the type nibble, so the two sides never cancel each
	// other even when they share one OID -- a regular file holding "target"
	// replaced by a symlink to "target" reports the symlink's target. The
	// arriving side is still an ordinary candidate for the one-for-one rule
	// above: a deletion elsewhere in the same commit that matches its identity
	// can claim it, and tree order decides which candidate that credit
	// silences.
	//
	// Known gap: only a DELETION mints a suppression credit -- an entry that
	// leaves the tree, which includes the old side of a type transition
	// because walkDiff splits that into a deletion plus an addition. An
	// in-place overwrite mints nothing: it is a single entry carrying both
	// OIDs, so the blob it displaces is never credited. A move whose source
	// path is simultaneously reoccupied by an entry of the SAME type
	// therefore still emits its destination as a full addition even though
	// the bytes are unchanged -- a commit that writes dst.txt with src.txt's
	// exact bytes while overwriting src.txt with different content reports
	// dst.txt's whole content as added lines.
	//
	// Widening credits to the old side of same-type modifications is a
	// deliberate non-goal, not an omission: under that rule a commit that
	// swaps two files' contents would emit nothing at all, because each
	// path's new bytes match the bytes the other path displaced.
	//
	// This mode exists for backward compatibility with callers that
	// need line-level granularity. Prefer ScanModeBlob for new integrations
	// because it avoids the overhead of diff computation and tree comparison.
	//
	// Reader shape: a text hunk arrives as a *bytes.Reader over a buffer of
	// its lines joined by '\n'. A binary hunk arrives as a *strings.Reader
	// over the whole payload with no intervening copy, so it aliases
	// object-store memory. Both expose the same io method set, including
	// io.WriterTo, but strings.Reader.WriteTo routes through io.WriteString:
	// when an io.Copy destination does not implement io.StringWriter, the
	// payload is converted with []byte(s), costing one full-size allocation
	// and copy per binary hunk. Read-loop consumers and io.StringWriter
	// sinks avoid that copy; io.Discard is such a sink, so measuring with it
	// hides the conversion.
	ScanModeHunks
)

func (m ScanMode) String() string {
	switch m {
	case ScanModeBlob:
		return "blob"
	case ScanModeHunks:
		return "hunks"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(m))
	}
}

// WithScanMode configures the default mode used by HistoryScanner.Scan.
func WithScanMode(mode ScanMode) ScannerOption {
	return func(hs *HistoryScanner) {
		hs.scanMode = mode
	}
}

// WithSkipMergeDiffs makes hunk scans emit no diffs for merge commits,
// matching `git log -p` semantics. See HistoryScanner.skipMergeDiffs.
func WithSkipMergeDiffs(skip bool) ScannerOption {
	return func(hs *HistoryScanner) {
		hs.skipMergeDiffs = skip
	}
}

// ScanMode returns the scanner's currently configured scan mode.
func (hs *HistoryScanner) ScanMode() ScanMode {
	return hs.scanMode
}

// SetScanMode updates the scanner's scan mode for subsequent Scan calls.
//
// Thread safety: SetScanMode is not safe for concurrent use with Scan. The
// caller must ensure no Scan is in progress when changing the mode.
func (hs *HistoryScanner) SetScanMode(mode ScanMode) {
	hs.scanMode = mode
}

// Scan runs the scanning strategy selected by the scanner's current ScanMode.
//
// Blob mode (ScanModeBlob, the default) is the recommended path for secret
// scanning. It visits every unique blob exactly once, in pack-offset order,
// and passes its full content to scanner.ScanBlob.
//
// Hunk mode (ScanModeHunks) diffs each commit against its first parent and
// yields added lines. It applies ScanModeHunks' one-for-one exact-OID move
// suppression, so unchanged bytes are not re-attributed to the destination
// path or moving commit. It is retained for backward compatibility.
func (hs *HistoryScanner) Scan(seen SeenSet, scanner BlobScanner) error {
	if scanner == nil {
		return fmt.Errorf("scanner is nil")
	}

	switch hs.scanMode {
	case ScanModeBlob:
		return hs.scanBlobsStreaming(seen, scanner)
	case ScanModeHunks:
		return hs.scanHunks(scanner)
	default:
		return fmt.Errorf("unsupported scan mode: %s", hs.scanMode)
	}
}

// maxReusedHunkPayloadBytes bounds the capacity scanHunks carries from one
// hunk to the next. Reuse is what makes the buffer worth having — a history
// of small hunks assembles every payload into the same array — but a single
// whole-file text hunk can reach MaxDiffSize (1 GiB), and Reset keeps the
// grown array. Without a cap, one large hunk early in a scan pins its payload
// until the scan returns, which the per-hunk buffer this replaced did not do.
// Past the cap the buffer is dropped for the GC: re-allocating for the next
// hunk is dwarfed by the cost of having diffed a file that large. 4 MiB
// matches maxPooledLineIndexBytes and the store's maxCacheableSize.
const maxReusedHunkPayloadBytes = 4 << 20 // 4 MiB

// releaseOversizedPayload drops payload's backing array when it has grown past
// maxReusedHunkPayloadBytes, reporting whether it did. Callers must have
// finished reading the assembled bytes: this abandons them to the GC.
func releaseOversizedPayload(payload *bytes.Buffer) bool {
	if payload.Cap() <= maxReusedHunkPayloadBytes {
		return false
	}
	*payload = bytes.Buffer{}
	return true
}

// scanHunks implements the legacy hunk-based scanning mode.
//
// It drives DiffHistoryHunksFunc, so no queue sits between a blob worker and
// scanner.ScanBlob. DiffHistoryHunksFunc invokes the callback concurrently
// from every blob worker, and one mutex serializes the whole per-hunk body.
// Both halves need it: the payload buffer is reused across hunks, which
// BlobScanner permits because an implementation may not retain the reader's
// bytes past the call, and ScanBlob is handed one hunk at a time to match
// every other ScanBlob call site in this package.
//
// Retention is one payload, not the largest payload the scan has seen: a
// buffer grown past maxReusedHunkPayloadBytes is released rather than carried.
//
// A binary hunk bypasses the buffer entirely. Its single line already holds the
// whole new blob, so it is streamed straight to ScanBlob instead of being
// copied into the payload — the case that would otherwise grow the buffer to
// MaxDiffSize on every checked-in binary.
//
// A scan error aborts the walk, and DiffHistoryHunksFunc returns the first
// error observed, whether it came from a scan or from the walk itself.
func (hs *HistoryScanner) scanHunks(scanner BlobScanner) error {
	var (
		mu      sync.Mutex
		payload bytes.Buffer
	)

	return hs.DiffHistoryHunksFunc(func(hunk HunkAddition) error {
		mu.Lock()
		defer mu.Unlock()

		meta := ScanMeta{
			Commit: hunk.commit,
			Path:   hunk.path,
		}

		var err error
		if hunk.isBinary && len(hunk.lines) == 1 {
			// A binary hunk carries the whole new blob as its single line, a
			// zero-copy view of the store's buffer. Assembling it into payload
			// would copy the entire file and then immediately release the
			// grown array; reading the string directly skips both.
			err = scanner.ScanBlob(strings.NewReader(hunk.lines[0]), meta)
		} else {
			payload.Reset()
			for i, line := range hunk.lines {
				if i > 0 {
					payload.WriteByte('\n')
				}
				payload.WriteString(line)
			}
			err = scanner.ScanBlob(bytes.NewReader(payload.Bytes()), meta)

			// Release an oversized array now that ScanBlob has returned; the
			// reader handed to it does not outlive the call.
			releaseOversizedPayload(&payload)
		}

		if err != nil {
			return fmt.Errorf("scan hunk %s:%s:%d-%d: %w",
				hunk.commit, hunk.path, hunk.startLine, hunk.endLine, err)
		}
		return nil
	})
}
