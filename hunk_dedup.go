// hunk_dedup.go
//
// First-introduction line dedup for hunk scans (WithHunkLineDedup).
//
// The default hunk pipeline (history_scanner.go) emits every added hunk of
// every commit; a line that is re-added, moved, or replayed through history
// is emitted once per occurrence. This file implements the opt-in dedup
// policy at whole-hunk granularity: a hunk is emitted intact iff it
// contains at least one line not seen earlier in a deterministic total
// order over the history:
//
//	(commit parent-first index, blob-pair index within commit,
//	 hunk index, line index)
//
// Hunks are never split: emitting whole hunks guarantees a multi-line
// secret contiguous within one hunk is never truncated by dedup, even when
// its boundary lines duplicate previously-seen text (sub-hunk emission
// decapitates that class of secrets by construction, so it is not offered).
//
// Dedup verdicts depend on the order lines are observed, so the pipeline
// must impose that total order on decisions without serializing the
// expensive stages. The shape is four stages, deadlock-free by
// construction because no stage ever blocks waiting on something
// downstream of itself:
//
//  1. Ordered commit source: orderCommitsParentFirst(loadAllCommits()),
//     dispatched by the sequencer with a bounded look-ahead window.
//  2. Parallel tree diff: workers resolve each dispatched commit's pair
//     list into a per-commit ring slot (no channel sends, so tree workers
//     can never block).
//  3. Sequencer (the calling goroutine): walks commits in order, waits for
//     each slot, and forwards pairs to blobChan stamped with a monotonic
//     seq. Dispatch order == seq order, so in-flight seqs are bounded by
//     cap(blobChan) + the hunk-worker count + cap(resultChan), and the
//     reorder buffer cannot grow without bound.
//  4. Parallel hunk workers -> serial decision stage -> parallel yield:
//     hunk workers compute each pair's hunks into a slice and hand
//     (seq, hunks) to the decision goroutine, which reorders by seq,
//     applies the fingerprint table's whole-hunk verdicts (serial, cheap),
//     and fans surviving hunks out to a pool of yield workers that invoke
//     fn concurrently. Only dedup decisions are serialized; output is
//     deterministic as a multiset, never as an order.
//
// Cross-file dependencies:
//   - firstParentTree, emitCommitBlobPairs, streamBlobPairHunks
//     (history_scanner.go): the reused diff machinery.
//   - orderCommitsParentFirst (commit_order.go): the deterministic order.
//   - farm.Hash64: the line fingerprint hash, matching the existing
//     line-hashing convention in diff_blob.go.

package objstore

import (
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/dgryski/go-farm"
)

const (
	// Production tables start at 256 KiB and grow on demand. The default
	// 32 MiB ceiling holds roughly 2.9M unique lines at the 0.7 load factor.
	dedupInitialSlotsLog2  = 15
	defaultHunkDedupBudget = 32 << 20

	// dedupZeroFingerprint replaces a raw fingerprint of zero, which the
	// open-addressing table reserves as its empty-slot sentinel. Lines
	// hashing to zero and to this constant alias each other; with a
	// 64-bit hash the aliasing probability is negligible and costs at
	// most one suppressed emission.
	dedupZeroFingerprint = 0x9e3779b97f4a7c15

	// dedupLookaheadCommits bounds how many commits ahead of the
	// sequencer's cursor may be tree-diffed concurrently. It is also the
	// size of the per-commit slot ring: an index is dispatched only while
	// it is fewer than dedupLookaheadCommits ahead of the cursor, so a
	// ring slot is never reused before the sequencer has consumed its
	// previous occupant.
	dedupLookaheadCommits = 256

	// dedupBlobChanCap, together with the hunk-worker count and
	// cap(resultChan), bounds the number of in-flight seqs and therefore
	// the decision stage's reorder buffer. It is sized well below the
	// streaming pipeline's blobChan depth because measurements showed
	// reorder-buffer memory scales with this cap while wall time is
	// insensitive to it.
	dedupBlobChanCap = 512

	// dedupYieldChanCap decouples the serial decision stage from fn
	// scheduling: a whale pair can release thousands of hunks at once, and
	// a shallow buffer would stall the decision loop (and transitively
	// the reorder release) on every fn hiccup.
	dedupYieldChanCap = 256
)

// lineFingerprint hashes one added line (without its trailing newline —
// tokenize already strips it) to the 64-bit fingerprint used for dedup.
// FarmHash matches the package's existing line-hashing convention
// (addedHunksWithHashing in diff_blob.go).
func lineFingerprint(line string) uint64 {
	return farm.Hash64([]byte(line))
}

// lineFingerprintSet is a fixed-capacity open-addressing set of 64-bit line
// fingerprints with linear probing. Zero is the empty-slot sentinel;
// fingerprints of zero are remapped to dedupZeroFingerprint.
//
// The set is used from a single goroutine (the decision stage), so it needs
// no atomics: dedup decisions are serialized by pipeline design, which also
// makes the insert order — and therefore the saturation point — fully
// deterministic.
//
// When the insert count reaches the load-factor bound the set saturates and
// fails open: markNew reports every subsequent fingerprint as new, so lines
// are emitted rather than dropped and lookup chains never degrade.
type lineFingerprintSet struct {
	// slots holds the fingerprints; a zero slot is empty.
	slots []uint64

	// mask is len(slots)-1 for power-of-two index wrapping.
	mask uint64

	// remaining counts inserts left before the load-factor bound.
	remaining int
	count     int
	maxSlots  int

	// saturated is set once remaining hits zero; from then on markNew
	// returns true unconditionally (fail-open).
	saturated bool
}

// newLineFingerprintSet returns a set with 2^log2Slots slots that saturates
// at a 0.7 load factor. Tests use small log2Slots values to exercise
// saturation cheaply; production uses dedupTableSlotsLog2.
func newLineFingerprintSet(log2Slots uint) *lineFingerprintSet {
	n := 1 << log2Slots
	return &lineFingerprintSet{
		slots:     make([]uint64, n),
		mask:      uint64(n - 1),
		remaining: n * 7 / 10,
		maxSlots:  n,
	}
}

func newLineFingerprintSetWithBudget(log2Initial uint, budget int) *lineFingerprintSet {
	maxSlots := budget / 8
	if maxSlots < 2 {
		maxSlots = 2
	}
	maxPower := 1
	for maxPower <= maxSlots/2 {
		maxPower <<= 1
	}
	initial := 1 << log2Initial
	if initial > maxPower {
		initial = maxPower
	}
	return &lineFingerprintSet{
		slots:     make([]uint64, initial),
		mask:      uint64(initial - 1),
		remaining: initial * 7 / 10,
		maxSlots:  maxPower,
	}
}

func (s *lineFingerprintSet) grow() bool {
	if len(s.slots) >= s.maxSlots {
		return false
	}
	newLen := min(len(s.slots)*2, s.maxSlots)
	old := s.slots
	s.slots = make([]uint64, newLen)
	s.mask = uint64(newLen - 1)
	for _, fp := range old {
		if fp == 0 {
			continue
		}
		i := fp & s.mask
		for s.slots[i] != 0 {
			i = (i + 1) & s.mask
		}
		s.slots[i] = fp
	}
	s.remaining = newLen*7/10 - s.count
	return true
}

// markNew records fp and reports whether it was absent (i.e. the line is
// new). After saturation it reports true for every fingerprint.
func (s *lineFingerprintSet) markNew(fp uint64) bool {
	if s.saturated {
		return true
	}
	if fp == 0 {
		fp = dedupZeroFingerprint
	}
	i := fp & s.mask
	for {
		switch s.slots[i] {
		case fp:
			return false
		case 0:
			s.slots[i] = fp
			s.count++
			s.remaining--
			if s.remaining <= 0 {
				s.saturated = !s.grow()
			}
			return true
		}
		i = (i + 1) & s.mask
	}
}

// has reports whether fp is already in the set, without inserting it.
// After saturation it reports false for every fingerprint so verdicts stay
// fail-open (hunks are emitted rather than suppressed), mirroring markNew.
func (s *lineFingerprintSet) has(fp uint64) bool {
	if s.saturated {
		return false
	}
	if fp == 0 {
		fp = dedupZeroFingerprint
	}
	i := fp & s.mask
	for {
		switch s.slots[i] {
		case fp:
			return true
		case 0:
			return false
		}
		i = (i + 1) & s.mask
	}
}

// dedupHunkEmission decides whether one hunk survives dedup, marking every
// line in set as a side effect. It reports true iff at least one line was
// unseen when the hunk was reached; if every line was already seen the hunk
// is suppressed. The caller forwards its own HunkAddition on a true
// verdict, so hunks-emitted-intact is structural: this function cannot
// return a modified hunk, and hunks are never split.
//
// Verdicts for all lines are computed against the set state as of the start
// of the hunk (probe first, mark after), so duplicate lines inside one hunk
// cannot suppress each other or affect this hunk's verdict: a block whose
// closing line repeats an earlier line of the same hunk — armored key
// blocks sharing an END marker — still counts every occurrence as new.
//
// Binary hunks bypass dedup entirely: they always survive and never mark
// the set.
func dedupHunkEmission(h HunkAddition, set *lineFingerprintSet) bool {
	if h.isBinary {
		return true
	}

	anyNew := false
	for _, line := range h.lines {
		if !set.has(lineFingerprint(line)) {
			anyNew = true
			break
		}
	}
	for _, line := range h.lines {
		set.markNew(lineFingerprint(line))
	}
	return anyNew
}

// dedupCommitSlot is one ring entry the tree-diff stage fills for the
// sequencer. Ownership alternates strictly: the sequencer arms done and
// publishes the slot via treeIdxChan; exactly one tree worker overwrites
// pairs and err and closes done; the sequencer reads them after <-done. The
// channel send and close provide the necessary happens-before edges, so no
// lock is needed.
type dedupCommitSlot struct {
	// pairs is the commit's blob-pair list in deterministic tree order.
	pairs []blobPairWork

	// err is the tree-stage failure for this commit, set before done is
	// closed.
	err error

	// done is closed by the tree worker once pairs/err are final.
	done chan struct{}
}

// dedupPairWork is one blob pair stamped with its global sequence number.
type dedupPairWork struct {
	seq  uint64
	work blobPairWork
}

// dedupPairResult carries one pair's computed hunks back to the decision
// stage. hunks may be empty; every dispatched seq produces exactly one
// result on the happy path so the reorder cursor always advances.
type dedupPairResult struct {
	seq   uint64
	hunks []HunkAddition
}

// collectCommitPairs resolves c's first-parent tree and collects the
// commit's changed blob pairs in deterministic tree order. Errors come back
// pre-wrapped with the same message formats the streaming pipeline uses, so
// tree workers only record and propagate them.
func (hs *HistoryScanner) collectCommitPairs(c commitInfo) ([]blobPairWork, error) {
	parentTree, err := hs.firstParentTree(c)
	if err != nil {
		return nil, fmt.Errorf("resolve first-parent tree for commit %s: %w", c.OID, err)
	}
	var pairs []blobPairWork
	if err := hs.emitCommitBlobPairs(c, parentTree, func(w blobPairWork) error {
		pairs = append(pairs, w)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed processing commit %s (tree: %s): %w", c.OID, c.TreeOID, err)
	}
	return pairs, nil
}

// diffHistoryHunksDedup is the WithHunkLineDedup implementation of
// DiffHistoryHunksFunc. See the file header for the pipeline shape and the
// option's doc comment for the emission contract.
//
// Goroutine/channel layout (N = runtime.NumCPU()):
//
//	caller goroutine        sequencer: dispatches slot indices, forwards
//	                        seq-stamped pairs, then runs the shutdown chain
//	tree workers            min(N, maxTreeDiffWorkers), consume treeIdxChan,
//	                        fill slots, never block
//	hunk workers            N, consume blobChan, produce resultChan
//	decision goroutine      1, reorders by seq, owns the fingerprint set,
//	                        produces yieldChan (closed on exit)
//	yield workers           N, consume yieldChan, call fn concurrently
//
// On any error (setError closes stopCh) every stage unblocks via its stopCh
// select and the shutdown chain — close(treeIdxChan), treeWG.Wait(),
// close(blobChan), hunkWG.Wait(), close(resultChan), <-decisionDone,
// yieldWG.Wait() — still runs to completion, so no goroutine outlives the
// call.
func (hs *HistoryScanner) diffHistoryHunksDedup(fn func(HunkAddition) error) error {
	defer hs.stopProfiling() // Ensure profiling is stopped even on error

	if err := hs.startProfiling(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to start profiling: %v\n", err)
	}

	commits, err := hs.loadAllCommits()
	if err != nil {
		return err
	}
	// Publish every commit's tree OID up front so firstParentTree never
	// re-inflates a header. This covers skipped merge commits too: a merge
	// excluded from diffing can still be another commit's first parent.
	for _, c := range commits {
		hs.treeOIDs.Store(c.OID, c.TreeOID)
	}

	// loadAllCommits already returns parent-first order on the ref-walk
	// path, but the commit-graph path materializes rows in on-disk
	// (OID-lexicographic) order, so the dedup pipeline imposes the order
	// itself. orderCommitsParentFirst is idempotent and cheap relative to
	// the scan.
	order := orderCommitsParentFirst(commits)
	if hs.skipMergeDiffs {
		filtered := order[:0]
		for _, c := range order {
			if len(c.ParentOIDs) > 1 {
				continue
			}
			filtered = append(filtered, c)
		}
		order = filtered
	}

	numWorkers := runtime.NumCPU()
	treeWorkers := min(numWorkers, maxTreeDiffWorkers)

	treeIdxChan := make(chan int, dedupLookaheadCommits)
	blobChan := make(chan dedupPairWork, dedupBlobChanCap)
	resultChan := make(chan dedupPairResult, numWorkers)
	yieldChan := make(chan HunkAddition, dedupYieldChanCap)
	stopCh := make(chan struct{})
	slots := make([]dedupCommitSlot, dedupLookaheadCommits)

	var (
		stopOnce sync.Once
		treeWG   sync.WaitGroup
		hunkWG   sync.WaitGroup
		yieldWG  sync.WaitGroup
		firstErr error
	)
	setError := func(err error) {
		if err == nil {
			return
		}
		stopOnce.Do(func() {
			firstErr = err
			close(stopCh)
		})
	}

	for range treeWorkers {
		treeWG.Add(1)
		go func() {
			defer treeWG.Done()
			for {
				select {
				case <-stopCh:
					return
				case idx, ok := <-treeIdxChan:
					if !ok {
						return
					}
					slot := &slots[idx%dedupLookaheadCommits]
					pairs, err := hs.collectCommitPairs(order[idx])
					// err must be visible before done closes: the
					// sequencer reads both after <-slot.done.
					slot.pairs, slot.err = pairs, err
					close(slot.done)
					if err != nil {
						setError(err)
						return
					}
				}
			}
		}()
	}

	for range numWorkers {
		hunkWG.Add(1)
		go func() {
			defer hunkWG.Done()
			for {
				select {
				case <-stopCh:
					return
				case pw, ok := <-blobChan:
					if !ok {
						return
					}
					var hunks []HunkAddition
					if err := hs.streamBlobPairHunks(pw.work, func(h HunkAddition) error {
						hunks = append(hunks, h)
						return nil
					}); err != nil {
						setError(fmt.Errorf("failed diffing %s in commit %s: %w", pw.work.path, pw.work.commit, err))
						return
					}
					select {
					case <-stopCh:
						return
					case resultChan <- dedupPairResult{seq: pw.seq, hunks: hunks}:
					}
				}
			}
		}()
	}

	decisionDone := make(chan struct{})
	go func() {
		defer close(decisionDone)
		defer close(yieldChan)
		set := newLineFingerprintSetWithBudget(dedupInitialSlotsLog2, hs.hunkDedupBudget)
		// In-flight seqs are bounded by cap(blobChan) + numWorkers +
		// cap(resultChan), so the reorder buffer stays small; it only
		// grows toward that bound when pair completion times are skewed.
		pending := make(map[uint64]dedupPairResult, 64)
		next := uint64(0)
		for {
			select {
			case <-stopCh:
				return
			case res, ok := <-resultChan:
				if !ok {
					return
				}
				pending[res.seq] = res
				for {
					r, ok := pending[next]
					if !ok {
						break
					}
					delete(pending, next)
					next++
					for _, h := range r.hunks {
						if !dedupHunkEmission(h, set) {
							continue
						}
						select {
						case <-stopCh:
							return
						case yieldChan <- h:
						}
					}
				}
			}
		}
	}()

	for range numWorkers {
		yieldWG.Add(1)
		go func() {
			defer yieldWG.Done()
			for {
				select {
				case <-stopCh:
					return
				case h, ok := <-yieldChan:
					if !ok {
						return
					}
					if err := fn(h); err != nil {
						setError(err)
						return
					}
				}
			}
		}()
	}

	// Sequencer: runs on the calling goroutine. dispatch keeps up to
	// dedupLookaheadCommits commits in flight ahead of the cursor; the
	// wait-then-forward loop below imposes the global pair order.
	nextDispatch := 0
	dispatch := func(cursor int) bool {
		for nextDispatch < len(order) && nextDispatch-cursor < dedupLookaheadCommits {
			slot := &slots[nextDispatch%dedupLookaheadCommits]
			// Re-arm only the done channel: the tree worker overwrites
			// pairs and err wholesale (adopting collectCommitPairs'
			// returned slice) before closing done, and the ring slot's
			// previous occupant (nextDispatch - dedupLookaheadCommits)
			// was fully consumed before the window allowed this dispatch.
			slot.done = make(chan struct{})
			select {
			case <-stopCh:
				return false
			case treeIdxChan <- nextDispatch:
			}
			nextDispatch++
		}
		return true
	}

	var seq uint64
seqLoop:
	for i := range order {
		if !dispatch(i) {
			break
		}
		slot := &slots[i%dedupLookaheadCommits]
		select {
		case <-stopCh:
			break seqLoop
		case <-slot.done:
		}
		if slot.err != nil {
			// The tree worker already routed the error through setError.
			break
		}
		for _, w := range slot.pairs {
			select {
			case <-stopCh:
				break seqLoop
			case blobChan <- dedupPairWork{seq: seq, work: w}:
				seq++
			}
		}
	}

	// Shutdown chain: close each stage's input only after its upstream
	// senders have exited, and wait for every goroutine so none outlives
	// this call — on the error path stopCh has every stage unblocked.
	close(treeIdxChan)
	treeWG.Wait()
	close(blobChan)
	hunkWG.Wait()
	close(resultChan)
	<-decisionDone
	yieldWG.Wait()

	return firstErr
}
