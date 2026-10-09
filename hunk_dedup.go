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
//     seq, dedupPairBatchSize pairs per message. Dispatch order == seq
//     order, and the sequencer parks on dedupInFlightWindow once
//     dedupMaxInFlightPairs seqs are undecided, so the decision stage's
//     reorder buffer is bounded by construction however long one pair
//     takes.
//  4. Parallel hunk workers -> serial decision stage -> parallel yield:
//     hunk workers compute each pair's hunks and run prefilterHunk against
//     the set's published snapshot, so the decision goroutine receives
//     (seq, hunks, unresolved fingerprints). It reorders by seq, settles
//     each whole-hunk verdict with decideResult (serial, but over the
//     unresolved fingerprints only), pre-processes the backlog with
//     prepBacklog while it waits behind a straggling pair, and fans
//     surviving hunks out in batches to a pool of yield workers that
//     invoke fn concurrently. Only dedup decisions are serialized; output
//     is deterministic as a multiset, never as an order.
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
	"sync/atomic"

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

	// dedupPairBatchSize is how many seq-stamped pairs travel per channel
	// message on blobChan and resultChan. The sequencer is a single
	// producer feeding every hunk worker, and each per-pair send under
	// receiver contention cost several microseconds of its time (1.2s of a
	// 1.4s scan on a 154k-pair repository), so pairs move in batches. The
	// sequencer flushes a partial batch before it would block on a tree
	// slot, so batching never starves the workers.
	dedupPairBatchSize = 16

	// dedupMaxInFlightPairs bounds seqs stamped but not yet decided. The
	// decision stage drains resultChan into its reorder buffer
	// unconditionally (so a straggling pair's result can always arrive),
	// which means the buffer is bounded by what the sequencer stamps; the
	// sequencer parks on dedupInFlightWindow once this many seqs are
	// undecided. 65536 is about half a second of worker throughput on a
	// 32-core host: a single whale pair (a 40 MB binary, a multi-MB
	// generated file) holds the order for 50-100 ms, and parking earlier
	// measurably idled the workers behind it.
	dedupMaxInFlightPairs = 65536

	// dedupBlobChanCap is in batches: 32 batches of dedupPairBatchSize
	// pairs keep every hunk worker supplied through the sequencer's
	// own scheduling gaps without queueing much work ahead of the
	// decision stage.
	dedupBlobChanCap = 32

	// dedupYieldChanCap decouples the serial decision stage from fn
	// scheduling: a whale pair can release thousands of hunks at once, and
	// a shallow buffer would stall the decision loop (and transitively
	// the reorder release) on every fn hiccup. Each element is a batch of
	// surviving hunks, so the channel carries far fewer messages than hunks.
	dedupYieldChanCap = 256

	// dedupYieldBatchHunks is the batch size the decision stage flushes
	// survivors at. One channel send per hunk put the decision goroutine on
	// the critical path under receiver contention (~2µs per contended
	// send); batching amortizes it.
	dedupYieldBatchHunks = 64

	// dedupProbePrefetch is how many unresolved fingerprints
	// verdictUnresolved touches ahead of probing them.
	dedupProbePrefetch = 16
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
// Exactly one goroutine (the decision stage) mutates the set, which keeps
// the insert order — and therefore the growth and saturation points —
// fully deterministic. Other goroutines may concurrently probe a published
// snapshot (see snapshot and fingerprintSnapshot.contains): the writer
// stores slot values with atomic stores and readers load them with atomic
// loads, and slots only ever transition empty -> fingerprint, so a reader
// observes a subset of the writer's current contents.
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

	// published is the snapshot concurrent readers probe. It always refers
	// to the current slots slice; grow replaces it after the new slice is
	// fully populated, and a reader still holding the previous slice sees
	// a frozen, valid subset of the set.
	published atomic.Pointer[fingerprintSnapshot]
}

// fingerprintSnapshot is a read-only view of a lineFingerprintSet's slots
// for concurrent speculative probes. Contents only grow, so a hit is
// authoritative (the fingerprint is in the set) while a miss is only a hint
// (the writer may have inserted it since).
type fingerprintSnapshot struct {
	slots []uint64
	mask  uint64
}

// contains reports whether fp is present in the snapshot. fp must already
// have zero remapped to dedupZeroFingerprint. The probe terminates because
// the writer keeps load below the saturation bound, so every snapshot has
// empty slots.
func (sn *fingerprintSnapshot) contains(fp uint64) bool {
	i := fp & sn.mask
	for {
		v := atomic.LoadUint64(&sn.slots[i])
		if v == fp {
			return true
		}
		if v == 0 {
			return false
		}
		i = (i + 1) & sn.mask
	}
}

// snapshot returns the current published view for concurrent probes.
func (s *lineFingerprintSet) snapshot() *fingerprintSnapshot {
	return s.published.Load()
}

func (s *lineFingerprintSet) publish() {
	s.published.Store(&fingerprintSnapshot{slots: s.slots, mask: s.mask})
}

// newLineFingerprintSet returns a set with 2^log2Slots slots that saturates
// at a 0.7 load factor. Tests use small log2Slots values to exercise
// saturation cheaply; production uses dedupTableSlotsLog2.
func newLineFingerprintSet(log2Slots uint) *lineFingerprintSet {
	n := 1 << log2Slots
	s := &lineFingerprintSet{
		slots:     make([]uint64, n),
		mask:      uint64(n - 1),
		remaining: n * 7 / 10,
		maxSlots:  n,
	}
	s.publish()
	return s
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
	s := &lineFingerprintSet{
		slots:     make([]uint64, initial),
		mask:      uint64(initial - 1),
		remaining: initial * 7 / 10,
		maxSlots:  maxPower,
	}
	s.publish()
	return s
}

func (s *lineFingerprintSet) grow() bool {
	if len(s.slots) >= s.maxSlots {
		return false
	}
	newLen := min(len(s.slots)*2, s.maxSlots)
	old := s.slots
	// The new slice is private until publish, so plain stores are fine;
	// readers keep probing the old slice, which is never written again.
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
	s.publish()
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
			// Atomic store pairs with fingerprintSnapshot.contains' atomic
			// load; the slot transitions exactly once, empty -> fp.
			atomic.StoreUint64(&s.slots[i], fp)
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

// verdictUnresolved settles one text hunk's whole-hunk verdict given the
// fingerprints of its lines that a speculative snapshot probe did NOT find
// (unresolved, in line order; zero already remapped), and lineCount, the
// hunk's total line count.
//
// Equivalence with dedupHunkEmission (which hashes and marks every line):
// a line the snapshot contained is in the set at this hunk's position, so
// markNew would report it seen and insert nothing — skipping it changes
// neither the verdict nor the insert sequence. Saturated at entry, every
// markNew reports true, so the verdict is "non-empty" exactly as the full
// pass computes. Saturation tripped mid-hunk happens only via an insert of
// an unresolved line, which already settled the verdict true.
func (s *lineFingerprintSet) verdictUnresolved(unresolved []uint64, lineCount int) bool {
	if s.saturated {
		return lineCount > 0
	}
	anyNew := false
	for len(unresolved) > 0 {
		n := min(len(unresolved), dedupProbePrefetch)
		// Touch each fingerprint's home slot before probing it. The loads
		// are independent, so the core overlaps their cache misses instead
		// of taking them one at a time inside markNew's dependent probe
		// loop; the table is far larger than L2 and a hunk's lines land on
		// unrelated lines of it.
		var touched uint64
		for _, fp := range unresolved[:n] {
			touched += s.slots[fp&s.mask]
		}
		prefetchSink = touched
		for _, fp := range unresolved[:n] {
			if s.markNew(fp) {
				anyNew = true
			}
		}
		unresolved = unresolved[n:]
	}
	return anyNew
}

// prefetchSink keeps verdictUnresolved's touch loop observable so the
// compiler cannot drop the loads.
var prefetchSink uint64

// decideResult settles every hunk of one pair in order and appends the
// survivors to out.
//
// Only candidates need a verdict: a binary hunk always survives, and a text
// hunk with unresolved fingerprints is probed with verdictUnresolved. A
// text hunk that is not a candidate had every line in the snapshot and is
// therefore a duplicate while the set is unsaturated. Once the set
// saturates (at entry or during a candidate's probe) the single-pass rule
// emits every later non-empty hunk, candidates or not, so the tail of the
// pair is scanned in full from that point.
func (s *lineFingerprintSet) decideResult(r dedupPairResult, out []HunkAddition) []HunkAddition {
	hunks := r.hunks
	i := 0 // hunks[:i] are decided
	for _, c := range r.candidates {
		if s.saturated {
			break
		}
		h := &hunks[c.hunk]
		if h.isBinary || s.verdictUnresolved(c.unresolved, len(h.lines)) {
			out = append(out, *h)
		}
		i = c.hunk + 1
	}
	if s.saturated {
		for ; i < len(hunks); i++ {
			h := &hunks[i]
			if h.isBinary || len(h.lines) > 0 {
				out = append(out, *h)
			}
		}
	}
	return out
}

// contains reports whether fp (zero already remapped) is in the set. It is
// a read-only probe for the owning goroutine; concurrent readers use the
// published snapshot instead.
func (s *lineFingerprintSet) contains(fp uint64) bool {
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

// prepBacklog shrinks the unresolved lists of a pending result while the
// decision stage waits for an earlier pair. It drops a fingerprint when
// either proof of duplication is already available:
//
//   - the set contains it: the set only grows, so it is still present when
//     this pair's turn comes and markNew would report it seen;
//   - scratch recorded it at an earlier position in the backlog: that
//     occurrence is processed first and either inserts it or finds it
//     present, so again it is present at this pair's turn. Saturation
//     cannot break this: verdictUnresolved ignores the list once the set is
//     saturated.
//
// Every drop removes a probe that would otherwise run inside the serial
// drain after the straggler, which is the window where the decision
// goroutine is the pipeline's bottleneck.
func (s *lineFingerprintSet) prepBacklog(r dedupPairResult, scratch *backlogScratch) {
	for ci := range r.candidates {
		c := &r.candidates[ci]
		if c.unresolved == nil {
			continue
		}
		pos := r.seq<<20 | uint64(c.hunk&(1<<20-1))
		kept := c.unresolved[:0]
		for _, fp := range c.unresolved {
			if s.contains(fp) || scratch.seenBefore(fp, pos) {
				continue
			}
			kept = append(kept, fp)
		}
		c.unresolved = kept
	}
}

// backlogScratch records, for fingerprints seen while pre-processing the
// reorder backlog, the earliest (seq, hunk) position that mentioned them.
// It is a bounded open-addressing table; once it reaches its load bound it
// stops recording and seenBefore only answers from what it holds, which
// keeps it a pure optimization. reset clears it between backlogs.
type backlogScratch struct {
	fps  []uint64
	pos  []uint64
	mask uint64
	used int
	cap  int
}

const backlogScratchSlotsLog2 = 20

func newBacklogScratch() *backlogScratch {
	return &backlogScratch{}
}

func (b *backlogScratch) reset() {
	if b.used == 0 {
		return
	}
	clear(b.fps)
	b.used = 0
}

// seenBefore reports whether fp was recorded at a position before pos (or
// in the same hunk, where an earlier line of the hunk already covers it),
// recording pos otherwise when room remains.
func (b *backlogScratch) seenBefore(fp, pos uint64) bool {
	if b.fps == nil {
		n := 1 << backlogScratchSlotsLog2
		b.fps = make([]uint64, n)
		b.pos = make([]uint64, n)
		b.mask = uint64(n - 1)
		b.cap = n * 7 / 10
	}
	i := fp & b.mask
	for {
		switch b.fps[i] {
		case fp:
			if b.pos[i] <= pos {
				return true
			}
			b.pos[i] = pos
			return false
		case 0:
			if b.used >= b.cap {
				return false
			}
			b.fps[i] = fp
			b.pos[i] = pos
			b.used++
			return false
		}
		i = (i + 1) & b.mask
	}
}

// dedupInFlightWindow bounds the pairs that have been stamped with a seq but
// not yet decided, which bounds the decision stage's reorder buffer and the
// hunks it retains, regardless of how long one straggling pair takes.
//
// The sequencer calls wait before stamping each seq; the decision stage
// calls advanced after settling each pair. The wake channel has capacity
// one and advanced sends to it without blocking, so a wake-up is never
// lost: either the sequencer is parked on it, or the token is waiting for
// the next wait.
type dedupInFlightWindow struct {
	decided atomic.Uint64
	wake    chan struct{}
}

func newDedupInFlightWindow() *dedupInFlightWindow {
	return &dedupInFlightWindow{wake: make(chan struct{}, 1)}
}

// advanced records that one more pair, in seq order, has been decided.
func (w *dedupInFlightWindow) advanced() {
	w.decided.Add(1)
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// wait blocks until fewer than dedupMaxInFlightPairs seqs below seq remain
// undecided, or stopCh closes (reported as false).
func (w *dedupInFlightWindow) wait(seq uint64, stopCh <-chan struct{}) bool {
	for seq-w.decided.Load() >= dedupMaxInFlightPairs {
		select {
		case <-stopCh:
			return false
		case <-w.wake:
		}
	}
	return true
}

// dedupHunkEmission decides whether one hunk survives dedup, marking every
// line in set as a side effect. It reports true iff at least one line was
// unseen when the hunk was reached; if every line was already seen the hunk
// is suppressed. The caller forwards its own HunkAddition on a true
// verdict, so hunks-emitted-intact is structural: this function cannot
// return a modified hunk, and hunks are never split.
//
// Duplicate lines inside one hunk cannot suppress each other or affect this
// hunk's verdict: a block whose closing line repeats an earlier line of the
// same hunk — armored key blocks sharing an END marker — still counts every
// occurrence as new.
//
// That holds even though each line is marked as it is examined, which is what
// lets one pass settle the verdict and so hashes each line once rather than
// twice. Let S be the set state at hunk entry. Deciding against S alone means
// "∃ line ∉ S"; deciding while marking means "∃ line at i ∉ S ∪ {lines before
// i}". The two agree: if any line is absent from S then the FIRST such line
// has every predecessor already in S, so the union adds nothing and that line
// is absent either way; conversely the union contains S, so absence from the
// union implies absence from S.
//
// The loop must not break early: the verdict is settled by the first unseen
// line, but every line still has to be marked for later hunks.
//
// That argument reads markNew's result as "was absent", which a saturated
// table breaks, so saturation is covered separately. Saturated at entry:
// markNew reports true for every line, so any non-empty text hunk is
// emitted. Saturation reached partway through a hunk: the insertion that
// tripped it reported absence, so the verdict is already true. A hunk whose
// lines are all present inserts nothing and so cannot begin saturation.
// Every case therefore agrees with a verdict taken against S alone.
//
// Binary hunks bypass dedup entirely: they always survive and never mark
// the set.
func dedupHunkEmission(h HunkAddition, set *lineFingerprintSet) bool {
	if h.isBinary {
		return true
	}

	anyNew := false
	for _, line := range h.lines {
		// markNew both reports absence and inserts, in a single probe and a
		// single hash of the line. It is the only mutator here, so the insert
		// sequence — and with it the growth and saturation points — is fixed
		// by the line order alone.
		if set.markNew(lineFingerprint(line)) {
			anyNew = true
		}
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

// dedupCandidate names one hunk of a pair that the decision stage must
// still look at: a binary hunk (always emitted, never marked) or a text
// hunk with at least one line the worker's snapshot probe did not find.
// unresolved holds those fingerprints in line order, zero already
// remapped; it is nil for binary hunks.
type dedupCandidate struct {
	hunk       int
	unresolved []uint64
}

// dedupPairResult carries one pair's computed hunks back to the decision
// stage. hunks may be empty; every dispatched seq produces exactly one
// result on the happy path so the reorder cursor always advances.
//
// candidates is in hunk order. A text hunk absent from it had every line
// present in the snapshot, so it is a duplicate at its position unless the
// set is saturated when the pair is reached (see decideResult).
type dedupPairResult struct {
	seq        uint64
	hunks      []HunkAddition
	candidates []dedupCandidate
}

// prefilterHunk hashes h's lines and probes sn for each, returning the
// fingerprints the snapshot did not contain, appended to buf.
//
// Soundness of treating a snapshot hit as a settled duplicate: the decision
// goroutine processes pairs strictly in seq order and this hunk's pair has
// not been handed to it yet, so every fingerprint in the snapshot was
// inserted while processing an earlier seq — i.e. it is in the set at this
// hunk's position in the total order. A miss is only a hint and is resolved
// authoritatively by the decision stage.
func prefilterHunk(h *HunkAddition, sn *fingerprintSnapshot, buf []uint64) []uint64 {
	for _, line := range h.lines {
		fp := lineFingerprint(line)
		if fp == 0 {
			fp = dedupZeroFingerprint
		}
		if !sn.contains(fp) {
			buf = append(buf, fp)
		}
	}
	return buf
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
//	hunk workers            N, consume blobChan, hash each line and probe
//	                        the published fingerprint snapshot, produce
//	                        resultChan with only the unresolved fingerprints
//	decision goroutine      1, reorders by seq, owns the fingerprint set,
//	                        settles verdicts from the unresolved fingerprints
//	                        and produces batched yieldChan (closed on exit)
//	yield workers           N, consume yieldChan batches, call fn concurrently
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
	blobChan := make(chan []dedupPairWork, dedupBlobChanCap)
	resultChan := make(chan []dedupPairResult, numWorkers)
	yieldChan := make(chan []HunkAddition, dedupYieldChanCap)
	stopCh := make(chan struct{})
	slots := make([]dedupCommitSlot, dedupLookaheadCommits)

	// The fingerprint set is owned by the decision goroutine; hunk workers
	// only read its published snapshot.
	set := newLineFingerprintSetWithBudget(dedupInitialSlotsLog2, hs.hunkDedupBudget)
	inFlight := newDedupInFlightWindow()

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
				case works, ok := <-blobChan:
					if !ok {
						return
					}
					results := make([]dedupPairResult, 0, len(works))
					for _, pw := range works {
						var hunks []HunkAddition
						if err := hs.streamBlobPairHunks(pw.work, func(h HunkAddition) error {
							hunks = append(hunks, h)
							return nil
						}); err != nil {
							setError(fmt.Errorf("failed diffing %s in commit %s: %w", pw.work.path, pw.work.commit, err))
							return
						}
						// Hash and speculatively probe here, in parallel, so
						// the decision goroutine only touches fingerprints the
						// snapshot did not already hold. One buffer per pair
						// keeps the unresolved lists as sub-slices of a single
						// allocation.
						var buf []uint64
						var cands []dedupCandidate
						for i := range hunks {
							h := &hunks[i]
							if h.isBinary {
								cands = append(cands, dedupCandidate{hunk: i})
								continue
							}
							start := len(buf)
							buf = prefilterHunk(h, set.snapshot(), buf)
							if len(buf) > start {
								cands = append(cands, dedupCandidate{hunk: i, unresolved: buf[start:len(buf):len(buf)]})
							}
						}
						results = append(results, dedupPairResult{seq: pw.seq, hunks: hunks, candidates: cands})
					}
					select {
					case <-stopCh:
						return
					case resultChan <- results:
					}
				}
			}
		}()
	}

	decisionDone := make(chan struct{})
	go func() {
		defer close(decisionDone)
		defer close(yieldChan)
		// In-flight seqs are bounded by (cap(blobChan) + numWorkers +
		// cap(resultChan)) * dedupPairBatchSize, so the reorder buffer
		// stays small; it only grows toward that bound when pair completion
		// times are skewed.
		pending := make(map[uint64]dedupPairResult, 64)
		next := uint64(0)
		batch := make([]HunkAddition, 0, dedupYieldBatchHunks)
		// backlog holds seqs of pending results not yet pre-processed by
		// prepBacklog; it is worked through only while next is missing and
		// resultChan is empty, i.e. while this goroutine would otherwise
		// idle behind a straggling pair.
		var backlog []uint64
		scratch := newBacklogScratch()
		// flush hands the current batch to the yield pool and starts a
		// fresh one. It reports false when the pipeline is stopping.
		flush := func() bool {
			if len(batch) == 0 {
				return true
			}
			select {
			case <-stopCh:
				return false
			case yieldChan <- batch:
			}
			batch = make([]HunkAddition, 0, dedupYieldBatchHunks)
			return true
		}
		// drain settles every pair that is ready in seq order. It reports
		// false when the pipeline is stopping.
		drain := func() bool {
			for {
				r, ok := pending[next]
				if !ok {
					break
				}
				delete(pending, next)
				next++
				batch = set.decideResult(r, batch)
				if len(batch) >= dedupYieldBatchHunks && !flush() {
					return false
				}
				inFlight.advanced()
			}
			if len(pending) == 0 {
				backlog = backlog[:0]
				scratch.reset()
			}
			// Release what is ready rather than holding a partial batch
			// until the next result arrives.
			return flush()
		}
		accept := func(results []dedupPairResult) {
			for _, res := range results {
				pending[res.seq] = res
				backlog = append(backlog, res.seq)
			}
		}
		for {
			// Prefer new results; they may carry next.
			select {
			case <-stopCh:
				return
			case results, ok := <-resultChan:
				if !ok {
					flush()
					return
				}
				accept(results)
				if !drain() {
					return
				}
				continue
			default:
			}
			// Nothing ready: pre-process one backlog entry so the drain
			// after the straggler has little left to probe, then block.
			if len(backlog) > 0 {
				seq := backlog[len(backlog)-1]
				backlog = backlog[:len(backlog)-1]
				if r, ok := pending[seq]; ok {
					set.prepBacklog(r, scratch)
				}
				continue
			}
			select {
			case <-stopCh:
				return
			case results, ok := <-resultChan:
				if !ok {
					flush()
					return
				}
				accept(results)
				if !drain() {
					return
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
				case batch, ok := <-yieldChan:
					if !ok {
						return
					}
					for _, h := range batch {
						if err := fn(h); err != nil {
							setError(err)
							return
						}
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
	batch := make([]dedupPairWork, 0, dedupPairBatchSize)
	// flushBatch hands the accumulated pairs to the hunk workers. It
	// reports false when the pipeline is stopping.
	flushBatch := func() bool {
		if len(batch) == 0 {
			return true
		}
		select {
		case <-stopCh:
			return false
		case blobChan <- batch:
		}
		batch = make([]dedupPairWork, 0, dedupPairBatchSize)
		return true
	}
seqLoop:
	for i := range order {
		if !dispatch(i) {
			break
		}
		slot := &slots[i%dedupLookaheadCommits]
		select {
		case <-slot.done:
		default:
			// The tree stage has not finished this commit: release the
			// partial batch so hunk workers stay busy while we wait.
			if !flushBatch() {
				break seqLoop
			}
			select {
			case <-stopCh:
				break seqLoop
			case <-slot.done:
			}
		}
		if slot.err != nil {
			// The tree worker already routed the error through setError.
			break
		}
		for _, w := range slot.pairs {
			if seq-inFlight.decided.Load() >= dedupMaxInFlightPairs {
				// Window full: release what we hold so the decision stage
				// can make progress, then park until it does.
				if !flushBatch() || !inFlight.wait(seq, stopCh) {
					break seqLoop
				}
			}
			batch = append(batch, dedupPairWork{seq: seq, work: w})
			seq++
			// Once every commit has been dispatched to the tree stage the
			// scan is in its final window; single-pair batches there spread
			// the last pairs across workers instead of leaving one worker
			// with a sixteen-pair tail.
			if (len(batch) == dedupPairBatchSize || nextDispatch == len(order)) && !flushBatch() {
				break seqLoop
			}
		}
	}
	flushBatch()

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
