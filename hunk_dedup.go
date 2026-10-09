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
//     dispatched to the tree stage up to dedupLookaheadCommits ahead of
//     the emit cursor while the pairs held ahead of it fit under
//     dedupPendingPairsCap.
//  2. Parallel tree diff: workers resolve each dispatched commit's pair
//     list into a per-commit ring slot and estimate each pair's blob size
//     from pack headers (no channel sends, so tree workers never block).
//  3. Sequencer: state under seqMu that hunk workers advance when they
//     need work (takeOrdered), so the in-order producer never competes
//     with its consumers for a P. A stamp cursor assigns seqs to commits
//     as soon as their slots complete and forwards expensive pairs at
//     once on expensiveChan, so a multi-MB blob's materialization
//     overlaps ordinary work; an emit cursor hands out every remaining
//     pair in order, dedupPairBatchSize at a time. Stamping parks on
//     dedupInFlightWindow once dedupMaxInFlightPairs seqs are undecided,
//     so the reorder ring is bounded by construction however long one
//     pair takes, and while delivered hunk bytes not yet seen by fn exceed
//     dedupRetainedBytesCap. The emit cursor applies both bounds again per
//     pair, so a commit wider than the window stays bounded as the window
//     slides through it. A parked sequencer still returns queued
//     expensive work, which the decision stage may need to advance.
//  4. Parallel hunk workers -> serial decision stage -> parallel yield:
//     hunk workers compute each pair's hunks, run prefilterHunk against
//     the set's published snapshot, and deliver (seq, hunks, unresolved
//     fingerprints) into the reorder ring without blocking. The decision
//     goroutine settles each whole-hunk verdict in seq order with
//     decideResult (serial, but over the unresolved fingerprints only),
//     pre-processes the backlog with prepBacklog while it waits behind a
//     straggling pair, and fans surviving hunks out in batches to a pool
//     of yield workers that invoke fn concurrently. Only dedup decisions
//     are serialized; output is deterministic as a multiset, never as an
//     order.
//
// Cross-file dependencies:
//   - firstParentTree, emitCommitBlobPairsTo, pairHunks
//     (history_scanner.go): the reused diff machinery.
//   - orderCommitsParentFirst (commit_order.go): the deterministic order.
//   - farm.Hash64: the line fingerprint hash, matching the existing
//     line-hashing convention in diff_blob.go.

package objstore

import (
	"fmt"
	"math"
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

	// dedupLookaheadCommits bounds how many commits ahead of the emit
	// cursor may be tree-diffed. It is also the size of the per-commit slot
	// ring: an index is dispatched only while it is fewer than
	// dedupLookaheadCommits ahead of the cursor, so a ring slot is never
	// reused before the sequencer has consumed its previous occupant. The
	// stamp cursor can run this far ahead, which is how far in advance an
	// expensive pair can start: 8192 commits is several hundred
	// milliseconds of ordinary work on a 32-core host, enough to hide a
	// 100 ms blob materialization completely.
	dedupLookaheadCommits = 8192

	// dedupExpensiveChanCap bounds pairs forwarded ahead of order on
	// expensiveChan. The sequencer only forwards early while the channel
	// has room, so the channel never blocks it.
	dedupExpensiveChanCap = 64

	// dedupExpensivePairBytes is the estimated blob size from which a pair
	// counts as expensive: materializing a blob this large takes tens of
	// milliseconds and the store never caches it (maxCacheableSize), so
	// the pair would hold the in-order decision stage for that long.
	// Expensive pairs are forwarded as soon as their commit is tree-diffed,
	// up to dedupLookaheadCommits ahead of order, so that latency overlaps
	// ordinary work instead.
	dedupExpensivePairBytes = 4 << 20

	// dedupSizeProbeHops bounds the delta-chain headers estimatePackedSize
	// follows; a long chain's early hops already reveal a large base.
	dedupSizeProbeHops = 8

	// dedupEarlyBytesCap bounds the estimated blob bytes of expensive pairs
	// forwarded ahead of order and not yet decided. Their results, and the
	// blobs their lines reference, sit in the reorder ring until their
	// turn, so the cap keeps that retention to a few blobs' worth.
	dedupEarlyBytesCap = 256 << 20

	// dedupPairBatchSize is how many in-order pairs a hunk worker takes per
	// takeOrdered call. One pair per call put the seqMu hand-off on every
	// pair's critical path; sixteen amortizes it while keeping a worker's
	// share small enough that the last batches finish together.
	dedupPairBatchSize = 16

	// dedupMaxInFlightPairs bounds seqs stamped but not yet decided, and
	// therefore the reorder ring and the hunks it retains. Delivery into
	// the ring never blocks, so this window is the pipeline's only
	// backpressure. 65536 is about half a second of worker throughput on a
	// 32-core host: a single expensive pair (a 40 MB binary, a multi-MB
	// generated file) holds the order for 50-100 ms, and parking earlier
	// measurably idled the workers behind it.
	dedupMaxInFlightPairs = 65536

	// dedupPendingPairsCap bounds blob pairs held in tree-diffed slots
	// ahead of the emit cursor.
	dedupPendingPairsCap = 4 * dedupMaxInFlightPairs

	// dedupDispatchDepth is how many commits per tree worker may wait in
	// the tree stage at once.
	dedupDispatchDepth = 2

	// dedupRetainedBytesCap bounds hunk bytes held from delivery to the
	// decision stage until the consumer callback has seen them.
	dedupRetainedBytesCap = 512 << 20

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

type dedupLimits struct {
	// inFlightPairs must be a power of two: the reorder ring is indexed by
	// seq & (inFlightPairs-1).
	inFlightPairs      uint64
	lookaheadCommits   int
	expensivePairBytes uint64
	earlyBytesCap      uint64
	yieldChanCap       int
	workers            int
	pendingPairsCap    int64
	retainedBytesCap   int64
}

func defaultDedupLimits() dedupLimits {
	return dedupLimits{
		inFlightPairs:      dedupMaxInFlightPairs,
		lookaheadCommits:   dedupLookaheadCommits,
		expensivePairBytes: dedupExpensivePairBytes,
		earlyBytesCap:      dedupEarlyBytesCap,
		yieldChanCap:       dedupYieldChanCap,
		pendingPairsCap:    dedupPendingPairsCap,
		retainedBytesCap:   dedupRetainedBytesCap,
	}
}

// dedupProbe records pipeline observations for package tests.
type dedupProbe struct {
	// windowWaits counts sequencer waits on the in-flight window that
	// returned ready.
	windowWaits atomic.Uint64

	peakPendingPairs  atomic.Int64
	peakRetainedBytes atomic.Int64
}

func notePeak(peak *atomic.Int64, v int64) {
	for {
		cur := peak.Load()
		if v <= cur || peak.CompareAndSwap(cur, v) {
			return
		}
	}
}

// hunkRetainedBytes estimates line storage using a 16-byte string header
// per line.
func hunkRetainedBytes(h *HunkAddition) int64 {
	n := int64(len(h.lines)) * 16
	for _, line := range h.lines {
		n += int64(len(line))
	}
	return n
}

// lineFingerprint hashes one added line (without its trailing newline,
// which tokenize already strips) to the 64-bit fingerprint used for dedup.
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
// the insert order (and therefore the growth and saturation points)
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

	// prefetchSink receives verdictUnresolved's touch-loop sum so the
	// compiler preserves the loads.
	prefetchSink uint64
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
// markNew would report it seen and insert nothing: skipping it changes
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
		s.prefetchSink = touched
		for _, fp := range unresolved[:n] {
			if s.markNew(fp) {
				anyNew = true
			}
		}
		unresolved = unresolved[n:]
	}
	return anyNew
}

// decideResult settles every hunk of one pair in order and appends the
// survivors to out.
//
// Only candidates need a verdict: an exempt hunk always survives, and a
// text hunk with unresolved fingerprints is probed with verdictUnresolved. A
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
		if h.dedupExempt() || s.verdictUnresolved(c.unresolved, len(h.lines)) {
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
		pos := backlogPos(r.seq, c.hunk)
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

// backlogPos encodes a (seq, hunk) position so that integer order equals
// the pipeline's total order over hunks. Each half gets 32 bits: a pair's
// hunk count is bounded by its line count, at most MaxDiffSize, and a
// scan's pair count stays far below 2^32.
func backlogPos(seq uint64, hunk int) uint64 {
	return seq<<32 | uint64(uint32(hunk))
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
	// stampedSeqs mirrors the sequencer's seq counter for the decision
	// stage's bookkeeping.
	stampedSeqs atomic.Uint64
	wake        chan struct{}
	width       uint64
	probe       *dedupProbe
}

// stamped returns how many seqs the sequencer has assigned so far.
func (w *dedupInFlightWindow) stamped() uint64 { return w.stampedSeqs.Load() }

func newDedupInFlightWindow(width uint64, probe *dedupProbe) *dedupInFlightWindow {
	return &dedupInFlightWindow{wake: make(chan struct{}, 1), width: width, probe: probe}
}

// advanced records that n more pairs, in seq order, have been decided.
func (w *dedupInFlightWindow) advanced(n uint64) {
	w.decided.Add(n)
	w.notify()
}

// notify wakes a parked sequencer so it re-evaluates its condition.
func (w *dedupInFlightWindow) notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

type parkOutcome int

const (
	parkReady parkOutcome = iota
	parkStopped
	parkWork
)

func (w *dedupInFlightWindow) park(ready func() bool, stopCh <-chan struct{}, work <-chan []dedupPairWork) ([]dedupPairWork, parkOutcome) {
	for {
		if ready() {
			if w.probe != nil {
				w.probe.windowWaits.Add(1)
			}
			return nil, parkReady
		}
		select {
		case <-stopCh:
			return nil, parkStopped
		case works := <-work:
			return works, parkWork
		case <-w.wake:
		}
	}
}

// dedupHunkEmission decides whether one hunk survives dedup, marking every
// line in set as a side effect. It reports true iff at least one line was
// unseen when the hunk was reached; if every line was already seen the hunk
// is suppressed. The caller forwards its own HunkAddition on a true
// verdict, so hunks-emitted-intact is structural: this function cannot
// return a modified hunk, and hunks are never split.
//
// This single pass is the specification the pipeline's split verdict
// (prefilterHunk + prepBacklog + decideResult) is checked against; the
// pipeline takes the split path so that hashing and most probing run on the
// parallel workers.
//
// Duplicate lines inside one hunk leave its verdict unchanged: a block whose
// closing line repeats an earlier line of the same hunk (armored key blocks
// sharing an END marker) survives whenever its first occurrence was unseen.
// The first occurrence inserts the fingerprint and reports new; later
// occurrences report seen, and the verdict is already settled by then.
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
// Binary hunks and too-large placeholders (dedupExempt) bypass dedup
// entirely: they always survive and never mark the set.
func dedupHunkEmission(h HunkAddition, set *lineFingerprintSet) bool {
	if h.dedupExempt() {
		return true
	}

	anyNew := false
	for _, line := range h.lines {
		// markNew both reports absence and inserts, in a single probe and a
		// single hash of the line. It is the only mutator here, so the insert
		// sequence (and with it the growth and saturation points) is fixed
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

	// firstSeq is the seq of pairs[0]; the sequencer sets it when it
	// stamps the commit, which may happen well before the commit's
	// ordinary pairs are forwarded.
	firstSeq uint64

	// expensive lists the pairs whose blobs the tree worker estimated at
	// dedupExpensivePairBytes or more (see estimatePackedSize), in
	// increasing index order, with the estimate.
	expensive []dedupExpensivePair

	// early lists the indices into pairs the sequencer already forwarded
	// ahead of order as expensive pairs, in increasing order.
	early []int
}

// estimatePackedSize returns an estimate of oid's materialized size from pack
// headers alone, without inflating anything: the header size of a
// non-delta object, or for a delta the largest header size seen while
// following up to dedupSizeProbeHops base links. It reports 0 for objects it
// cannot find or follow (loose objects, broken chains); the estimate is a
// scheduling hint and never affects results.
func (hs *HistoryScanner) estimatePackedSize(oid Hash) uint64 {
	if oid.IsZero() {
		return 0
	}
	pack, off, ok := hs.store.findPackedObject(oid)
	if !ok {
		return 0
	}
	var best uint64
	for hop := 0; hop < dedupSizeProbeHops; hop++ {
		var buf [32]byte
		n, err := pack.ReadAt(buf[:], int64(off))
		if n == 0 || (err != nil && n < 1) {
			return best
		}
		typ, size, hdrLen := parseObjectHeaderUnsafe(buf[:n])
		if hdrLen <= 0 {
			return best
		}
		best = max(best, size)
		switch typ {
		case ObjOfsDelta:
			back, _, err := readOfsDeltaOffset(pack, int64(off)+int64(hdrLen))
			if err != nil || back == 0 || back > off {
				return best
			}
			off -= back
		case ObjRefDelta:
			var base Hash
			if _, err := pack.ReadAt(base[:], int64(off)+int64(hdrLen)); err != nil {
				return best
			}
			if pack, off, ok = hs.store.findPackedObject(base); !ok {
				return best
			}
		default:
			return best
		}
	}
	return best
}

// dedupExpensivePair names one pair of a commit whose blobs are estimated
// large, with the larger of the two estimates.
type dedupExpensivePair struct {
	index int
	bytes uint64
}

func (hs *HistoryScanner) expensivePairs(pairs []blobPairWork) []dedupExpensivePair {
	threshold := hs.dedupLimits.expensivePairBytes
	var out []dedupExpensivePair
	for i := range pairs {
		w := &pairs[i]
		est := max(hs.estimatePackedSize(w.newOID), hs.estimatePackedSize(w.oldOID))
		if est >= threshold {
			out = append(out, dedupExpensivePair{index: i, bytes: est})
		}
	}
	return out
}

// dedupPairWork is one blob pair stamped with its global sequence number.
// earlyBytes is the size estimate charged against dedupEarlyBytesCap when
// the pair was forwarded ahead of order, and zero otherwise.
type dedupPairWork struct {
	seq        uint64
	work       blobPairWork
	earlyBytes uint64
}

// kib is the retainedKiB total of the pairs whose hunks are in the batch.
type dedupYieldBatch struct {
	hunks []HunkAddition
	kib   int64
}

// dedupCandidate names one hunk of a pair that the decision stage must
// still look at: a dedup-exempt hunk (always emitted, never marked) or a
// text hunk with at least one line the worker's snapshot probe did not
// find. unresolved holds those fingerprints in line order, zero already
// remapped; it is nil for exempt hunks.
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

	// earlyKiB is the pair's dedupPairWork.earlyBytes in KiB. retainedKiB
	// is the hunkRetainedBytes total of hunks in KiB, rounded up; surviving
	// lines alias the pair's blob, so the whole pair stays charged while
	// any of its hunks waits for fn. Both fields are 32 bits so a result
	// stays 64 bytes: the reorder ring holds a window of them and is zeroed
	// on every scan.
	earlyKiB    uint32
	retainedKiB uint32
}

// toKiB rounds n bytes up to whole KiB, saturating at the uint32 range.
func toKiB(n uint64) uint32 {
	return uint32(min((n+1023)>>10, math.MaxUint32))
}

// prefilterHunk hashes h's lines and probes sn for each, returning the
// fingerprints the snapshot did not contain, appended to buf.
//
// Soundness of treating a snapshot hit as a settled duplicate: the decision
// goroutine processes pairs strictly in seq order and this hunk's pair has
// not been handed to it yet, so every fingerprint in the snapshot was
// inserted while processing an earlier seq: i.e. it is in the set at this
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

// pairCandidates runs the worker-side half of the split verdict over one
// pair's hunks: it returns the hunks the decision stage must still look at
// (see dedupCandidate), in hunk order, and the pair's hunkRetainedBytes
// total. One buffer per pair keeps the unresolved lists as sub-slices of a
// single allocation.
func pairCandidates(hunks []HunkAddition, sn *fingerprintSnapshot) (cands []dedupCandidate, total int64) {
	var buf []uint64
	for i := range hunks {
		h := &hunks[i]
		total += hunkRetainedBytes(h)
		if h.dedupExempt() {
			cands = append(cands, dedupCandidate{hunk: i})
			continue
		}
		start := len(buf)
		buf = prefilterHunk(h, sn, buf)
		if len(buf) > start {
			cands = append(cands, dedupCandidate{hunk: i, unresolved: buf[start:len(buf):len(buf)]})
		}
	}
	return cands, total
}

// collectCommitPairs resolves c's first-parent tree and collects the
// commit's changed blob pairs in deterministic tree order. Errors come back
// pre-wrapped with the same message formats the streaming pipeline uses, so
// tree workers only record and propagate them. A closed stopCh aborts the
// tree walk with errScanAborted. admit, when non-nil, runs before each pair
// is appended and may block or return an error to abort.
func (hs *HistoryScanner) collectCommitPairs(c commitInfo, stopCh <-chan struct{}, admit func() error) ([]blobPairWork, error) {
	parentTree, err := hs.firstParentTree(c)
	if err != nil {
		return nil, fmt.Errorf("resolve first-parent tree for commit %s: %w", c.OID, err)
	}
	var pairs []blobPairWork
	if err := hs.emitCommitBlobPairsTo(c, parentTree, func(w blobPairWork) error {
		if admit != nil {
			if err := admit(); err != nil {
				return err
			}
		}
		pairs = append(pairs, w)
		return nil
	}, stopCh); err != nil {
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
//	caller goroutine        starts the stages, then runs the shutdown chain
//	tree workers            min(N, maxTreeDiffWorkers), consume treeIdxChan,
//	                        fill slots, never block
//	hunk workers            N, take batches through takeWork (expensiveChan
//	                        first, then the seqMu-guarded sequencer), hash
//	                        each line, probe the published fingerprint
//	                        snapshot, and deliver into the reorder ring
//	decision goroutine      1, settles verdicts in seq order from the ring,
//	                        owns the fingerprint set, produces batched
//	                        yieldChan (closed on exit)
//	yield workers           N, consume yieldChan batches, call fn concurrently
//
// On any error (setError closes stopCh) every stage unblocks via its stopCh
// select, and the shutdown chain (hunkWG.Wait(), close(treeIdxChan),
// treeWG.Wait(), <-decisionDone, yieldWG.Wait()) still runs to completion,
// so no goroutine outlives the call.
func (hs *HistoryScanner) diffHistoryHunksDedup(fn func(HunkAddition) error) error {
	defer hs.stopProfiling() // Ensure profiling is stopped even on error
	// The tree memo only pays off while this scan resolves first-parent
	// trees; dropping it here keeps the scanner's steady-state memory
	// independent of history size.
	defer hs.treeOIDs.Clear()

	if err := hs.startProfiling(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to start profiling: %v\n", err)
	}

	// loadAllCommits re-walks refs when the ref tips moved, so a reused
	// scanner sees commits added since its previous scan.
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

	limits := hs.dedupLimits
	window := limits.inFlightPairs
	ringMask := window - 1
	lookahead := limits.lookaheadCommits
	numWorkers := limits.workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	treeWorkers := min(numWorkers, maxTreeDiffWorkers)

	treeIdxChan := make(chan int, lookahead)
	expensiveChan := make(chan []dedupPairWork, dedupExpensiveChanCap)
	// Results travel through a reorder ring indexed by seq: the sequencer
	// never stamps a seq that is dedupMaxInFlightPairs or more ahead of the
	// decided count, so every undecided seq maps to a distinct slot, each
	// written by exactly one worker and read by the decision goroutine.
	// Workers therefore never block on delivery; the only backpressure is
	// the in-flight window itself.
	ring := make([]dedupPairResult, window)
	present := make([]atomic.Bool, window)
	// pendingPairs counts pairs in tree-diffed slots the emit cursor has
	// not passed; retainedKiB counts result KiB from delivery until fn
	// has seen every surviving hunk (dropped hunks release at decision).
	var pendingPairs, retainedKiB atomic.Int64
	retainedKiBCap := limits.retainedBytesCap >> 10
	probe := hs.dedupProbe
	// arrived collects delivered seqs for the decision goroutine's backlog
	// pre-processing; resultsReady wakes it (without blocking the sender).
	var (
		arrivedMu    sync.Mutex
		arrived      []uint64
		resultsReady = make(chan struct{}, 1)
	)
	// deliver publishes one result as soon as its pair is materialized, so
	// the retained-byte charge is visible to the admission gate and to
	// workers mid-batch before the rest of a batch is computed.
	deliver := func(res dedupPairResult) {
		if v := retainedKiB.Add(int64(res.retainedKiB)); probe != nil {
			notePeak(&probe.peakRetainedBytes, v<<10)
		}
		i := res.seq & ringMask
		ring[i] = res
		present[i].Store(true)
		arrivedMu.Lock()
		arrived = append(arrived, res.seq)
		arrivedMu.Unlock()
		select {
		case resultsReady <- struct{}{}:
		default:
		}
	}
	// wakeCh is closed and replaced whenever the retained charge drops back
	// under its cap, the decided count advances, or the emit cursor passes
	// a commit, so a goroutine holding at one of those bounds can wait
	// without polling.
	var (
		wakeMu sync.Mutex
		wakeCh = make(chan struct{})
	)
	wakeWaiters := func() {
		wakeMu.Lock()
		close(wakeCh)
		wakeCh = make(chan struct{})
		wakeMu.Unlock()
	}
	currentWake := func() <-chan struct{} {
		wakeMu.Lock()
		defer wakeMu.Unlock()
		return wakeCh
	}
	// emitCursor mirrors the sequencer's emitCommit for tree workers.
	var emitCursor atomic.Int64
	// slotReady is pinged (without blocking) by tree workers when a slot
	// completes so the sequencer can stamp ahead while it waits elsewhere.
	slotReady := make(chan struct{}, 1)
	yieldChan := make(chan dedupYieldBatch, limits.yieldChanCap)
	stopCh := make(chan struct{})
	slots := make([]dedupCommitSlot, lookahead)

	// The fingerprint set is owned by the decision goroutine; hunk workers
	// only read its published snapshot.
	set := newLineFingerprintSetWithBudget(dedupInitialSlotsLog2, hs.hunkDedupBudget)
	inFlight := newDedupInFlightWindow(window, hs.dedupProbe)
	// releaseRetained wakes the sequencer only when the release brings the
	// charge back under the cap, the one transition stampable waits on.
	releaseRetained := func(n int64) {
		if n == 0 {
			return
		}
		if after := retainedKiB.Add(-n); after+n > retainedKiBCap && after <= retainedKiBCap {
			inFlight.notify()
			wakeWaiters()
		}
	}

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

	// outstanding counts commits dispatched but not yet diffed.
	// diffedPairs / diffedCommits is the mean pairs per diffed commit, which
	// projects what outstanding commits will add to pendingPairs.
	var outstanding, diffedPairs, diffedCommits atomic.Int64

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
					slot := &slots[idx%lookahead]
					// Each pair is charged to pendingPairs as it is
					// collected, and collection holds at the cap for every
					// commit except the one under the emit cursor, which
					// alone can release the pairs ahead of it. A held
					// commit therefore adds at most one pair past the cap.
					var charged int64
					admit := func() error {
						if p := pendingPairs.Add(1); probe != nil {
							notePeak(&probe.peakPendingPairs, p)
						}
						charged++
						for pendingPairs.Load() > limits.pendingPairsCap && int64(idx) != emitCursor.Load() {
							wake := currentWake()
							if pendingPairs.Load() <= limits.pendingPairsCap || int64(idx) == emitCursor.Load() {
								break
							}
							select {
							case <-stopCh:
								return errScanAborted
							case <-wake:
							}
						}
						return nil
					}
					pairs, err := hs.collectCommitPairs(order[idx], stopCh, admit)
					if err != nil {
						pendingPairs.Add(-charged)
					}
					var expensive []dedupExpensivePair
					if err == nil {
						expensive = hs.expensivePairs(pairs)
					}
					slot.expensive = expensive
					// err must be visible before done closes: the
					// sequencer reads both after <-slot.done.
					slot.pairs, slot.err = pairs, err
					diffedPairs.Add(int64(len(pairs)))
					diffedCommits.Add(1)
					outstanding.Add(-1)
					close(slot.done)
					select {
					case slotReady <- struct{}{}:
					default:
					}
					if err != nil {
						setError(err)
						return
					}
				}
			}
		}()
	}

	// Sequencer state, advanced by whichever hunk worker needs work under
	// seqMu. Running the sequencer on the consumers' own goroutines keeps
	// the in-order producer from competing with them for a P: a dedicated
	// producer goroutine on a saturated machine spent more time runnable
	// than running and starved the workers it fed.
	//
	// Two cursors move over the commit order. The stamp cursor assigns seqs
	// to commits as soon as their slots complete (contiguously, within the
	// in-flight window) and forwards expensive pairs at once on
	// expensiveChan, so their latency overlaps ordinary work. The emit
	// cursor follows behind and hands out every remaining pair in order.
	// Both read seqs from the slot, so seq order is commit order regardless
	// of which path a pair travelled.
	// earlyBytes is the estimated size of expensive pairs forwarded ahead
	// of order and not yet decided (see dedupEarlyBytesCap).
	var earlyBytes atomic.Uint64
	var (
		seqMu        sync.Mutex
		nextDispatch int    // commits [0, nextDispatch) are in the tree stage
		stamp        int    // commits [0, stamp) have seqs assigned
		seq          uint64 // next seq to assign
		emitCommit   int    // emit cursor: commit index
		emitPair     int    // emit cursor: index into slot.pairs
		emitEarly    []int  // emitCommit's early list, consumed as we pass
		seqDone      bool   // every pair has been handed out
	)
	// dispatch keeps the tree stage at most lookahead commits ahead of the
	// emit cursor and stops adding commits while pendingPairs, plus the
	// pairs projected for commits still in the tree stage, would exceed its
	// cap. The commit under the emit cursor is always dispatched, so the
	// emit cursor never waits on a budget only it can release. takeOrdered
	// calls dispatch under seqMu on every pass, so the hunk workers that
	// consume pairs are the ones that refill the tree stage. treeIdxChan's
	// capacity equals lookahead, so the send never blocks.
	dispatchDepth := int64(dedupDispatchDepth * treeWorkers)
	tailPairs := int64(numWorkers * dedupPairBatchSize)
	canDispatch := func(next int) bool {
		if next >= len(order) || next-emitCommit >= lookahead {
			return false
		}
		if next <= emitCommit {
			return true
		}
		pending, out := pendingPairs.Load(), outstanding.Load()
		if pending >= limits.pendingPairsCap {
			return false
		}
		if out < dispatchDepth {
			return true
		}
		// Past the floor, dispatch only while the projected pairs of the
		// outstanding commits plus this one fit under the cap.
		commits := diffedCommits.Load()
		if commits == 0 {
			return false
		}
		perCommit := (diffedPairs.Load() + commits - 1) / commits
		return pending+(out+1)*max(perCommit, 1) <= limits.pendingPairsCap
	}
	dispatch := func() {
		for canDispatch(nextDispatch) {
			slot := &slots[nextDispatch%lookahead]
			// Re-arm only the done channel: the tree worker overwrites
			// pairs and err wholesale (adopting collectCommitPairs'
			// returned slice) before closing done, and the ring slot's
			// previous occupant (nextDispatch - dedupLookaheadCommits)
			// was fully consumed before the window allowed this dispatch.
			slot.done = make(chan struct{})
			slot.early = slot.early[:0]
			outstanding.Add(1)
			treeIdxChan <- nextDispatch
			nextDispatch++
		}
	}
	stampable := func(n uint64) bool {
		if n == 0 {
			return true
		}
		if retainedKiB.Load() > retainedKiBCap {
			return false
		}
		decided := inFlight.decided.Load()
		if n > window {
			return seq == decided
		}
		return seq+n <= decided+window
	}
	// Stamping applies the window and the retained-byte cap per commit;
	// admissible applies them per pair, which matters inside a commit wider
	// than the window, where the window slides pair by pair. While retained
	// bytes exceed the cap, the seq the decision stage is waiting for still
	// passes, so the decision stage keeps draining and releasing bytes.
	admissible := func(s uint64) bool {
		decided := inFlight.decided.Load()
		if s >= decided+window {
			return false
		}
		return s == decided || retainedKiB.Load() <= retainedKiBCap
	}
	// stampAhead assigns seqs to every further commit whose slot is done,
	// without blocking, and forwards its expensive pairs while
	// expensiveChan has room. It stops at the first incomplete slot, at
	// the in-flight window, or at a tree-stage error (reported as false).
	stampAhead := func() bool {
		for stamp < nextDispatch {
			slot := &slots[stamp%lookahead]
			select {
			case <-slot.done:
			default:
				return true
			}
			if slot.err != nil {
				return false
			}
			n := uint64(len(slot.pairs))
			if !stampable(n) {
				return true
			}
			slot.firstSeq = seq
			// The ring indexes results by seq modulo the window, so an early
			// pair past the window edge of a wide commit would share a slot
			// with an undecided in-order pair. slot.expensive is in index
			// order, so the first pair past the edge ends the loop.
			edge := inFlight.decided.Load() + window
			for _, e := range slot.expensive {
				if seq+uint64(e.index) >= edge || earlyBytes.Load()+e.bytes > limits.earlyBytesCap {
					break
				}
				select {
				case expensiveChan <- []dedupPairWork{{seq: seq + uint64(e.index), work: slot.pairs[e.index], earlyBytes: e.bytes}}:
					slot.early = append(slot.early, e.index)
					earlyBytes.Add(uint64(toKiB(e.bytes)) << 10)
				default:
					// No room ahead of order; the emit cursor hands this
					// pair out in its turn.
				}
			}
			seq += n
			inFlight.stampedSeqs.Store(seq)
			stamp++
		}
		return true
	}
	// takeOrdered hands out the next batch of in-order pairs, blocking (with
	// seqMu held) only while nothing can be handed out: the emit cursor's
	// slot is still in the tree stage, or the in-flight window is full.
	// Nothing it waits on needs seqMu, so holding it is safe: tree workers
	// and the decision goroutine run independently of the sequencer. It
	// returns nil, false once every pair has been handed out or the
	// pipeline is stopping.
	takeOrdered := func() ([]dedupPairWork, bool) {
		seqMu.Lock()
		defer seqMu.Unlock()
		if seqDone {
			return nil, false
		}
		var batch []dedupPairWork
		for emitCommit < len(order) {
			dispatch()
			if !stampAhead() {
				setError(slots[stamp%lookahead].err)
				return nil, false
			}
			slot := &slots[emitCommit%lookahead]
			if stamp <= emitCommit {
				// Not stamped yet: stampAhead stopped at slot(stamp),
				// which is either still in the tree stage or blocked by
				// the window. Hand out what we have first so the caller
				// stays busy, then wait for whichever it is.
				if len(batch) > 0 {
					return batch, true
				}
				blocked := &slots[stamp%lookahead]
				select {
				case <-blocked.done:
				default:
					select {
					case <-stopCh:
						return nil, false
					case works := <-expensiveChan:
						return works, true
					case <-blocked.done:
					case <-slotReady:
					}
					continue
				}
				n := uint64(len(blocked.pairs))
				works, outcome := inFlight.park(func() bool { return stampable(n) }, stopCh, expensiveChan)
				switch outcome {
				case parkStopped:
					return nil, false
				case parkWork:
					return works, true
				}
				continue
			}
			if emitPair == 0 {
				emitEarly = slot.early
			}
			for ; emitPair < len(slot.pairs); emitPair++ {
				if len(emitEarly) > 0 && emitEarly[0] == emitPair {
					emitEarly = emitEarly[1:]
					continue
				}
				s := slot.firstSeq + uint64(emitPair)
				if !admissible(s) {
					if len(batch) > 0 {
						return batch, true
					}
					works, outcome := inFlight.park(func() bool { return admissible(s) }, stopCh, expensiveChan)
					switch outcome {
					case parkStopped:
						return nil, false
					case parkWork:
						return works, true
					}
				}
				if batch == nil {
					batch = make([]dedupPairWork, 0, dedupPairBatchSize)
				}
				batch = append(batch, dedupPairWork{seq: s, work: slot.pairs[emitPair]})
				// In the final stretch, single-pair batches spread the
				// remaining work across workers. The stretch begins once every
				// commit is dispatched and the pairs still ahead of the cursor
				// fit in one batch per worker. The pendingPairs threshold
				// preserves full-sized batches through the body of a scan whose
				// repository is dispatched whole on the first pass.
				if len(batch) == dedupPairBatchSize ||
					(nextDispatch == len(order) && pendingPairs.Load() <= tailPairs) {
					emitPair++
					return batch, true
				}
			}
			pendingPairs.Add(-int64(len(slot.pairs)))
			// The emit cursor is the slot's last reader; dropping the lists
			// lets the pairs be collected before the slot is reused.
			slot.pairs, slot.expensive = nil, nil
			emitPair = 0
			emitCommit++
			emitCursor.Store(int64(emitCommit))
			wakeWaiters()
		}
		seqDone = true
		if len(batch) > 0 {
			return batch, true
		}
		return nil, false
	}
	// takeWork returns the next batch for a hunk worker. Early-forwarded
	// expensive pairs take priority: they were sent ahead of order
	// precisely so their latency overlaps the ordinary stream. Once the
	// ordered stream is exhausted a worker drains whatever is still queued
	// on expensiveChan before it exits.
	takeWork := func() ([]dedupPairWork, bool) {
		select {
		case <-stopCh:
			return nil, false
		case works := <-expensiveChan:
			return works, true
		default:
		}
		if works, ok := takeOrdered(); ok {
			return works, true
		}
		select {
		case works := <-expensiveChan:
			return works, true
		default:
			return nil, false
		}
	}

	// process materializes one pair and delivers its result. It reports
	// false after recording an error.
	process := func(pw dedupPairWork) bool {
		added, err := hs.pairHunks(pw.work)
		if err != nil {
			setError(fmt.Errorf("failed diffing %s in commit %s: %w", pw.work.path, pw.work.commit, err))
			return false
		}
		// pairHunks returns lines that are either compacted into their own
		// buffer or span the whole new blob, so the retained-byte charge
		// below (line bytes) is what a result actually pins.
		hunks := make([]HunkAddition, len(added))
		for i := range added {
			hunks[i] = newHunkAddition(pw.work, added[i])
		}
		// Hash and speculatively probe here, in parallel, so the decision
		// goroutine only touches fingerprints the snapshot did not already
		// hold.
		cands, total := pairCandidates(hunks, set.snapshot())
		deliver(dedupPairResult{
			seq: pw.seq, hunks: hunks, candidates: cands,
			earlyKiB: toKiB(pw.earlyBytes), retainedKiB: toKiB(uint64(total)),
		})
		return true
	}
	// admitted reports whether a worker may materialize seq s now: the
	// batch it belongs to was handed out under the cap, but earlier pairs
	// may have pushed the charge past it since. The seq the decision stage
	// waits for always passes, which keeps that stage draining.
	admitted := func(s uint64) bool {
		return retainedKiB.Load() <= retainedKiBCap || s == inFlight.decided.Load()
	}
	// runBatch materializes every pair in queue, each once admitted. While
	// none is admitted it waits for the charge to drop or the decided count
	// to move, taking early-forwarded expensive work into the same queue so
	// that work is gated too and the decision stage's next seq is never left
	// in the channel. It reports false when the pipeline is stopping.
	anyAdmitted := func(queue []dedupPairWork) bool {
		for _, pw := range queue {
			if admitted(pw.seq) {
				return true
			}
		}
		return false
	}
	runBatch := func(queue []dedupPairWork) bool {
		for len(queue) > 0 {
			for i := 0; i < len(queue); {
				if !admitted(queue[i].seq) {
					i++
					continue
				}
				if !process(queue[i]) {
					return false
				}
				queue = append(queue[:i], queue[i+1:]...)
			}
			if len(queue) == 0 {
				return true
			}
			// Take the wake channel before re-checking so a wake between
			// the check and the select is still seen.
			wake := currentWake()
			if anyAdmitted(queue) {
				continue
			}
			select {
			case <-stopCh:
				return false
			case early := <-expensiveChan:
				queue = append(queue, early...)
			case <-wake:
			}
		}
		return true
	}

	for range numWorkers {
		hunkWG.Add(1)
		go func() {
			defer hunkWG.Done()
			for {
				works, ok := takeWork()
				if !ok {
					return
				}
				if !runBatch(works) {
					return
				}
			}
		}()
	}

	// hunkersDone closes once every hunk worker has exited, so the decision
	// goroutine can tell a quiet ring from a finished scan.
	hunkersDone := make(chan struct{})
	go func() {
		hunkWG.Wait()
		close(hunkersDone)
	}()

	decisionDone := make(chan struct{})
	go func() {
		defer close(decisionDone)
		defer close(yieldChan)
		next := uint64(0)
		batch := dedupYieldBatch{hunks: make([]HunkAddition, 0, dedupYieldBatchHunks)}
		// backlog holds delivered seqs not yet pre-processed by prepBacklog;
		// it is worked through only while next is missing and no new
		// results are waiting, i.e. while this goroutine would otherwise
		// idle behind a straggling pair.
		var backlog []uint64
		scratch := newBacklogScratch()
		// flush hands the current batch to the yield pool and starts a
		// fresh one. It reports false when the pipeline is stopping.
		flush := func() bool {
			if len(batch.hunks) == 0 {
				return true
			}
			select {
			case <-stopCh:
				return false
			case yieldChan <- batch:
			}
			batch = dedupYieldBatch{hunks: make([]HunkAddition, 0, dedupYieldBatchHunks)}
			return true
		}
		// drain settles every pair that is ready in seq order. It reports
		// false when the pipeline is stopping.
		drain := func() bool {
			start := next
			for {
				i := next & ringMask
				if !present[i].Load() {
					break
				}
				r := ring[i]
				ring[i] = dedupPairResult{}
				present[i].Store(false)
				if r.seq != next {
					setError(fmt.Errorf("dedup reorder ring slot %d holds seq %d, want %d", i, r.seq, next))
					return false
				}
				next++
				if r.earlyKiB != 0 {
					earlyBytes.Add(^(uint64(r.earlyKiB)<<10 - 1))
				}
				before := len(batch.hunks)
				batch.hunks = set.decideResult(r, batch.hunks)
				switch {
				case len(batch.hunks) > before:
					batch.kib += int64(r.retainedKiB)
				default:
					releaseRetained(int64(r.retainedKiB))
				}
				if len(batch.hunks) >= dedupYieldBatchHunks && !flush() {
					return false
				}
			}
			if next != start {
				inFlight.advanced(next - start)
				wakeWaiters()
				if next == inFlight.stamped() {
					// Nothing undecided remains, so the backlog is empty
					// in effect; forget it and the scratch it fed.
					backlog = backlog[:0]
					scratch.reset()
				}
			}
			// Release what is ready rather than holding a partial batch
			// until the next result arrives.
			return flush()
		}
		// collect moves newly delivered seqs into the backlog and reports
		// whether there were any.
		collect := func() bool {
			arrivedMu.Lock()
			n := len(arrived)
			backlog = append(backlog, arrived...)
			arrived = arrived[:0]
			arrivedMu.Unlock()
			return n > 0
		}
		for {
			select {
			case <-stopCh:
				return
			case <-resultsReady:
				collect()
				if !drain() {
					return
				}
				continue
			default:
			}
			// Nothing new: pre-process one backlog entry so the drain after
			// the straggler has little left to probe, then block.
			if len(backlog) > 0 {
				seq := backlog[len(backlog)-1]
				backlog = backlog[:len(backlog)-1]
				if seq >= next {
					if i := seq & ringMask; present[i].Load() {
						set.prepBacklog(ring[i], scratch)
					}
				}
				continue
			}
			select {
			case <-stopCh:
				return
			case <-resultsReady:
				collect()
				if !drain() {
					return
				}
			case <-hunkersDone:
				// Late deliveries may have raced the close; one last pass.
				collect()
				drain()
				return
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
					for _, h := range batch.hunks {
						if err := fn(h); err != nil {
							setError(err)
							return
						}
					}
					releaseRetained(batch.kib)
				}
			}
		}()
	}

	// Shutdown chain: close each stage's input only after its upstream
	// senders have exited, and wait for every goroutine so none outlives
	// this call: on the error path stopCh has every stage unblocked.
	hunkWG.Wait()
	close(treeIdxChan)
	treeWG.Wait()
	<-decisionDone
	yieldWG.Wait()

	return firstErr
}
