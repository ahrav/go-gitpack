// commit_enum.go
//
// Pack-order commit enumeration for the ref-walk commit load.
//
// A reachable walk over the commit DAG discovers each commit only after
// inflating its child, so its critical path is the first-parent chain: one
// header inflation after another, however many workers run. On the rails
// history that chain held the sixteen-worker walk to 176 ms at a quarter of
// its CPU capacity. The pack index already names every object, so the
// commits stored whole in the packs are read here in parallel, index range
// by index range, and the reachable walk then runs over them in memory,
// touching the store only for commits the enumeration did not cover:
// delta-encoded commits (under 1% of rails), loose commits, and tags.

package objstore

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// enumChunk is how many index entries an enumeration worker claims at a
// time; a contiguous range keeps its oidTable and entry reads sequential.
const enumChunk = 2048

// enumeratePackedCommits parses every commit object stored whole (not as a
// delta) in the store's packs and returns their commitInfos in no particular
// order. Objects that fail to inflate or parse are left out rather than
// reported: an unreachable object cannot fail a scan, and a reachable one is
// read again, with its error, by the walk that follows.
func (s *store) enumeratePackedCommits(numWorkers int) []commitInfo {
	type packRange struct {
		pf    *idxFile
		start int
	}
	var ranges []packRange
	for _, pf := range s.packs {
		for start := 0; start < len(pf.entries); start += enumChunk {
			ranges = append(ranges, packRange{pf, start})
		}
	}
	if len(ranges) == 0 {
		return nil
	}
	numWorkers = max(1, min(numWorkers, len(ranges)))

	var (
		next    atomic.Int64
		mu      sync.Mutex
		results [][]commitInfo
		wg      sync.WaitGroup
	)
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out []commitInfo
			scratch := getDeltaScratch(maxOneShotCommitBytes)
			defer putDeltaScratch(scratch)
			for {
				i := int(next.Add(1) - 1)
				if i >= len(ranges) {
					break
				}
				r := ranges[i]
				end := min(r.start+enumChunk, len(r.pf.entries))
				for j := r.start; j < end; j++ {
					off := r.pf.entries[j].offset
					var hdr [32]byte
					n, _ := r.pf.pack.ReadAt(hdr[:], int64(off))
					typ, size, hdrLen := parseObjectHeaderUnsafe(hdr[:n])
					if hdrLen <= 0 || typ != ObjCommit || size > maxOneShotCommitBytes {
						continue
					}
					buf := scratch.buf[:size]
					if err := inflateExact(r.pf.pack, int64(off)+int64(hdrLen), buf); err != nil {
						continue
					}
					// parseCommitInfoFromHeader stops at the blank line
					// that ends the header, so the message is never read.
					info, err := parseCommitInfoFromHeader(r.pf.oidTable[j], buf)
					if err != nil {
						continue
					}
					out = append(out, info)
				}
			}
			mu.Lock()
			results = append(results, out)
			mu.Unlock()
		}()
	}
	wg.Wait()

	total := 0
	for _, r := range results {
		total += len(r)
	}
	all := make([]commitInfo, 0, total)
	for _, r := range results {
		all = append(all, r...)
	}
	return all
}

// enumShards is the number of OID-prefix shards the enumerated-commit index
// is split into so that it can be built and queried in parallel; a power of
// two.
const enumShards = 16

// enumReadQueue bounds the uncovered OIDs queued for the reader pool and
// the finished reads waiting to be folded back into the walk. Uncovered
// commits are rare (delta-encoded commits, tags, loose commits), and the walk
// keeps going over the enumerated commits while they are read.
const enumReadQueue = 256

// enumIndex maps enumerated commit OIDs to their positions, sharded by the
// leading OID bits.
type enumIndex [enumShards]map[Hash]int32

func (ix *enumIndex) lookup(oid Hash) (int32, bool) {
	i, ok := ix[int(oid[0])>>4][oid]
	return i, ok
}

// loadCommitsReachable returns every commit reachable from tips, in no
// particular order, with each commit's parents resolved to positions in the
// result (-1 for a parent absent from the store). Commits the pack
// enumeration covered are followed in memory over their parent indexes; the
// walk reads the rest through walkOne, in parallel batches, with the same
// handling of tags, stale refs and shallow boundaries as the ref walk.
func (hs *HistoryScanner) loadCommitsReachable(tips []Hash) ([]commitInfo, commitParents, error) {
	numWorkers := runtime.NumCPU()
	enumerated := hs.store.enumeratePackedCommits(numWorkers)

	var ix enumIndex
	for sh := range enumShards {
		ix[sh] = make(map[Hash]int32, len(enumerated)/enumShards+1)
	}
	var wg sync.WaitGroup
	for sh := range enumShards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m := ix[sh]
			for i := range enumerated {
				if oid := &enumerated[i].OID; int(oid[0])>>4 == sh {
					m[*oid] = int32(i)
				}
			}
		}()
	}
	wg.Wait()

	// parentIdx holds each enumerated commit's parents as indexes into
	// enumerated, -1 for a parent the enumeration did not cover, in one
	// flat array: parentIdx[parentStart[i]:parentStart[i+1]].
	parentStart := make([]int32, len(enumerated)+1)
	for i := range enumerated {
		parentStart[i+1] = parentStart[i] + int32(len(enumerated[i].ParentOIDs))
	}
	parentIdx := make([]int32, parentStart[len(enumerated)])
	var next atomic.Int64
	const resolveChunk = 4096
	for range min(numWorkers, len(enumerated)/resolveChunk+1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				start := int(next.Add(resolveChunk) - resolveChunk)
				if start >= len(enumerated) {
					return
				}
				for i := start; i < min(start+resolveChunk, len(enumerated)); i++ {
					dst := parentIdx[parentStart[i]:parentStart[i+1]]
					for j, p := range enumerated[i].ParentOIDs {
						if k, ok := ix.lookup(p); ok {
							dst[j] = k
						} else {
							dst[j] = -1
						}
					}
				}
			}
		}()
	}
	wg.Wait()

	visited := make([]bool, len(enumerated))
	seenOther := make(map[Hash]struct{})
	// The walk itself touches only the int32 arrays above, which fit the
	// cache, and records the enumerated commits it reaches as visitOrder;
	// the result is assembled from it afterwards, in parallel. others
	// collects the store-read commits in the order their reads were
	// consumed; they follow the enumerated commits in the result.
	visitOrder := make([]int32, 0, len(enumerated))
	var others []commitInfo
	var stack []int32 // enumerated commits to visit
	var classify func(Hash)

	// Uncovered OIDs go to a reader pool through readReq while the walk
	// continues over the enumerated commits; each read comes back on
	// readRes with the commit (or nothing, for a stale ref or a tag whose
	// target is pushed) and the OIDs to classify next. inFlight counts
	// reads requested and not yet consumed.
	type read struct {
		infos  []commitInfo
		pushes []Hash
		err    error
	}
	readReq := make(chan Hash, enumReadQueue)
	readRes := make(chan read, enumReadQueue)
	readers := min(numWorkers, enumReadQueue)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for oid := range readReq {
				var r read
				hs.walkOne(oid,
					func(info commitInfo) error { r.infos = append(r.infos, info); return nil },
					func(next Hash) { r.pushes = append(r.pushes, next) },
					func(err error) {
						if r.err == nil {
							r.err = err
						}
					})
				readRes <- r
			}
		}()
	}
	defer func() {
		close(readReq)
		wg.Wait()
	}()
	inFlight := 0
	var firstErr error
	// consume folds one finished read into the walk.
	consume := func(r read) {
		inFlight--
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		others = append(others, r.infos...)
		for _, oid := range r.pushes {
			classify(oid)
		}
	}
	// request hands an uncovered OID to the readers, consuming finished
	// reads while the queue is full so the request never deadlocks against
	// a reader blocked on a full result channel.
	request := func(oid Hash) {
		for {
			select {
			case readReq <- oid:
				inFlight++
				return
			case r := <-readRes:
				consume(r)
			}
		}
	}
	// classify routes an OID to the in-memory stack or the readers.
	classify = func(oid Hash) {
		if i, ok := ix.lookup(oid); ok {
			if !visited[i] {
				visited[i] = true
				stack = append(stack, i)
			}
			return
		}
		if _, ok := seenOther[oid]; ok {
			return
		}
		seenOther[oid] = struct{}{}
		request(oid)
	}
	// assemble builds the result from visitOrder and others and resolves
	// every result's parents to result positions: pos maps an enumerated
	// commit to its position, otherPos a store-read one. The scatter and
	// the resolution run in parallel chunks; the maps are read-only here.
	assemble := func() ([]commitInfo, commitParents) {
		ne := len(visitOrder)
		out := make([]commitInfo, ne+len(others))
		copy(out[ne:], others)
		pos := make([]int32, len(enumerated))
		otherPos := make(map[Hash]int32, len(others))
		for q := range others {
			otherPos[others[q].OID] = int32(ne + q)
		}
		// The reader pool is still registered on wg until the deferred
		// close, so the chunk workers use their own group.
		const chunk = 4096
		var next atomic.Int64
		var cw sync.WaitGroup
		for range min(numWorkers, ne/chunk+1) {
			cw.Add(1)
			go func() {
				defer cw.Done()
				for {
					lo := int(next.Add(chunk) - chunk)
					if lo >= ne {
						return
					}
					for p := lo; p < min(lo+chunk, ne); p++ {
						i := visitOrder[p]
						out[p] = enumerated[i]
						pos[i] = int32(p)
					}
				}
			}()
		}
		cw.Wait()
		start := make([]int32, len(out)+1)
		for i := range out {
			start[i+1] = start[i] + int32(len(out[i].ParentOIDs))
		}
		idx := make([]int32, start[len(out)])
		next.Store(0)
		for range min(numWorkers, len(out)/chunk+1) {
			cw.Add(1)
			go func() {
				defer cw.Done()
				for {
					lo := int(next.Add(chunk) - chunk)
					if lo >= len(out) {
						return
					}
					for i := lo; i < min(lo+chunk, len(out)); i++ {
						dst := idx[start[i]:start[i+1]]
						if i < ne {
							src := visitOrder[i]
							for j, k := range parentIdx[parentStart[src]:parentStart[src+1]] {
								if k >= 0 {
									dst[j] = pos[k]
									continue
								}
								if p, ok := otherPos[out[i].ParentOIDs[j]]; ok {
									dst[j] = p
								} else {
									dst[j] = -1
								}
							}
							continue
						}
						for j, oid := range out[i].ParentOIDs {
							if k, ok := ix.lookup(oid); ok {
								dst[j] = pos[k]
							} else if p, ok := otherPos[oid]; ok {
								dst[j] = p
							} else {
								dst[j] = -1
							}
						}
					}
				}
			}()
		}
		cw.Wait()
		return out, commitParents{start: start, idx: idx}
	}
	for _, tip := range tips {
		classify(tip)
	}
	for firstErr == nil {
		for len(stack) > 0 {
			n := len(stack) - 1
			i := stack[n]
			stack = stack[:n]
			visitOrder = append(visitOrder, i)
			for j, k := range parentIdx[parentStart[i]:parentStart[i+1]] {
				if k < 0 {
					oid := enumerated[i].ParentOIDs[j]
					if _, ok := seenOther[oid]; !ok {
						seenOther[oid] = struct{}{}
						request(oid)
					}
					continue
				}
				if !visited[k] {
					visited[k] = true
					stack = append(stack, k)
				}
			}
		}
		if inFlight == 0 {
			break
		}
		// Nothing left in memory: wait for a read, then take every other
		// finished read before resuming the walk.
		consume(<-readRes)
		for len(readRes) > 0 {
			consume(<-readRes)
		}
	}
	for inFlight > 0 {
		consume(<-readRes)
	}
	if firstErr != nil {
		return nil, commitParents{}, firstErr
	}
	out, parents := assemble()
	return out, parents, nil
}
