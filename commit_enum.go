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

// enumBatch is how many uncovered OIDs the reachable walk gathers before
// reading them from the store in parallel. Uncovered commits are rare, so a
// batch is usually a few delta-encoded commits or tags; reading them one at
// a time serialized about a third of the load on the rails history.
const enumBatch = 256

// enumIndex maps enumerated commit OIDs to their positions, sharded by the
// leading OID bits.
type enumIndex [enumShards]map[Hash]int32

func (ix *enumIndex) lookup(oid Hash) (int32, bool) {
	i, ok := ix[int(oid[0])>>4][oid]
	return i, ok
}

// loadCommitsReachable returns every commit reachable from tips, in no
// particular order. Commits the pack enumeration covered are followed in
// memory over their parent indexes; the walk reads the rest through walkOne,
// in parallel batches, with the same handling of tags, stale refs and shallow
// boundaries as the ref walk.
func (hs *HistoryScanner) loadCommitsReachable(tips []Hash) ([]commitInfo, error) {
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
	out := make([]commitInfo, 0, len(enumerated))
	var stack []int32   // enumerated commits to visit
	var pending []Hash  // uncovered OIDs to read from the store
	var oidStack []Hash // OIDs from store reads, not yet classified
	// classify routes an OID to the in-memory stack or the pending batch.
	classify := func(oid Hash) {
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
		pending = append(pending, oid)
	}
	// readPending reads the gathered uncovered OIDs in parallel and feeds
	// their commits and parents (or tag targets) back into the walk.
	readPending := func() error {
		type read struct {
			infos  []commitInfo
			pushes []Hash
			err    error
		}
		reads := make([]read, len(pending))
		var next atomic.Int64
		for range min(numWorkers, len(pending)) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1) - 1)
					if i >= len(pending) {
						return
					}
					r := &reads[i]
					hs.walkOne(pending[i],
						func(info commitInfo) error { r.infos = append(r.infos, info); return nil },
						func(next Hash) { r.pushes = append(r.pushes, next) },
						func(err error) {
							if r.err == nil {
								r.err = err
							}
						})
				}
			}()
		}
		wg.Wait()
		pending = pending[:0]
		for i := range reads {
			if reads[i].err != nil {
				return reads[i].err
			}
			out = append(out, reads[i].infos...)
			oidStack = append(oidStack, reads[i].pushes...)
		}
		return nil
	}
	for _, tip := range tips {
		classify(tip)
	}
	for {
		for len(stack) > 0 || len(oidStack) > 0 {
			for len(oidStack) > 0 {
				n := len(oidStack) - 1
				oid := oidStack[n]
				oidStack = oidStack[:n]
				classify(oid)
			}
			for len(stack) > 0 {
				n := len(stack) - 1
				i := stack[n]
				stack = stack[:n]
				out = append(out, enumerated[i])
				for j, k := range parentIdx[parentStart[i]:parentStart[i+1]] {
					if k < 0 {
						oid := enumerated[i].ParentOIDs[j]
						if _, ok := seenOther[oid]; !ok {
							seenOther[oid] = struct{}{}
							pending = append(pending, oid)
						}
						continue
					}
					if !visited[k] {
						visited[k] = true
						stack = append(stack, k)
					}
				}
			}
			if len(pending) >= enumBatch {
				if err := readPending(); err != nil {
					return nil, err
				}
			}
		}
		if len(pending) == 0 {
			return out, nil
		}
		if err := readPending(); err != nil {
			return nil, err
		}
	}
}
