// commit_fallback.go
//
// Fallback commit-history discovery for repositories that lack a commit-graph
// file.  When no *.graph / commit-graph-chain is present, the HistoryScanner
// falls back to this code path, which reconstructs the same commitInfo list
// by walking reachable commits from refs.
//
// The entry point is loadFromRefs, which:
//  1. Collects all ref tips (HEAD, refs/*, packed-refs).
//  2. Performs a depth-first, stack-based walk over the commit DAG.
//  3. Resolves annotated tags to their target commits.
//  4. Returns commits in parent-first topological order.
//
// Because every commit must be individually inflated from a packfile (or
// loose object), this path is significantly slower than the commit-graph
// reader and should only be used when the commit-graph is unavailable.

package objstore

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// loadFromRefs is the entry point for the commit-graph fallback path.
// It discovers all ref tips, walks the reachable commit DAG, and returns
// commits ordered parent-first (i.e. every parent appears before its
// children) so that downstream consumers can process them in a single
// forward pass.
func (hs *HistoryScanner) loadFromRefs() ([]commitInfo, error) {
	out := make([]commitInfo, 0, 256)
	if err := hs.walkCommitsFromRefs(func(info commitInfo) error {
		out = append(out, info)
		return nil
	}); err != nil {
		return nil, err
	}
	return orderCommitsParentFirst(out), nil
}

// walkCommitsFromRefs performs a ref-based reachable commit walk and calls visit
// once per commit.
//
// The walk is a parallel BFS over the commit DAG: header inflation (a zlib
// decompression per commit) dominates the cost and is embarrassingly
// parallel, so a serial walk leaves the machine idle while downstream
// pipeline stages starve. Worker goroutines pop frontier OIDs from a shared
// stack, inflate and parse headers concurrently, then push unseen parents.
//
// Ordering: visits happen in nondeterministic order. Both callers tolerate
// this — DiffHistoryHunks processes commits independently, and loadFromRefs
// re-establishes parent-first order via orderCommitsParentFirst.
//
// visit is invoked under an internal mutex, so it may touch caller state
// without additional locking, but it must not block indefinitely.
func (hs *HistoryScanner) walkCommitsFromRefs(visit func(commitInfo) error) error {
	if visit == nil {
		return nil
	}

	tips, err := collectRefTips(hs.gitDir)
	if err != nil {
		return err
	}
	if len(tips) == 0 {
		return nil
	}

	numWorkers := min(runtime.NumCPU(), 16)

	var (
		mu       sync.Mutex // guards seen, stack, active, firstErr
		cond     = sync.NewCond(&mu)
		seen     = make(map[Hash]struct{}, len(tips)*4)
		stack    = append([]Hash(nil), tips...)
		active   int
		firstErr error
		visitMu  sync.Mutex
	)

	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cond.Broadcast()
	}

	worker := func() {
		for {
			mu.Lock()
			for len(stack) == 0 && active > 0 && firstErr == nil {
				cond.Wait()
			}
			if firstErr != nil || len(stack) == 0 {
				// Done: either an error occurred, or the stack is empty with
				// no worker still processing (which could add more work).
				// Broadcast so peers blocked in Wait also observe the
				// termination condition and exit.
				mu.Unlock()
				cond.Broadcast()
				return
			}
			n := len(stack) - 1
			oid := stack[n]
			stack = stack[:n]
			if _, ok := seen[oid]; ok {
				mu.Unlock()
				continue
			}
			seen[oid] = struct{}{}
			active++
			mu.Unlock()

			pushed := hs.walkOne(oid, visit, &visitMu, func(next Hash) {
				mu.Lock()
				if _, ok := seen[next]; !ok {
					stack = append(stack, next)
				}
				mu.Unlock()
			}, fail)

			mu.Lock()
			active--
			mu.Unlock()
			// Wake waiters: either new work was pushed or active hit zero.
			if pushed {
				cond.Broadcast()
			} else {
				cond.Signal()
			}
		}
	}

	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker()
		}()
	}
	wg.Wait()

	return firstErr
}

// walkOne processes a single OID popped from the walk frontier: it inflates
// the commit header, invokes visit, and enqueues parents via push. Tag
// objects are peeled to their target. The return value reports whether any
// new OIDs were pushed. Errors are reported through fail.
func (hs *HistoryScanner) walkOne(
	oid Hash,
	visit func(commitInfo) error,
	visitMu *sync.Mutex,
	push func(Hash),
	fail func(error),
) (pushed bool) {
	hdr, err := hs.store.readCommitHeader(oid)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			// Stale refs and shallow parents can legitimately point to objects
			// absent from local packs. Skip and continue the reachable walk.
			return false
		}
		if !errors.Is(err, ErrObjectNotCommit) {
			fail(fmt.Errorf("read commit header %s: %w", oid, err))
			return false
		}
		// Non-commit refs (tags, trees, etc.) are allowed.
		target, ok, tagErr := hs.resolveTagTarget(oid)
		if tagErr != nil {
			if errors.Is(tagErr, ErrObjectNotFound) {
				return false
			}
			fail(tagErr)
			return false
		}
		if ok {
			push(target)
			return true
		}
		return false
	}

	info, err := parseCommitInfoFromHeader(oid, hdr)
	if err != nil {
		fail(err)
		return false
	}

	visitMu.Lock()
	err = visit(info)
	visitMu.Unlock()
	if err != nil {
		fail(err)
		return false
	}

	for _, p := range info.ParentOIDs {
		push(p)
		pushed = true
	}
	return pushed
}

// resolveTagTarget attempts to peel an annotated tag object to find its
// ultimate target commit OID.
//
// Return semantics:
//   - (target, true, nil)  -- oid is a tag whose "object" header points at a
//     commit or another tag (which will be resolved on the next walk iteration).
//   - (zero, false, nil)   -- oid is a tag that points at a non-commit/non-tag
//     object (e.g. a tree or blob), or has no "object" header at all. The
//     caller should skip it silently.
//   - (zero, false, err)   -- an I/O or parse error occurred.
func (hs *HistoryScanner) resolveTagTarget(oid Hash) (Hash, bool, error) {
	data, typ, err := hs.store.getMaterialized(oid)
	if err != nil {
		return Hash{}, false, err
	}
	if typ != ObjTag {
		return Hash{}, false, nil
	}

	var (
		targetOID  Hash
		targetType string
	)

	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			break
		}

		switch {
		case bytes.HasPrefix(line, []byte("object ")):
			h, ok := parseHashToken(btostr(line[len("object "):]))
			if !ok {
				return Hash{}, false, fmt.Errorf("invalid tag target hash in %s", oid)
			}
			targetOID = h
		case bytes.HasPrefix(line, []byte("type ")):
			targetType = btostr(line[len("type "):])
		}
	}
	if err := sc.Err(); err != nil {
		return Hash{}, false, err
	}

	if targetOID.IsZero() {
		return Hash{}, false, nil
	}
	if targetType != "" && targetType != "commit" && targetType != "tag" {
		return Hash{}, false, nil
	}
	return targetOID, true, nil
}

// collectRefTips discovers all unique commit-ish OIDs reachable from the
// repository's refs. The search proceeds in the following order:
//
//  1. HEAD          -- resolved through its symref (if any) or as a detached hash.
//  2. refs/**       -- every loose ref file under .git/refs/ is read.
//  3. packed-refs   -- the packed-refs file is scanned for both regular lines
//     and peeled "^" lines (which record the peeled tag target).
//
// Duplicates are suppressed via a seen-set so each OID appears at most once in
// the returned slice. The output is sorted by hash for deterministic ordering
// in downstream consumers.
func collectRefTips(gitDir string) ([]Hash, error) {
	seen := make(map[Hash]struct{}, 128)
	out := make([]Hash, 0, 128)

	add := func(h Hash) {
		if h.IsZero() {
			return
		}
		if _, ok := seen[h]; ok {
			return
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}

	headPath := filepath.Join(gitDir, "HEAD")
	if headData, err := os.ReadFile(headPath); err == nil {
		headLine := strings.TrimSpace(string(headData))
		if strings.HasPrefix(headLine, "ref: ") {
			refName := strings.TrimSpace(strings.TrimPrefix(headLine, "ref: "))
			h, ok, err := readRefHash(gitDir, refName)
			if err != nil {
				return nil, err
			}
			if ok {
				add(h)
			}
		} else if h, ok := parseHashToken(headLine); ok {
			add(h)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	refsDir := filepath.Join(gitDir, "refs")
	if err := filepath.WalkDir(refsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if h, ok := parseHashToken(strings.TrimSpace(string(b))); ok {
			add(h)
		}
		return nil
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	packedRefsPath := filepath.Join(gitDir, "packed-refs")
	if packed, err := os.ReadFile(packedRefsPath); err == nil {
		sc := bufio.NewScanner(bytes.NewReader(packed))
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			switch {
			case line == "", strings.HasPrefix(line, "#"), strings.HasPrefix(line, "^"):
				if strings.HasPrefix(line, "^") {
					if h, ok := parseHashToken(strings.TrimPrefix(line, "^")); ok {
						add(h)
					}
				}
				continue
			}

			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			if h, ok := parseHashToken(fields[0]); ok {
				add(h)
			}
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	slices.SortFunc(out, func(a, b Hash) int {
		return bytes.Compare(a[:], b[:])
	})
	return out, nil
}

func readRefHash(gitDir, refName string) (Hash, bool, error) {
	refPath := filepath.Join(gitDir, filepath.FromSlash(refName))
	if b, err := os.ReadFile(refPath); err == nil {
		h, ok := parseHashToken(strings.TrimSpace(string(b)))
		return h, ok, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Hash{}, false, err
	}

	packedRefsPath := filepath.Join(gitDir, "packed-refs")
	packed, err := os.ReadFile(packedRefsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Hash{}, false, nil
		}
		return Hash{}, false, err
	}

	sc := bufio.NewScanner(bytes.NewReader(packed))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "^") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[1] != refName {
			continue
		}
		h, ok := parseHashToken(fields[0])
		return h, ok, nil
	}
	if err := sc.Err(); err != nil {
		return Hash{}, false, err
	}
	return Hash{}, false, nil
}

// parseCommitInfoFromHeader extracts structured metadata from the raw header
// bytes of a commit object.
//
// Required fields: the header MUST contain at least one "tree" line and one
// "committer" line. If either is missing, the function returns an error.
// "parent" lines are optional (root commits have none); zero or more may be
// present and are collected in order.
//
// The function stops scanning at the first blank line (the separator between
// header and body in the Git commit format), so the body is never inspected.
func parseCommitInfoFromHeader(oid Hash, hdr []byte) (commitInfo, error) {
	info := commitInfo{
		OID:        oid,
		ParentOIDs: make([]Hash, 0, 2),
	}

	sc := bufio.NewScanner(bytes.NewReader(hdr))
	var haveTree, haveTS bool
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			break
		}

		switch {
		case bytes.HasPrefix(line, []byte("tree ")):
			treeOID, ok := parseHashToken(btostr(line[len("tree "):]))
			if !ok {
				return commitInfo{}, fmt.Errorf("invalid tree hash in commit %s", oid)
			}
			info.TreeOID = treeOID
			haveTree = true
		case bytes.HasPrefix(line, []byte("parent ")):
			parentOID, ok := parseHashToken(btostr(line[len("parent "):]))
			if !ok {
				return commitInfo{}, fmt.Errorf("invalid parent hash in commit %s", oid)
			}
			info.ParentOIDs = append(info.ParentOIDs, parentOID)
		case bytes.HasPrefix(line, []byte("committer ")):
			ts, err := parseHeaderTimestamp(line)
			if err != nil {
				return commitInfo{}, err
			}
			info.Timestamp = ts
			haveTS = true
		}
	}
	if err := sc.Err(); err != nil {
		return commitInfo{}, err
	}

	if !haveTree {
		return commitInfo{}, fmt.Errorf("missing tree line in commit %s", oid)
	}
	if !haveTS {
		return commitInfo{}, fmt.Errorf("missing committer timestamp in commit %s", oid)
	}

	return info, nil
}

// parseHeaderTimestamp extracts the Unix epoch timestamp from a "committer"
// (or "author") header line.
//
// The Git format is: "committer <name> <<email>> <timestamp> <tz>", where
// fields are whitespace-separated. The timestamp is always the second-to-last
// field (len(fields)-2), and the timezone is the last. Indexing from the end
// avoids having to handle names or emails that contain spaces.
func parseHeaderTimestamp(line []byte) (int64, error) {
	fields := bytes.Fields(line)
	if len(fields) < 3 {
		return 0, fmt.Errorf("invalid committer line: %q", string(line))
	}
	// "committer <name...> <timestamp> <tz>"
	secField := fields[len(fields)-2]
	ts, err := strconv.ParseInt(btostr(secField), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid committer timestamp %q: %w", btostr(secField), err)
	}
	return ts, nil
}

// parseHashToken extracts a SHA-1 hash from the beginning of text.
//
// The function trims leading/trailing whitespace, then considers only the
// first 40 characters. Any trailing content (e.g. a ref name after a space
// in packed-refs, or a newline) is silently ignored -- the truncation to
// exactly 40 hex characters is intentional so that lines like
// "abc123... refs/heads/main" are handled without an explicit split.
//
// Returns the parsed hash and true on success, or (zero, false) if text is
// shorter than 40 characters or is not valid hex.
func parseHashToken(text string) (Hash, bool) {
	text = strings.TrimSpace(text)
	if len(text) < 40 {
		return Hash{}, false
	}
	h, err := ParseHash(text[:40])
	if err != nil {
		return Hash{}, false
	}
	return h, true
}
