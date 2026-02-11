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
	"slices"
	"strconv"
	"strings"
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

	seen := make(map[Hash]struct{}, len(tips)*4)
	stack := append([]Hash(nil), tips...)

	for len(stack) > 0 {
		n := len(stack) - 1
		oid := stack[n]
		stack = stack[:n]

		if _, ok := seen[oid]; ok {
			continue
		}
		seen[oid] = struct{}{}

		hdr, err := hs.store.readCommitHeader(oid)
		if err != nil {
			if errors.Is(err, ErrObjectNotFound) {
				// Stale refs and shallow parents can legitimately point to objects
				// absent from local packs. Skip and continue the reachable walk.
				continue
			}
			if !errors.Is(err, ErrObjectNotCommit) {
				return fmt.Errorf("read commit header %s: %w", oid, err)
			}
			// Non-commit refs (tags, trees, etc.) are allowed.
			target, ok, tagErr := hs.resolveTagTarget(oid)
			if tagErr != nil {
				if errors.Is(tagErr, ErrObjectNotFound) {
					continue
				}
				return tagErr
			}
			if ok {
				stack = append(stack, target)
			}
			continue
		}

		info, err := parseCommitInfoFromHeader(oid, hdr)
		if err != nil {
			return err
		}
		if err := visit(info); err != nil {
			return err
		}

		for _, p := range info.ParentOIDs {
			if _, ok := seen[p]; ok {
				continue
			}
			stack = append(stack, p)
		}
	}

	return nil
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
