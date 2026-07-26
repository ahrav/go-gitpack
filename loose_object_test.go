// loose_object_test.go covers the loose-object read path's memory contract.
//
// Loose objects are the one path that does not learn an object's size before
// allocating for it: pack reads size the target from the object header, while
// readLooseObject streams the zlib body and validates the length afterwards.
// That makes it the path where a returned buffer can carry spare capacity, and
// callers that alias an object buffer instead of copying it — pairCache.add for
// whole-blob hunk results, the offset cache's admission check — charge only the
// bytes the object reports.
package objstore

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestReadLooseObject_ReturnsExactCapacityBuffer asserts the returned slice's
// capacity equals its length.
//
// io.ReadAll grows by append and lands on a capacity from the allocator's size
// classes, so a small object comes back with hundreds of bytes of slack. Any
// consumer that retains the slice retains that slack while accounting for only
// len(data), so the slack is invisible to every budget in the package.
func TestReadLooseObject_ReturnsExactCapacityBuffer(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repo := t.TempDir()
	run := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t",
			"GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t",
			"GIT_COMMITTER_EMAIL=t@example.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %s", args, string(out))
		}
		return strings.TrimSpace(string(out))
	}

	run("init", "--quiet")

	// Sizes chosen to sit inside, on, and past io.ReadAll's 512-byte starting
	// capacity so the assertion holds across its growth steps rather than for
	// one lucky size.
	cases := map[string]int{
		"tiny.txt":  6,
		"exact.txt": 512,
		"grown.txt": 5000,
	}
	for name, size := range cases {
		body := strings.Repeat("a", size)
		if err := os.WriteFile(filepath.Join(repo, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	run("add", ".")
	run("commit", "-m", "loose", "--quiet")
	// No repack: the blobs stay loose, which is what routes reads through
	// readLooseObject rather than the pack path.

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("NewHistoryScanner: %v", err)
	}
	defer scanner.Close()
	s := scanner.store

	for name, size := range cases {
		oidText := run("rev-parse", "HEAD:"+name)
		oid, err := ParseHash(oidText)
		if err != nil {
			t.Fatalf("ParseHash(%q): %v", oidText, err)
		}

		data, typ, err := s.readLooseObject(oid)
		if err != nil {
			t.Fatalf("readLooseObject(%s): %v", name, err)
		}
		if typ != ObjBlob {
			t.Fatalf("%s: type = %v, want %v", name, typ, ObjBlob)
		}
		if len(data) != size {
			t.Fatalf("%s: len = %d, want %d", name, len(data), size)
		}
		if cap(data) != len(data) {
			t.Errorf("%s: cap = %d, len = %d — spare capacity is retained by "+
				"aliasing callers but charged by none",
				name, cap(data), len(data))
		}
	}
}
