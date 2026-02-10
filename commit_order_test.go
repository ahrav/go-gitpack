// commit_order_test.go tests the topological ordering of commits used to
// ensure that parent commits are always processed before their children during
// history scanning.

package objstore

import "testing"

// TestOrderCommitsParentFirst verifies that orderCommitsParentFirst produces a
// topological ordering where every parent appears before all of its children.
//
// The test uses the following DAG:
//
//	root (t=10)
//	├── mid  (t=20)
//	│   └── leaf (t=30)
//	└── side (t=25)
//
// The input slice is deliberately unsorted (leaf, side, root, mid) to confirm
// that the function does not depend on input order. The assertions check:
//   - root appears before both mid and side.
//   - mid appears before leaf.
//
// No ordering constraint exists between mid and side because they are on
// independent branches.
func TestOrderCommitsParentFirst(t *testing.T) {
	root := Hash{1}
	mid := Hash{2}
	leaf := Hash{3}
	side := Hash{4}

	in := []commitInfo{
		{OID: leaf, ParentOIDs: []Hash{mid}, Timestamp: 30},
		{OID: side, ParentOIDs: []Hash{root}, Timestamp: 25},
		{OID: root, ParentOIDs: nil, Timestamp: 10},
		{OID: mid, ParentOIDs: []Hash{root}, Timestamp: 20},
	}

	out := orderCommitsParentFirst(in)
	pos := make(map[Hash]int, len(out))
	for i, c := range out {
		pos[c.OID] = i
	}

	// root before mid and side, mid before leaf.
	if pos[root] >= pos[mid] {
		t.Fatalf("expected root before mid: %+v", out)
	}
	if pos[root] >= pos[side] {
		t.Fatalf("expected root before side: %+v", out)
	}
	if pos[mid] >= pos[leaf] {
		t.Fatalf("expected mid before leaf: %+v", out)
	}
}
