package objstore

import "testing"

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
