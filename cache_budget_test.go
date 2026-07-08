package objstore

import "testing"

func TestPairCacheBudgetCanDisableAndRetain(t *testing.T) {
	var oldOID, newOID Hash
	newOID[0] = 1
	key := makePairKey(oldOID, newOID)
	hunks := []AddedHunk{{StartLine: 1, Lines: []string{"secret"}}}

	disabled := newPairCacheWithBudget(0)
	disabled.add(key, hunks)
	if _, ok := disabled.get(key); ok {
		t.Fatalf("pair cache retained an entry with a zero budget")
	}

	enabled := newPairCacheWithBudget(1 << 20)
	enabled.add(key, hunks)
	if got, ok := enabled.get(key); !ok || len(got) != 1 || got[0].Lines[0] != "secret" {
		t.Fatalf("pair cache did not retain entry under budget: ok=%v got=%v", ok, got)
	}
}

func TestOffsetCacheBudgetCanDisableAndRetain(t *testing.T) {
	disabled := newOffsetCacheWithBudget(0)
	disabled.add(nil, 1, []byte("blob"), ObjBlob)
	if _, _, ok := disabled.get(nil, 1); ok {
		t.Fatalf("offset cache retained an entry with a zero budget")
	}

	enabled := newOffsetCacheWithBudget(1 << 20)
	enabled.add(nil, 1, []byte("blob"), ObjBlob)
	got, typ, ok := enabled.get(nil, 1)
	if !ok || typ != ObjBlob || string(got) != "blob" {
		t.Fatalf("offset cache did not retain entry under budget: ok=%v typ=%v got=%q", ok, typ, got)
	}
}
