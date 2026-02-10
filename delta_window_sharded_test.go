package objstore

import "testing"

func TestShardedDeltaWindow_BasicAddAcquire(t *testing.T) {
	w := newShardedDeltaWindow(8, 8<<20)

	var oid Hash
	oid[0] = 0x7f
	data := []byte("hello")
	if err := w.add(oid, data, ObjBlob); err != nil {
		t.Fatalf("add: %v", err)
	}

	h, ok := w.acquire(oid)
	if !ok {
		t.Fatalf("expected cache hit")
	}
	defer h.Release()

	if got := string(h.Data()); got != "hello" {
		t.Fatalf("unexpected data: %q", got)
	}
	if got := h.Type(); got != ObjBlob {
		t.Fatalf("unexpected type: %v", got)
	}
}
