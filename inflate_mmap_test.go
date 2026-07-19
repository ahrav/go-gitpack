//go:build linux || darwin || windows

package objstore

import "testing"

func TestGetZlibReaderAtRejectsOversizedWindow(t *testing.T) {
	// CM=deflate and FCHECK is valid, but CINFO=8 exceeds RFC 1950's limit.
	data := []byte{0x88, 0x1c}
	if _, _, err := getZlibReaderAt(data, 0); err == nil {
		t.Fatal("expected invalid CINFO to be rejected")
	}
}
