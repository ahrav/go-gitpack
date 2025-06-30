package objstore

import "unsafe"

// zero‑copy []byte → string (safe as long as old/new aren’t mutated afterwards).
func btostr(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
