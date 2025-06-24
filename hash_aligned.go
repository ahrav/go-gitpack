//go:build !arm || arm64
// +build !arm arm64

package objstore

import "unsafe"

// Uint64 returns the first eight bytes of h as an implementation-native uint64.
// This version uses unsafe cast for aligned architectures.
func (h Hash) Uint64() uint64 { return *(*uint64)(unsafe.Pointer(&h[0])) }
