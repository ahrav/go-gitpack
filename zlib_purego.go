//go:build !cgo || !gitpack_libdeflate

package objstore

// libdeflateAvailable reports that this build uses the pure-Go flate backend.
const libdeflateAvailable = false

// inflateZlibOneShot is unreachable in pure-Go builds; the callers branch on
// libdeflateAvailable before calling it.
func inflateZlibOneShot(src []byte, dst []byte) (int, error) {
	panic("inflateZlibOneShot called in a build without gitpack_libdeflate")
}
