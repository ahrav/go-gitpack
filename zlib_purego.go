//go:build !cgo || !gitpack_libdeflate

package objstore

// libdeflateAvailable reports that this build uses the pure-Go flate backend.
const libdeflateAvailable = false

// inflateZlibOneShot uses the direct-to-destination one-shot Go decoder.
func inflateZlibOneShot(src []byte, dst []byte) (int, error) {
	return inflatePackZlibGo(src, dst)
}
