//go:build !arm64 || purego || (cgo && gitpack_libdeflate)

package objstore

func (d *goInflater) decodeHuffman(r *deflateBits, dst []byte, out int) (int, error) {
	return d.decodeHuffmanGo(r, dst, out)
}
