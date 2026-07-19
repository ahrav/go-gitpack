//go:build !arm64 || purego || gitpack_libdeflate

package objstore

func (d *goInflater) decodeHuffman(r *deflateBits, dst []byte, out int) (int, error) {
	return d.decodeHuffmanGo(r, dst, out)
}
