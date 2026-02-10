package objstore

const (
	modeTypeMask = 0o170000
	modeTree     = 0o040000
	modeBlob     = 0o100000
	modeSymlink  = 0o120000
	modeGitlink  = 0o160000
)

func isTreeMode(mode uint32) bool {
	return mode&modeTypeMask == modeTree
}

func isBlobMode(mode uint32) bool {
	switch mode & modeTypeMask {
	case modeBlob, modeSymlink:
		return true
	default:
		return false
	}
}
