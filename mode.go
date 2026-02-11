// mode.go
//
// Git tree-entry file mode constants and classification helpers.
//
// Git stores a POSIX-like mode for every entry in a tree object. Only a
// small set of values is legal (Git rejects anything else during fsck).
// The constants below cover all modes that can appear in a well-formed
// repository. The helpers classify a raw mode into "tree" or "blob"
// categories used by the tree-walking and scanning layers.
//
// Reference: https://git-scm.com/docs/git-fast-import#_file_modes

package objstore

const (
	// modeTypeMask isolates the 4-bit file-type nibble from a 16-bit
	// POSIX mode. Applying (mode & modeTypeMask) strips permission bits
	// and yields one of the type constants below.
	modeTypeMask = 0o170000

	// modeTree (040000) marks a subdirectory entry, i.e. a reference to
	// another tree object in the object store.
	modeTree = 0o040000

	// modeBlob (100000) is the base type for regular file entries. The
	// low permission bits (e.g. 100644, 100755) are not relevant for
	// type classification and are stripped by modeTypeMask.
	modeBlob = 0o100000

	// modeSymlink (120000) marks a symbolic link. The blob content is
	// the link target path.
	modeSymlink = 0o120000

	// modeGitlink (160000) marks a gitlink (submodule) entry. The
	// object ID stored in the tree points to a commit in the submodule
	// repository, not an object in the current repository's store.
	modeGitlink = 0o160000
)

// isTreeMode reports whether mode represents a directory (tree) entry.
func isTreeMode(mode uint32) bool {
	return mode&modeTypeMask == modeTree
}

// isBlobMode reports whether mode represents file-like content that
// should be scanned for secrets or processed as blob data.
//
// Symlinks are intentionally treated as blobs here because their content
// (the link target path) is stored as a blob object in the pack/loose
// store and may contain sensitive information worth scanning. Gitlinks
// (submodule references) are excluded because their "content" is a
// commit hash in an external repository, not scannable data.
func isBlobMode(mode uint32) bool {
	switch mode & modeTypeMask {
	case modeBlob, modeSymlink:
		return true
	default:
		return false
	}
}
