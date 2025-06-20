package main

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	objstore "github.com/ahrav/go-gitpack"
)

func main() {
	fmt.Println("=== Git Object Store Example ===")
	fmt.Println()

	// Demonstrate ParseHash with various hash strings.
	demonstrateParseHash()
	fmt.Println()

	// Create a temporary directory for our example pack files.
	tempDir, err := os.MkdirTemp("", "objstore-example-")
	if err != nil {
		log.Fatal("Failed to create temp dir:", err)
	}
	defer os.RemoveAll(tempDir)

	packDir := filepath.Join(tempDir, "objects", "pack")
	if err := os.MkdirAll(packDir, 0755); err != nil {
		log.Fatal("Failed to create pack dir:", err)
	}

	// Create example git objects and pack them.
	objects := createExampleObjects()
	packPath, idxPath := createExamplePack(packDir, objects)

	fmt.Printf("Created example pack files:\n")
	fmt.Printf("  Pack: %s\n", packPath)
	fmt.Printf("  Index: %s\n", idxPath)
	fmt.Println()

	// Demonstrate Store usage.
	demonstrateStore(packDir, objects)
}

// demonstrateParseHash shows how to use ParseHash with different inputs.
func demonstrateParseHash() {
	fmt.Println("--- ParseHash Examples ---")

	examples := []struct {
		name   string
		input  string
		valid  bool
		reason string
	}{
		{
			name:  "Valid Git commit hash",
			input: "d670460b4b4aece5915caf5c68d12f560a9fe3e4",
			valid: true,
		},
		{
			name:  "Valid Git blob hash",
			input: "89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0",
			valid: true,
		},
		{
			name:  "Valid hash with lowercase hex",
			input: "abcdef1234567890abcdef1234567890abcdef12",
			valid: true,
		},
		{
			name:   "Invalid - too short",
			input:  "abcdef123456",
			valid:  false,
			reason: "Hash must be exactly 40 characters",
		},
		{
			name:   "Invalid - too long",
			input:  "abcdef1234567890abcdef1234567890abcdef123456",
			valid:  false,
			reason: "Hash must be exactly 40 characters",
		},
		{
			name:   "Invalid - contains non-hex characters",
			input:  "ghijkl1234567890abcdef1234567890abcdef12",
			valid:  false,
			reason: "Hash must contain only hexadecimal characters (0-9, a-f)",
		},
		{
			name:  "Edge case - all zeros (null hash)",
			input: "0000000000000000000000000000000000000000",
			valid: true,
		},
		{
			name:  "Edge case - all F's",
			input: "ffffffffffffffffffffffffffffffffffffffff",
			valid: true,
		},
	}

	for _, example := range examples {
		fmt.Printf("Testing: %s\n", example.name)
		fmt.Printf("  Input: %s\n", example.input)

		hash, err := objstore.ParseHash(example.input)
		if example.valid {
			if err != nil {
				fmt.Printf("  ❌ Expected success but got error: %v\n", err)
			} else {
				fmt.Printf("  ✅ Successfully parsed\n")
				fmt.Printf("  Hash bytes: %x\n", hash[:])
				fmt.Printf("  Uint64 representation: %d\n", hash.Uint64())

				// Demonstrate round-trip conversion.
				hexString := hex.EncodeToString(hash[:])
				fmt.Printf("  Round-trip: %s -> %s\n", example.input, hexString)
			}
		} else {
			if err != nil {
				fmt.Printf("  ✅ Correctly rejected: %v\n", err)
				if example.reason != "" {
					fmt.Printf("  Reason: %s\n", example.reason)
				}
			} else {
				fmt.Printf("  ❌ Expected error but parsing succeeded\n")
			}
		}
		fmt.Println()
	}
}

// ExampleObject represents a Git object with its content and metadata.
type ExampleObject struct {
	Hash    objstore.Hash
	Type    objstore.ObjectType
	Content []byte
	Name    string // Human-readable name for the example
}

// createExampleObjects creates a set of realistic Git objects for demonstration.
func createExampleObjects() []ExampleObject {
	objects := make([]ExampleObject, 0)

	// Example 1: A simple blob (file content).
	readmeContent := []byte(`# Example Repository

This is an example repository demonstrating the go-gitpack objstore.

## Files

- README.md (this file)
- main.go (example application)
- config.json (configuration file)
`)
	readmeHash := calculateGitHash(objstore.ObjBlob, readmeContent)
	objects = append(objects, ExampleObject{
		Hash:    readmeHash,
		Type:    objstore.ObjBlob,
		Content: readmeContent,
		Name:    "README.md blob",
	})

	// Example 2: A configuration file blob.
	configContent := []byte(`{
  "name": "objstore-example",
  "version": "1.0.0",
  "settings": {
    "cache_size": 16384,
    "max_delta_depth": 50,
    "verify_crc": false
  }
}`)
	configHash := calculateGitHash(objstore.ObjBlob, configContent)
	objects = append(objects, ExampleObject{
		Hash:    configHash,
		Type:    objstore.ObjBlob,
		Content: configContent,
		Name:    "config.json blob",
	})

	// Example 3: A source code blob.
	codeContent := []byte(`package main

import "fmt"

func main() {
    fmt.Println("Hello from objstore!")
}
`)
	codeHash := calculateGitHash(objstore.ObjBlob, codeContent)
	objects = append(objects, ExampleObject{
		Hash:    codeHash,
		Type:    objstore.ObjBlob,
		Content: codeContent,
		Name:    "main.go blob",
	})

	// Example 4: A tree object (directory listing).
	treeContent := createTreeContent([]treeEntry{
		{mode: "100644", name: "README.md", hash: readmeHash},
		{mode: "100644", name: "config.json", hash: configHash},
		{mode: "100644", name: "main.go", hash: codeHash},
	})
	treeHash := calculateGitHash(objstore.ObjTree, treeContent)
	objects = append(objects, ExampleObject{
		Hash:    treeHash,
		Type:    objstore.ObjTree,
		Content: treeContent,
		Name:    "root tree",
	})

	// Example 5: A commit object.
	commitContent := createCommitContent(
		treeHash,
		"Initial commit",
		"Example Author <author@example.com>",
		"Example Committer <committer@example.com>",
		1640995200, // 2022-01-01 00:00:00 UTC
	)
	commitHash := calculateGitHash(objstore.ObjCommit, commitContent)
	objects = append(objects, ExampleObject{
		Hash:    commitHash,
		Type:    objstore.ObjCommit,
		Content: commitContent,
		Name:    "initial commit",
	})

	return objects
}

type treeEntry struct {
	mode string
	name string
	hash objstore.Hash
}

func createTreeContent(entries []treeEntry) []byte {
	var buf bytes.Buffer
	for _, entry := range entries {
		fmt.Fprintf(&buf, "%s %s\x00", entry.mode, entry.name)
		buf.Write(entry.hash[:])
	}
	return buf.Bytes()
}

func createCommitContent(tree objstore.Hash, message, author, committer string, timestamp int64) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "tree %x\n", tree[:])
	fmt.Fprintf(&buf, "author %s %d +0000\n", author, timestamp)
	fmt.Fprintf(&buf, "committer %s %d +0000\n", committer, timestamp)
	fmt.Fprintf(&buf, "\n%s\n", message)
	return buf.Bytes()
}

func calculateGitHash(objType objstore.ObjectType, content []byte) objstore.Hash {
	h := sha1.New()
	header := fmt.Sprintf("%s %d\x00", objType.String(), len(content))
	h.Write([]byte(header))
	h.Write(content)

	var hash objstore.Hash
	copy(hash[:], h.Sum(nil))
	return hash
}

func createExamplePack(packDir string, objects []ExampleObject) (packPath, idxPath string) {
	packPath = filepath.Join(packDir, "example.pack")
	idxPath = filepath.Join(packDir, "example.idx")

	packFile, err := os.Create(packPath)
	if err != nil {
		log.Fatal("Failed to create pack file:", err)
	}
	defer packFile.Close()

	// Write pack header.
	packFile.Write([]byte("PACK"))                                 // signature
	binary.Write(packFile, binary.BigEndian, uint32(2))            // version
	binary.Write(packFile, binary.BigEndian, uint32(len(objects))) // object count

	// Track offsets for index.
	offsets := make([]uint64, len(objects))
	hashes := make([]objstore.Hash, len(objects))

	for i, obj := range objects {
		// Record current position as object offset.
		pos, _ := packFile.Seek(0, 1) // Get current position
		offsets[i] = uint64(pos)
		hashes[i] = obj.Hash

		writeObjectHeader(packFile, obj.Type, len(obj.Content))

		var compressed bytes.Buffer
		zw := zlib.NewWriter(&compressed)
		zw.Write(obj.Content)
		zw.Close()
		packFile.Write(compressed.Bytes())
	}

	createIndexFile(idxPath, hashes, offsets)

	return packPath, idxPath
}

func writeObjectHeader(w *os.File, objType objstore.ObjectType, size int) {
	// Simple case: size fits in 4 bits.
	if size < 16 {
		header := byte((byte(objType) << 4) | byte(size))
		w.Write([]byte{header})
		return
	}

	// Variable-length encoding for larger sizes.
	header := byte((byte(objType) << 4) | 0x0f | 0x80) // Set continuation bit.
	w.Write([]byte{header})

	// Write remaining size bytes.
	remaining := size - 15
	for remaining > 0 {
		b := byte(remaining & 0x7f)
		remaining >>= 7
		if remaining > 0 {
			b |= 0x80 // Set continuation bit.
		}
		w.Write([]byte{b})
	}
}

func createIndexFile(path string, hashes []objstore.Hash, offsets []uint64) {
	file, err := os.Create(path)
	if err != nil {
		log.Fatal("Failed to create index file:", err)
	}
	defer file.Close()

	// Write header.
	file.Write([]byte{0xff, 0x74, 0x4f, 0x63})      // Magic.
	binary.Write(file, binary.BigEndian, uint32(2)) // Version.

	// Sort objects by hash for proper index format.
	sortedData := make([]struct {
		hash   objstore.Hash
		offset uint64
	}, len(hashes))

	for i := range hashes {
		sortedData[i].hash = hashes[i]
		sortedData[i].offset = offsets[i]
	}

	// Simple bubble sort (fine for small examples).
	for i := 0; i < len(sortedData); i++ {
		for j := i + 1; j < len(sortedData); j++ {
			if bytes.Compare(sortedData[i].hash[:], sortedData[j].hash[:]) > 0 {
				sortedData[i], sortedData[j] = sortedData[j], sortedData[i]
			}
		}
	}

	// Build fanout table.
	fanout := make([]uint32, 256)
	for i, data := range sortedData {
		firstByte := data.hash[0]
		for j := int(firstByte); j < 256; j++ {
			fanout[j] = uint32(i + 1)
		}
	}

	// Write fanout table.
	for _, count := range fanout {
		binary.Write(file, binary.BigEndian, count)
	}

	// Write hashes.
	for _, data := range sortedData {
		file.Write(data.hash[:])
	}

	// Write CRC32s (using dummy values for this example).
	for range sortedData {
		binary.Write(file, binary.BigEndian, uint32(0x12345678))
	}

	// Write offsets.
	for _, data := range sortedData {
		binary.Write(file, binary.BigEndian, uint32(data.offset))
	}

	// Write checksums (dummy values).
	file.Write(make([]byte, 40)) // Pack + index checksums.
}

func demonstrateStore(packDir string, objects []ExampleObject) {
	fmt.Println("--- Store Operations ---")

	// Open the store.
	store, err := objstore.Open(packDir)
	if err != nil {
		log.Fatal("Failed to open store:", err)
	}
	defer store.Close()

	fmt.Printf("Opened store successfully\n")
	fmt.Println()

	// Demonstrate getting objects by hash.
	for _, obj := range objects {
		fmt.Printf("Retrieving %s:\n", obj.Name)
		fmt.Printf("  Hash: %x\n", obj.Hash[:])

		data, objType, err := store.Get(obj.Hash)
		if err != nil {
			fmt.Printf("  ❌ Error: %v\n", err)
			continue
		}

		fmt.Printf("  ✅ Successfully retrieved\n")
		fmt.Printf("  Type: %s\n", objType.String())
		fmt.Printf("  Size: %d bytes\n", len(data))

		// Verify content matches.
		if bytes.Equal(data, obj.Content) {
			fmt.Printf("  ✅ Content matches expected\n")
		} else {
			fmt.Printf("  ❌ Content mismatch!\n")
		}

		// Show a preview of the content (first 100 chars).
		preview := string(data)
		if len(preview) > 100 {
			preview = preview[:100] + "..."
		}
		fmt.Printf("  Preview: %q\n", preview)
		fmt.Println()
	}

	// Demonstrate error handling with non-existent hash.
	fmt.Println("Testing non-existent object:")
	nonExistentHash, _ := objstore.ParseHash("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	_, _, err = store.Get(nonExistentHash)
	if err != nil {
		fmt.Printf("  ✅ Correctly handled missing object: %v\n", err)
	} else {
		fmt.Printf("  ❌ Expected error for non-existent object\n")
	}
	fmt.Println()

	// Demonstrate store configuration.
	fmt.Println("Store configuration:")
	store.SetMaxDeltaDepth(25)
	fmt.Printf("  Max delta depth set to: 25\n")
	fmt.Printf("  CRC verification: %t\n", store.VerifyCRC)
}
