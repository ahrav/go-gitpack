package main

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	objstore "github.com/ahrav/go-gitpack"
)

func TestParseHashExamples(t *testing.T) {
	validHashes := []string{
		"d670460b4b4aece5915caf5c68d12f560a9fe3e4",
		"89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0",
		"abcdef1234567890abcdef1234567890abcdef12",
		"0000000000000000000000000000000000000000",
		"ffffffffffffffffffffffffffffffffffffffff",
	}

	for _, hashStr := range validHashes {
		t.Run("valid_"+hashStr[:8], func(t *testing.T) {
			hash, err := objstore.ParseHash(hashStr)
			if err != nil {
				t.Errorf("Expected valid hash %s to parse successfully, got error: %v", hashStr, err)
			}

			roundTrip := hex.EncodeToString(hash[:])
			if roundTrip != hashStr {
				t.Errorf("Round-trip failed: %s -> %s", hashStr, roundTrip)
			}
		})
	}

	invalidHashes := []struct {
		hash   string
		reason string
	}{
		{"abcdef123456", "too short"},
		{"abcdef1234567890abcdef1234567890abcdef123456", "too long"},
		{"ghijkl1234567890abcdef1234567890abcdef12", "non-hex characters"},
	}

	for _, test := range invalidHashes {
		t.Run("invalid_"+test.reason, func(t *testing.T) {
			_, err := objstore.ParseHash(test.hash)
			if err == nil {
				t.Errorf("Expected hash %s to be invalid (%s), but it parsed successfully", test.hash, test.reason)
			}
		})
	}
}

func TestCreateExampleObjects(t *testing.T) {
	objects := createExampleObjects()

	if len(objects) == 0 {
		t.Fatal("CreateExampleObjects should return at least one object.")
	}

	for _, obj := range objects {
		if obj.Hash == (objstore.Hash{}) {
			t.Errorf("Object %s has zero hash.", obj.Name)
		}

		if len(obj.Content) == 0 {
			t.Errorf("Object %s has empty content.", obj.Name)
		}

		expectedHash := calculateGitHash(obj.Type, obj.Content)
		if obj.Hash != expectedHash {
			t.Errorf("Object %s has mismatched hash: got %x, expected %x",
				obj.Name, obj.Hash[:], expectedHash[:])
		}
	}

	hasBlob := false
	hasTree := false
	hasCommit := false

	for _, obj := range objects {
		switch obj.Type {
		case objstore.ObjBlob:
			hasBlob = true
		case objstore.ObjTree:
			hasTree = true
		case objstore.ObjCommit:
			hasCommit = true
		}
	}

	if !hasBlob {
		t.Error("Example objects should include at least one blob.")
	}
	if !hasTree {
		t.Error("Example objects should include at least one tree.")
	}
	if !hasCommit {
		t.Error("Example objects should include at least one commit.")
	}
}

func TestPackCreationAndRetrieval(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "objstore-test-")
	if err != nil {
		t.Fatal("Failed to create temp dir:", err)
	}
	defer os.RemoveAll(tempDir)

	packDir := filepath.Join(tempDir, "objects", "pack")
	if err := os.MkdirAll(packDir, 0755); err != nil {
		t.Fatal("Failed to create pack dir:", err)
	}

	objects := createExampleObjects()
	packPath, idxPath := createExamplePack(packDir, objects)

	if _, err := os.Stat(packPath); os.IsNotExist(err) {
		t.Fatal("Pack file was not created.")
	}
	if _, err := os.Stat(idxPath); os.IsNotExist(err) {
		t.Fatal("Index file was not created.")
	}

	store, err := objstore.Open(packDir)
	if err != nil {
		t.Fatal("Failed to open store:", err)
	}
	defer store.Close()

	for _, expectedObj := range objects {
		t.Run("retrieve_"+expectedObj.Name, func(t *testing.T) {
			data, objType, err := store.Get(expectedObj.Hash)
			if err != nil {
				t.Fatalf("Failed to retrieve object %s: %v", expectedObj.Name, err)
			}

			if objType != expectedObj.Type {
				t.Errorf("Object %s: expected type %s, got %s",
					expectedObj.Name, expectedObj.Type.String(), objType.String())
			}

			if len(data) != len(expectedObj.Content) {
				t.Errorf("Object %s: expected size %d, got %d",
					expectedObj.Name, len(expectedObj.Content), len(data))
			}
		})
	}

	nonExistentHash, _ := objstore.ParseHash("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	_, _, err = store.Get(nonExistentHash)
	if err == nil {
		t.Error("Expected error when retrieving non-existent object.")
	}
}

func TestStoreConfiguration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "objstore-test-")
	if err != nil {
		t.Fatal("Failed to create temp dir:", err)
	}
	defer os.RemoveAll(tempDir)

	packDir := filepath.Join(tempDir, "objects", "pack")
	if err := os.MkdirAll(packDir, 0755); err != nil {
		t.Fatal("Failed to create pack dir:", err)
	}

	objects := createExampleObjects()[:1]
	createExamplePack(packDir, objects)

	store, err := objstore.Open(packDir)
	if err != nil {
		t.Fatal("Failed to open store:", err)
	}
	defer store.Close()

	store.SetMaxDeltaDepth(25)

	store.VerifyCRC = true
	if !store.VerifyCRC {
		t.Error("VerifyCRC should be settable to true.")
	}

	store.VerifyCRC = false
	if store.VerifyCRC {
		t.Error("VerifyCRC should be settable to false.")
	}
}
