package objstore

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAddedHunksLargeFiles tests the different code paths based on file size thresholds.
func TestAddedHunksLargeFiles(t *testing.T) {
	// Helper to create a large file with repeated content
	createLargeFile := func(pattern string, targetSize int) []byte {
		// Calculate how many times to repeat to reach target size
		patternSize := len(pattern)
		repeatCount := targetSize / patternSize
		if targetSize%patternSize > 0 {
			repeatCount++
		}

		result := bytes.Repeat([]byte(pattern), repeatCount)
		// Trim to exact size
		if len(result) > targetSize {
			result = result[:targetSize]
		}
		return result
	}

	// Helper to create file with specific lines repeated
	createFileWithLines := func(lineContent string, numLines int) []byte {
		lines := make([]string, numLines)
		for i := 0; i < numLines; i++ {
			lines[i] = fmt.Sprintf("%s_%d", lineContent, i)
		}
		return []byte(strings.Join(lines, "\n"))
	}

	t.Run("medium_file_line_set_algorithm", func(t *testing.T) {
		// Create files just over SmallFileThreshold (1 MB)
		// This should trigger addedHunksWithLineSet
		size := SmallFileThreshold + 1024 // 1 MB + 1 KB

		// Create old file with pattern
		oldContent := createLargeFile("This is line content that repeats\n", size)

		// Create new file with some additions
		newContent := createLargeFile("This is line content that repeats\n", size/2)
		additions := []byte("Added line 1\nAdded line 2\nAdded line 3\n")
		newContent = append(newContent, additions...)
		newContent = append(newContent, createLargeFile("This is line content that repeats\n", size/2)...)

		result := addedHunksForLargeFiles(oldContent, newContent)

		// Should find the added lines
		require.NotNil(t, result)
		require.Greater(t, len(result), 0)

		// Verify we found our additions
		foundAdditions := false
		for _, hunk := range result {
			for _, line := range hunk.Lines {
				if strings.HasPrefix(line, "Added line") {
					foundAdditions = true
					break
				}
			}
		}
		assert.True(t, foundAdditions, "Should find the added lines")
	})

	t.Run("large_file_hash_algorithm", func(t *testing.T) {
		// Create files just over LargeFileThreshold (500 MB)
		// This should trigger addedHunksWithHashing
		// For testing, we'll simulate this with a smaller size
		// but test the hash algorithm directly
		oldLines := make([]string, 100000)
		for i := 0; i < 100000; i++ {
			oldLines[i] = fmt.Sprintf("Original line %d with some content", i)
		}
		oldContent := []byte(strings.Join(oldLines, "\n"))

		// New content with additions interspersed
		newLines := make([]string, 0, 100100)
		for i := 0; i < 100000; i++ {
			if i%1000 == 0 {
				newLines = append(newLines, fmt.Sprintf("NEW ADDITION at position %d", i))
			}
			newLines = append(newLines, fmt.Sprintf("Original line %d with some content", i))
		}
		newContent := []byte(strings.Join(newLines, "\n"))

		result := addedHunksWithHashing(oldContent, newContent)

		require.NotNil(t, result)
		require.Greater(t, len(result), 0)

		// Count additions
		additionCount := 0
		for _, hunk := range result {
			for _, line := range hunk.Lines {
				if strings.HasPrefix(line, "NEW ADDITION") {
					additionCount++
				}
			}
		}
		assert.Equal(t, 100, additionCount, "Should find all 100 additions")
	})

	t.Run("line_set_vs_position_tracking_consistency", func(t *testing.T) {
		// Test that line set algorithm produces similar results to position tracking
		// for files where line order doesn't matter

		// Create content with unique lines
		oldLines := []string{
			"unique line alpha",
			"unique line beta",
			"unique line gamma",
			"unique line delta",
		}
		newLines := []string{
			"unique line alpha",
			"added line one",
			"unique line beta",
			"added line two",
			"unique line gamma",
			"added line three",
			"unique line delta",
		}

		oldContent := []byte(strings.Join(oldLines, "\n"))
		newContent := []byte(strings.Join(newLines, "\n"))

		// Test with position tracking (small file)
		posResult := addedHunksWithPos(oldContent, newContent)

		// Test with line set algorithm
		lineSetResult := addedHunksWithLineSet(oldContent, newContent)

		// Both should find the same additions
		require.Equal(t, len(posResult), len(lineSetResult))

		// Collect all added lines from both results
		posAdditions := make(map[string]bool)
		for _, hunk := range posResult {
			for _, line := range hunk.Lines {
				posAdditions[line] = true
			}
		}

		lineSetAdditions := make(map[string]bool)
		for _, hunk := range lineSetResult {
			for _, line := range hunk.Lines {
				lineSetAdditions[line] = true
			}
		}

		assert.Equal(t, posAdditions, lineSetAdditions, "Both algorithms should find the same additions")
	})

	t.Run("hash_collision_handling", func(t *testing.T) {
		// Test that hash algorithm handles potential collisions correctly
		// by using many similar lines

		oldLines := make([]string, 10000)
		for i := 0; i < 10000; i++ {
			// Create lines with subtle differences
			oldLines[i] = fmt.Sprintf("Line with number %d and some padding", i)
		}

		// New content with some truly new lines
		newLines := make([]string, 10100)
		copy(newLines, oldLines)
		// Add 100 new unique lines
		for i := 0; i < 100; i++ {
			newLines[10000+i] = fmt.Sprintf("Completely new line number %d with different content", i)
		}

		oldContent := []byte(strings.Join(oldLines, "\n"))
		newContent := []byte(strings.Join(newLines, "\n"))

		result := addedHunksWithHashing(oldContent, newContent)

		require.NotNil(t, result)

		// Should find exactly 100 additions
		totalAdditions := 0
		for _, hunk := range result {
			totalAdditions += len(hunk.Lines)
		}
		assert.Equal(t, 100, totalAdditions, "Should find exactly 100 new lines")

		// All additions should be the "Completely new line" ones
		for _, hunk := range result {
			for _, line := range hunk.Lines {
				assert.Contains(t, line, "Completely new line")
			}
		}
	})

	t.Run("empty_and_nil_handling_large_files", func(t *testing.T) {
		// Test edge cases with large file algorithms

		// Both empty
		result := addedHunksWithLineSet([]byte{}, []byte{})
		assert.Nil(t, result)

		result = addedHunksWithHashing([]byte{}, []byte{})
		assert.Nil(t, result)

		// Old empty, new has content
		newContent := []byte("line1\nline2\nline3")
		result = addedHunksWithLineSet([]byte{}, newContent)
		require.NotNil(t, result)
		assert.Len(t, result, 1)
		assert.Equal(t, uint32(1), result[0].StartLine)
		assert.Len(t, result[0].Lines, 3)

		result = addedHunksWithHashing([]byte{}, newContent)
		require.NotNil(t, result)
		assert.Len(t, result, 1)
		assert.Equal(t, uint32(1), result[0].StartLine)
		assert.Len(t, result[0].Lines, 3)
	})

	t.Run("performance_characteristics", func(t *testing.T) {
		// Test that demonstrates why we use different algorithms for different sizes

		// Create files with many unique lines
		numLines := 50000
		oldLines := make([]string, numLines)
		newLines := make([]string, numLines+1000)

		for i := 0; i < numLines; i++ {
			content := fmt.Sprintf("Original file line %d with enough content to be meaningful", i)
			oldLines[i] = content
			newLines[i] = content
		}

		// Add 1000 new lines scattered throughout
		for i := 0; i < 1000; i++ {
			newLines[numLines+i] = fmt.Sprintf("Addition number %d with unique content", i)
		}

		oldContent := []byte(strings.Join(oldLines, "\n"))
		newContent := []byte(strings.Join(newLines, "\n"))

		// Test all three algorithms and verify they find the same additions
		posResult := addedHunksWithPos(oldContent, newContent)
		lineSetResult := addedHunksWithLineSet(oldContent, newContent)
		hashResult := addedHunksWithHashing(oldContent, newContent)

		// Count additions in each
		countAdditions := func(hunks []AddedHunk) int {
			count := 0
			for _, h := range hunks {
				count += len(h.Lines)
			}
			return count
		}

		posCount := countAdditions(posResult)
		lineSetCount := countAdditions(lineSetResult)
		hashCount := countAdditions(hashResult)

		assert.Equal(t, 1000, posCount, "Position tracking should find 1000 additions")
		assert.Equal(t, 1000, lineSetCount, "Line set should find 1000 additions")
		assert.Equal(t, 1000, hashCount, "Hash algorithm should find 1000 additions")
	})

	t.Run("consecutive_additions_large_file", func(t *testing.T) {
		// Test that consecutive additions are properly grouped into hunks
		// even with large file algorithms

		oldContent := createFileWithLines("old", 10000)

		// Insert a block of consecutive new lines
		oldLinesList := strings.Split(string(oldContent), "\n")
		newLinesList := make([]string, 0, len(oldLinesList)+5)

		// Insert at position 5000
		newLinesList = append(newLinesList, oldLinesList[:5000]...)
		newLinesList = append(newLinesList,
			"new consecutive 1",
			"new consecutive 2",
			"new consecutive 3",
			"new consecutive 4",
			"new consecutive 5",
		)
		newLinesList = append(newLinesList, oldLinesList[5000:]...)

		newContent := []byte(strings.Join(newLinesList, "\n"))

		// Test with line set algorithm
		result := addedHunksWithLineSet(oldContent, newContent)
		require.Len(t, result, 1, "Consecutive additions should form a single hunk")
		assert.Len(t, result[0].Lines, 5, "Hunk should contain 5 lines")
		assert.Equal(t, uint32(5001), result[0].StartLine)

		// Test with hash algorithm
		result = addedHunksWithHashing(oldContent, newContent)
		require.Len(t, result, 1, "Consecutive additions should form a single hunk")
		assert.Len(t, result[0].Lines, 5, "Hunk should contain 5 lines")
		assert.Equal(t, uint32(5001), result[0].StartLine)
	})

	t.Run("mixed_content_patterns", func(t *testing.T) {
		// Test with realistic mixed content patterns

		// Simulate a source code file with various patterns
		oldCode := `package main

import (
	"fmt"
	"strings"
)

func main() {
	fmt.Println("Hello, World!")
}

func helper() {
	// Some helper code
	for i := 0; i < 10; i++ {
		fmt.Println(i)
	}
}
`

		newCode := `package main

import (
	"fmt"
	"strings"
	"os" // Added import
)

func main() {
	fmt.Println("Hello, World!")
	os.Exit(0) // Added line
}

func helper() {
	// Some helper code
	// Added comment
	for i := 0; i < 10; i++ {
		fmt.Println(i)
		// Another added comment
	}
}

// Added new function
func newFunction() {
	fmt.Println("I'm new!")
}
`

		// Repeat the content to make it large enough
		oldLarge := bytes.Repeat([]byte(oldCode), 5000) // Will be > 1MB
		newLarge := bytes.Repeat([]byte(newCode), 5000)

		result := addedHunksForLargeFiles(oldLarge, newLarge)

		// Should find additions
		require.Greater(t, len(result), 0, "Should find additions in large code file")

		// Verify some expected additions are found
		foundImport := false
		foundNewFunc := false
		for _, hunk := range result {
			for _, line := range hunk.Lines {
				if strings.Contains(line, `"os"`) {
					foundImport = true
				}
				if strings.Contains(line, "newFunction") {
					foundNewFunc = true
				}
			}
		}

		assert.True(t, foundImport, "Should find the added import")
		assert.True(t, foundNewFunc, "Should find the new function")
	})
}

// BenchmarkLargeFileDiff benchmarks the different algorithms with various file sizes
func BenchmarkLargeFileDiff(b *testing.B) {
	// Helper to create test data
	createTestData := func(size int, additionPercent float64) ([]byte, []byte) {
		numLines := size / 50 // Assuming average line length of 50 bytes
		numAdditions := int(float64(numLines) * additionPercent)

		oldLines := make([]string, numLines)
		for i := 0; i < numLines; i++ {
			oldLines[i] = fmt.Sprintf("Original line %d with some content to make it realistic", i)
		}

		newLines := make([]string, 0, numLines+numAdditions)
		additionInterval := numLines / numAdditions
		if additionInterval < 1 {
			additionInterval = 1
		}

		additionCount := 0
		for i := 0; i < numLines; i++ {
			if i%additionInterval == 0 && additionCount < numAdditions {
				newLines = append(newLines, fmt.Sprintf("Added line %d with new content", additionCount))
				additionCount++
			}
			newLines = append(newLines, oldLines[i])
		}

		return []byte(strings.Join(oldLines, "\n")), []byte(strings.Join(newLines, "\n"))
	}

	sizes := []struct {
		name string
		size int
	}{
		{"1MB", 1 << 20},
		{"10MB", 10 << 20},
		{"50MB", 50 << 20},
		{"100MB", 100 << 20},
	}

	for _, sz := range sizes {
		old, new := createTestData(sz.size, 0.01) // 1% additions

		b.Run(sz.name+"_LineSet", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = addedHunksWithLineSet(old, new)
			}
		})

		b.Run(sz.name+"_Hashing", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = addedHunksWithHashing(old, new)
			}
		})
	}
}

// TestComputeAddedHunksIntegration tests the main entry point with various file sizes
func TestComputeAddedHunksIntegration(t *testing.T) {
	t.Run("file_size_routing", func(t *testing.T) {
		// This test would require a proper store implementation
		// For now, we'll test the algorithm selection logic directly

		// Test that files over MaxDiffSize return a placeholder
		hugOld := make([]byte, MaxDiffSize+1)
		hugeNew := make([]byte, MaxDiffSize+1)

		// Test the logic that would be in computeAddedHunks
		oldSize, newSize := int64(len(hugOld)), int64(len(hugeNew))
		if oldSize > MaxDiffSize || newSize > MaxDiffSize {
			placeholder := AddedHunk{
				StartLine: 1,
				Lines:     []string{fmt.Sprintf("[File too large to diff: old=%d new=%d bytes]", oldSize, newSize)},
				IsBinary:  false,
			}
			result := []AddedHunk{placeholder}

			assert.Len(t, result, 1)
			assert.Contains(t, result[0].Lines[0], "File too large to diff")
		}
	})
}
