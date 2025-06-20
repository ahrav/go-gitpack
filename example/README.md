# Go-GitPack Object Store Example

This example demonstrates how to use the `objstore` package to work with Git objects and pack files. It showcases the core functionality including `ParseHash`, creating pack files, and retrieving objects from the store.

## What This Example Does

1. **Demonstrates ParseHash**: Shows how to parse Git SHA-1 hashes from strings, including various edge cases and error handling
2. **Creates Example Objects**: Generates realistic Git objects (blobs, trees, commits) with proper Git formatting
3. **Builds Pack Files**: Creates Git pack and index files from scratch to demonstrate the format
4. **Object Retrieval**: Shows how to open a store and retrieve objects by hash

## Running the Example

```bash
cd example
go run main.go
```

## Key Functions Demonstrated

### ParseHash Usage

The `ParseHash` function converts 40-character hexadecimal strings into `Hash` objects:

```go
// Valid usage
hash, err := objstore.ParseHash("d670460b4b4aece5915caf5c68d12f560a9fe3e4")
if err != nil {
    log.Fatal(err)
}

// The hash can be used for object lookups
data, objType, err := store.Get(hash)
```

#### Valid Hash Examples
- `d670460b4b4aece5915caf5c68d12f560a9fe3e4` - Typical Git commit hash
- `89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0` - Typical Git blob hash
- `0000000000000000000000000000000000000000` - Null hash (valid but never resolves)
- `ffffffffffffffffffffffffffffffffffffffff` - All F's (valid edge case)

#### Invalid Hash Examples
- `abcdef123456` - Too short (must be exactly 40 characters)
- `abcdef1234567890abcdef1234567890abcdef123456` - Too long
- `ghijkl1234567890abcdef1234567890abcdef12` - Contains non-hex characters

### Store Operations

```go
// Open a store from a pack directory
store, err := objstore.Open("/path/to/.git/objects/pack")
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Configure the store
store.SetMaxDeltaDepth(50)  // Maximum delta chain depth
store.VerifyCRC = true      // Enable CRC verification

// Retrieve an object
data, objType, err := store.Get(hash)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Object type: %s\n", objType.String())
fmt.Printf("Content: %s\n", string(data))
```

## Git Object Types

The example creates and demonstrates all major Git object types:

### Blob Objects
File content stored in Git. The example creates:
- `README.md` content
- `config.json` configuration file
- `main.go` source code

### Tree Objects
Directory listings that reference other objects by hash. Format:
```
<mode> <name>\0<20-byte-hash>
```

### Commit Objects
Commit metadata and references. Format:
```
tree <tree-hash>
author <name> <email> <timestamp> <timezone>
committer <name> <email> <timestamp> <timezone>

<commit message>
```

## Pack File Format

The example demonstrates creating Git pack files with the proper format:

1. **Pack Header**: `PACK` signature + version + object count
2. **Objects**: Variable-length headers + compressed content
3. **Index File**: Hash lookup table with fanout optimization

## Hash Calculation

Git objects are hashed using SHA-1 with this format:
```
sha1("<type> <size>\0<content>")
```

For example, a blob containing "Hello World" would be hashed as:
```
sha1("blob 11\0Hello World")
```

## Error Handling

The example demonstrates proper error handling for:
- Invalid hash formats
- Missing objects
- Store configuration errors
- Pack file corruption

## Performance Considerations

- The store uses memory-mapped files for efficiency
- An LRU cache avoids redundant decompression
- Delta chains are resolved with cycle detection
- CRC verification can be enabled for integrity checks

## Real-World Usage

This object store is designed for scenarios where you need:
- Fast, read-only access to Git objects
- Direct pack file access without shelling out to Git
- Low-latency object retrieval for indexing or search
- Working with large repositories where full checkout is unnecessary

## Testing Your Own Data

To test with real Git repositories:

1. Find pack files in your repository:
   ```bash
   ls .git/objects/pack/
   ```

2. Use the store to read them:
   ```go
   store, err := objstore.Open(".git/objects/pack")
   // ... use store.Get() with real hashes from git log --oneline
   ```

3. Get object hashes from Git:
   ```bash
   git log --oneline --format="%H"  # Commit hashes
   git ls-tree HEAD                 # Tree and blob hashes
   ```
