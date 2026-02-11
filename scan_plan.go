// scan_plan.go implements the blob-scanning pipeline for HistoryScanner.
//
// The pipeline operates in three phases:
//
//  1. Candidate collection -- walkBlobCandidates traverses every commit (via
//     walkCommitsFromRefs) and emits one blobRecord per changed blob. Candidates
//     are partitioned into 256 on-disk buckets keyed by the first byte of the
//     blob OID. Bucketing limits the working-set size during the subsequent
//     dedup pass.
//
//  2. Dedup / classification -- the prepare step reads each bucket sequentially,
//     deduplicates by blob OID (keeping the first occurrence), consults the
//     caller-supplied SeenSet for cross-run dedup, and classifies each surviving
//     blob as either "packed" (locatable in an open packfile via the index) or
//     "loose" (must be resolved through the loose-object directory). Packed
//     records include the packfile offset so that the execution phase can sort
//     by offset for sequential I/O.
//
//  3. Execution -- packed records are sorted by ascending packfile offset and
//     decoded in batches to maximise sequential read throughput on spinning
//     disks and to benefit from OS read-ahead on SSDs. Loose objects are read
//     individually via store.getNoCache.
//
// Two execution strategies are supported:
//
//   - Fast path (in-memory): When the total candidate footprint fits within
//     scanFastPathMaxBytes and the blob count is below scanFastPathMaxBlobs,
//     all records are held in memory (inMemoryScanPlan) and no temporary files
//     are created. This eliminates disk I/O overhead for small-to-medium
//     repositories.
//
//   - Spill path (disk-backed): When the fast-path thresholds are exceeded,
//     candidates are spilled to temporary files via spillWriter. The external
//     sort uses fixed-size chunks (scanPackSortChunkSize) that are individually
//     sorted and then merged with a min-heap (packedMergeHeap) to produce a
//     globally offset-ordered stream without requiring the entire dataset to
//     fit in memory.
//
// The streaming executor (streamingPackExecutor) provides a third mode that
// avoids materializing the full plan by flushing per-pack buffers as they
// reach scanPackFlushRecords or scanPackFlushBytes, trading some I/O ordering
// for lower peak memory.
package objstore

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"golang.org/x/exp/mmap"
)

// bytesReaderPool reuses bytes.Reader instances to avoid per-blob allocations.
var bytesReaderPool = sync.Pool{
	New: func() any { return new(bytes.Reader) },
}

// getBytesReader returns a pooled bytes.Reader reset to the given data.
func getBytesReader(data []byte) *bytes.Reader {
	r := bytesReaderPool.Get().(*bytes.Reader)
	r.Reset(data)
	return r
}

// putBytesReader returns a bytes.Reader to the pool.
func putBytesReader(r *bytes.Reader) {
	r.Reset(nil) // release reference to data
	bytesReaderPool.Put(r)
}

// SeenSet tracks globally scanned blobs across multiple scan runs.
//
// An implementation MUST be safe for concurrent use by multiple goroutines
// because the scan pipeline may call Has and Put from parallel decode workers.
//
// Contract:
//   - Has returns (true, nil) if the blob OID has been recorded by a prior
//     Put call in this or any previous scan run.
//   - Put records the blob OID so that future Has calls return true.
//   - Errors from Has or Put abort the current scan immediately.
//
// Typical implementations back SeenSet with a persistent store (e.g. a
// Bloom filter backed by disk) so that incremental scans can skip blobs
// processed in earlier invocations.
type SeenSet interface {
	Has(oid Hash) (bool, error)
	Put(oid Hash) error
}

// ScanJob is one unit of blob-centric scan work, representing a single blob
// that needs to be decoded and passed to a BlobScanner.
type ScanJob struct {
	// Blob is the object ID of the blob to scan.
	Blob Hash

	// Commit is the object ID of the commit that introduced or modified
	// this blob. Used for attribution in scan results.
	Commit Hash

	// Path is the repository-relative file path (forward-slash separated)
	// where this blob appears in the commit's tree.
	Path string

	// Pack points to the memory-mapped packfile that contains this blob.
	// Nil for loose objects (which are not represented as ScanJobs).
	Pack *mmap.ReaderAt

	// Offset is the byte offset within Pack where this blob's packfile
	// entry begins. Jobs are sorted by ascending Offset before execution
	// to achieve sequential I/O access patterns and benefit from OS
	// read-ahead, which is critical for performance on spinning disks.
	Offset uint64
}

// ScanMeta carries attribution and identity metadata that is passed alongside
// blob content to a BlobScanner. It identifies *which* blob is being scanned,
// *which* commit introduced it, and *where* in the tree it appeared.
type ScanMeta struct {
	// Blob is the object ID of the blob being scanned.
	Blob Hash

	// Commit is the object ID of the commit that introduced this blob change.
	Commit Hash

	// Path is the repository-relative file path (forward-slash separated).
	Path string
}

// BlobScanner consumes blob content plus metadata for each planned blob.
//
// Thread safety: ScanBlob may be called from multiple goroutines concurrently
// (one per decode worker). Implementations MUST synchronize any shared mutable
// state internally.
//
// Lifetime: the io.Reader passed to ScanBlob is only valid for the duration
// of the call. Callers must not retain the Reader or its underlying buffer
// after ScanBlob returns, because the buffer is recycled via a sync.Pool.
// If the implementation needs to keep blob data beyond the call, it must copy
// the bytes.
type BlobScanner interface {
	ScanBlob(r io.Reader, meta ScanMeta) error
}

const (
	// scanSpillBufSize (1 MiB) is the bufio buffer size used when writing or
	// reading spill files. 1 MiB balances syscall overhead reduction against
	// memory consumption when many bucket/pack writers are open concurrently.
	scanSpillBufSize = 1 << 20

	// scanPackSortChunkSize (32 MiB) caps the in-memory working set during
	// external sorting of packed blob records. Each chunk is sorted in-place
	// by packfile offset, written to a temporary file, and later merged via a
	// min-heap. 32 MiB keeps RSS bounded on machines with many large packs.
	scanPackSortChunkSize = 32 << 20

	// scanMaxPathBytes (1 MiB) is the maximum allowed length (in bytes) for a
	// single file path stored in a blob or packed-blob record. This guards
	// against corrupt or adversarial data causing unbounded allocations.
	scanMaxPathBytes = 1 << 20

	// scanFastPathMaxBytes (768 MiB) is the cumulative byte budget for the
	// in-memory fast path. If total candidate record bytes exceed this, the
	// scanner falls back to the disk-backed spill path.
	scanFastPathMaxBytes = 768 << 20

	// scanFastPathMaxBlobs (2 million) caps the number of unique blobs the
	// fast path will hold in memory. Together with scanFastPathMaxBytes, this
	// provides a dual guard: one for total memory, one for map overhead from
	// a very large number of small blobs.
	scanFastPathMaxBlobs = 2_000_000

	// scanPackFlushRecords (4096) triggers a per-pack buffer flush in the
	// streaming executor when the record count reaches this threshold.
	// Flushing in record-count increments bounds latency and ensures
	// forward progress regardless of individual record sizes.
	scanPackFlushRecords = 4096

	// scanPackFlushBytes (8 MiB) triggers a per-pack buffer flush in the
	// streaming executor when accumulated record bytes reach this threshold.
	// Together with scanPackFlushRecords, this provides a dual-trigger
	// mechanism so that a few very large paths do not cause unbounded growth.
	scanPackFlushBytes = 8 << 20

	// scanPackDecodeMinRecs (32) is the minimum number of records in a batch
	// before the decoder spawns multiple goroutines. Below this threshold the
	// overhead of goroutine creation and synchronisation outweighs
	// parallelism gains, so a single-threaded decode is used instead.
	scanPackDecodeMinRecs = 32

	// scanPackDecodeWorkers (24) is the hard upper bound on the number of
	// parallel decode goroutines regardless of GOMAXPROCS. This prevents
	// excessive contention on the mmap read path and keeps file-descriptor
	// pressure manageable.
	scanPackDecodeWorkers = 24

	// scanPackDecodeBatchN (2) is the multiplier applied to the worker count
	// to determine the decode batch size: batchSize = workers * N. The 2x
	// factor gives each worker roughly two records per round, amortising
	// goroutine scheduling overhead while keeping per-batch memory modest.
	scanPackDecodeBatchN = 2

	// blobRecordHeaderBytes is the fixed-size header for an on-disk blobRecord:
	// 20 bytes (blob OID) + 20 bytes (commit OID) + 4 bytes (path length, LE uint32).
	// The variable-length path follows immediately after the header.
	blobRecordHeaderBytes = 20 + 20 + 4

	// packedRecordHeaderBytes extends blobRecordHeaderBytes with an 8-byte
	// little-endian uint64 packfile offset prepended to the front. This offset
	// enables sorting records for sequential packfile I/O.
	packedRecordHeaderBytes = 8 + blobRecordHeaderBytes
)

// errScanPlanOverflow is returned by collectInMemoryScanPlan when the
// candidate set exceeds the fast-path thresholds (scanFastPathMaxBytes or
// scanFastPathMaxBlobs). The caller uses this sentinel to fall back to the
// disk-backed spill path.
var errScanPlanOverflow = errors.New("scan in-memory plan overflow")

// blobRecord is the in-memory and on-disk representation of a candidate blob
// before packfile location is resolved. The record is serialised as a
// fixed-size header (blobRecordHeaderBytes) followed by a variable-length
// path string. It is used during the candidate-collection phase and in the
// loose-object execution path.
type blobRecord struct {
	Blob   Hash
	Commit Hash
	Path   string
}

// packedBlobRecord extends blobRecord with the packfile byte offset where the
// blob's compressed data begins. Sorting a slice of packedBlobRecords by
// Offset yields sequential I/O order within a single packfile, which is the
// key optimisation for the execution phase.
type packedBlobRecord struct {
	Offset uint64
	Blob   Hash
	Commit Hash
	Path   string
}

// inMemoryScanPlan holds the complete set of deduplicated, classified blob
// records when the fast path is used (total footprint under
// scanFastPathMaxBytes and scanFastPathMaxBlobs). Packed records are grouped
// by pack index so they can be sorted by offset per-pack. Loose records are
// stored in encounter order since they are read individually.
type inMemoryScanPlan struct {
	// packedByID maps a pack index (position in store.packs) to the packed
	// blob records that reside in that pack.
	packedByID map[int][]packedBlobRecord

	// loose holds blob records that could not be located in any open
	// packfile and must be resolved through the loose-object directory.
	loose []blobRecord
}

// spillWriter wraps an os.File with a bufio.Writer for buffered writes to a
// temporary spill file. The bufio layer uses scanSpillBufSize to batch small
// writes into fewer syscalls. Close flushes the buffer before closing the
// underlying file. A nil receiver is safe to Close (no-op).
type spillWriter struct {
	file   *os.File
	writer *bufio.Writer
}

func openSpillWriter(path string) (*spillWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &spillWriter{
		file:   f,
		writer: bufio.NewWriterSize(f, scanSpillBufSize),
	}, nil
}

func (w *spillWriter) Close() error {
	if w == nil {
		return nil
	}
	var firstErr error
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	w.writer = nil
	w.file = nil
	return firstErr
}

func writeBlobRecord(w io.Writer, rec blobRecord) error {
	if len(rec.Path) > scanMaxPathBytes {
		return fmt.Errorf("scan path too long: %d bytes", len(rec.Path))
	}

	if _, err := w.Write(rec.Blob[:]); err != nil {
		return err
	}
	if _, err := w.Write(rec.Commit[:]); err != nil {
		return err
	}

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(rec.Path)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if len(rec.Path) == 0 {
		return nil
	}
	_, err := io.WriteString(w, rec.Path)
	return err
}

func readBlobRecord(r io.Reader) (blobRecord, error) {
	var hdr [blobRecordHeaderBytes]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return blobRecord{}, io.EOF
		}
		return blobRecord{}, err
	}

	var rec blobRecord
	copy(rec.Blob[:], hdr[0:20])
	copy(rec.Commit[:], hdr[20:40])
	pathLen := binary.LittleEndian.Uint32(hdr[40:44])
	if pathLen > scanMaxPathBytes {
		return blobRecord{}, fmt.Errorf("scan path exceeds max size: %d", pathLen)
	}
	if pathLen == 0 {
		return rec, nil
	}

	path := make([]byte, int(pathLen))
	if _, err := io.ReadFull(r, path); err != nil {
		return blobRecord{}, err
	}
	rec.Path = string(path)
	return rec, nil
}

func writePackedBlobRecord(w io.Writer, rec packedBlobRecord) error {
	if len(rec.Path) > scanMaxPathBytes {
		return fmt.Errorf("scan path too long: %d bytes", len(rec.Path))
	}

	var offBuf [8]byte
	binary.LittleEndian.PutUint64(offBuf[:], rec.Offset)
	if _, err := w.Write(offBuf[:]); err != nil {
		return err
	}
	if _, err := w.Write(rec.Blob[:]); err != nil {
		return err
	}
	if _, err := w.Write(rec.Commit[:]); err != nil {
		return err
	}

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(rec.Path)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if len(rec.Path) == 0 {
		return nil
	}
	_, err := io.WriteString(w, rec.Path)
	return err
}

func readPackedBlobRecord(r io.Reader) (packedBlobRecord, error) {
	var hdr [packedRecordHeaderBytes]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return packedBlobRecord{}, io.EOF
		}
		return packedBlobRecord{}, err
	}

	var rec packedBlobRecord
	rec.Offset = binary.LittleEndian.Uint64(hdr[0:8])
	copy(rec.Blob[:], hdr[8:28])
	copy(rec.Commit[:], hdr[28:48])
	pathLen := binary.LittleEndian.Uint32(hdr[48:52])
	if pathLen > scanMaxPathBytes {
		return packedBlobRecord{}, fmt.Errorf("scan path exceeds max size: %d", pathLen)
	}
	if pathLen == 0 {
		return rec, nil
	}

	path := make([]byte, int(pathLen))
	if _, err := io.ReadFull(r, path); err != nil {
		return packedBlobRecord{}, err
	}
	rec.Path = string(path)
	return rec, nil
}

func packedRecordApproxBytes(rec packedBlobRecord) int {
	return packedRecordHeaderBytes + len(rec.Path)
}

// packedDecodeResult holds the output of decoding a single packed blob. It is
// used as the element type of the results slice in decodePackedBatchParallel,
// where each goroutine writes to its own index without synchronisation.
type packedDecodeResult struct {
	data []byte
	typ  ObjectType
	err  error
}

// scanPackDecodeWorkerCount returns the number of parallel decode goroutines
// to use for the given record count. It ramps from 1 (below
// scanPackDecodeMinRecs) up to min(GOMAXPROCS, scanPackDecodeWorkers,
// recordCount) to avoid spawning more goroutines than there are records or
// available CPUs.
func scanPackDecodeWorkerCount(recordCount int) int {
	if recordCount < scanPackDecodeMinRecs {
		return 1
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > scanPackDecodeWorkers {
		workers = scanPackDecodeWorkers
	}
	if workers > recordCount {
		workers = recordCount
	}
	if workers < 2 {
		return 1
	}
	return workers
}

// scanPackDecodeBatchSize returns the number of records to decode per batch.
// The batch size is workers * scanPackDecodeBatchN, floored at
// scanPackDecodeMinRecs, so each worker gets at least scanPackDecodeBatchN
// records per round and very small batches are avoided.
func scanPackDecodeBatchSize(workers int) int {
	if workers <= 1 {
		return 1
	}
	batch := workers * scanPackDecodeBatchN
	if batch < scanPackDecodeMinRecs {
		return scanPackDecodeMinRecs
	}
	return batch
}

func decodePackedBatchParallel(
	store *store,
	packID int,
	pack *mmap.ReaderAt,
	recs []packedBlobRecord,
	workers int,
) []packedDecodeResult {
	results := make([]packedDecodeResult, len(recs))
	if len(recs) == 0 {
		return results
	}
	if workers <= 1 {
		for i := range recs {
			rec := recs[i]
			data, typ, err := store.getPackedObjectNoCache(pack, rec.Offset, rec.Blob)
			if err != nil {
				err = fmt.Errorf("scan pack %d offset %d blob %s: %w", packID, rec.Offset, rec.Blob, err)
			}
			results[i] = packedDecodeResult{
				data: data,
				typ:  typ,
				err:  err,
			}
		}
		return results
	}

	if workers > len(recs) {
		workers = len(recs)
	}

	jobs := make(chan int, len(recs))
	for i := range recs {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for idx := range jobs {
				rec := recs[idx]
				data, typ, err := store.getPackedObjectNoCache(pack, rec.Offset, rec.Blob)
				if err != nil {
					err = fmt.Errorf("scan pack %d offset %d blob %s: %w", packID, rec.Offset, rec.Blob, err)
				}
				results[idx] = packedDecodeResult{
					data: data,
					typ:  typ,
					err:  err,
				}
			}
		}()
	}
	wg.Wait()
	return results
}

func scanPackedRecordsOrdered(
	store *store,
	packID int,
	pack *mmap.ReaderAt,
	recs []packedBlobRecord,
	emit func(rec packedBlobRecord, data []byte, typ ObjectType) error,
) error {
	if len(recs) == 0 {
		return nil
	}
	if emit == nil {
		return fmt.Errorf("packed emit callback is nil")
	}

	workers := scanPackDecodeWorkerCount(len(recs))
	if workers <= 1 {
		results := decodePackedBatchParallel(store, packID, pack, recs, 1)
		for i := range recs {
			if results[i].err != nil {
				return results[i].err
			}
			if err := emit(recs[i], results[i].data, results[i].typ); err != nil {
				return err
			}
		}
		return nil
	}

	batchSize := scanPackDecodeBatchSize(workers)
	for start := 0; start < len(recs); start += batchSize {
		end := start + batchSize
		if end > len(recs) {
			end = len(recs)
		}
		batch := recs[start:end]
		results := decodePackedBatchParallel(store, packID, pack, batch, workers)
		for i := range batch {
			if results[i].err != nil {
				return results[i].err
			}
			if err := emit(batch[i], results[i].data, results[i].typ); err != nil {
				return err
			}
		}
	}
	return nil
}

// scanPlanner orchestrates the disk-backed (spill) scan pipeline. It owns the
// temporary directory and all spill file writers. The lifecycle is:
//
//  1. appendCandidate -- called once per candidate blob to write into the
//     appropriate OID-first-byte bucket file.
//  2. prepare -- closes bucket writers, reads each bucket for dedup and
//     classification, and writes per-pack and loose plan files.
//  3. execute -- reads the plan files, sorts packed records by offset, decodes
//     blobs in parallel batches, and invokes the BlobScanner callback.
//  4. Close -- removes the temporary directory and all spill files.
//
// Bucket dedup invariant: within a single bucket, the first occurrence of a
// blob OID is kept and all subsequent duplicates are dropped. Combined with
// the SeenSet check, this guarantees that each blob OID is scanned at most
// once per run and across runs.
type scanPlanner struct {
	store *store
	seen  SeenSet

	tempDir string

	// bucketWriters has one writer per OID-first-byte (0x00..0xFF). Nil
	// entries indicate buckets that have not been written to.
	bucketWriters [256]*spillWriter

	// packWriters maps pack index to the spill writer for that pack's plan
	// file. Created lazily during prepare.
	packWriters map[int]*spillWriter
	looseWriter *spillWriter

	// packFiles maps pack index to the filesystem path of the plan file.
	packFiles map[int]string
	looseFile string

	nextChunkID uint64
}

func newScanPlanner(store *store, seen SeenSet) (*scanPlanner, error) {
	tempDir, err := os.MkdirTemp("", "go-gitpack-scan-")
	if err != nil {
		return nil, err
	}
	return &scanPlanner{
		store:       store,
		seen:        seen,
		tempDir:     tempDir,
		packWriters: make(map[int]*spillWriter, 8),
		packFiles:   make(map[int]string, 8),
	}, nil
}

func (p *scanPlanner) Close() error {
	var firstErr error

	for i := 0; i < len(p.bucketWriters); i++ {
		if err := p.bucketWriters[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		p.bucketWriters[i] = nil
	}

	for packID, writer := range p.packWriters {
		if err := writer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(p.packWriters, packID)
	}

	if err := p.looseWriter.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	p.looseWriter = nil

	if p.tempDir != "" {
		if err := os.RemoveAll(p.tempDir); err != nil && firstErr == nil {
			firstErr = err
		}
		p.tempDir = ""
	}

	return firstErr
}

func (p *scanPlanner) bucketPath(idx byte) string {
	return filepath.Join(p.tempDir, fmt.Sprintf("bucket-%02x.bin", idx))
}

func (p *scanPlanner) packPath(packID int) string {
	return filepath.Join(p.tempDir, fmt.Sprintf("pack-%04d.bin", packID))
}

func (p *scanPlanner) nextChunkPath() string {
	path := filepath.Join(p.tempDir, fmt.Sprintf("pack-chunk-%06d.bin", p.nextChunkID))
	p.nextChunkID++
	return path
}

func (p *scanPlanner) ensureBucketWriter(idx byte) (*spillWriter, error) {
	writer := p.bucketWriters[idx]
	if writer != nil {
		return writer, nil
	}
	writer, err := openSpillWriter(p.bucketPath(idx))
	if err != nil {
		return nil, err
	}
	p.bucketWriters[idx] = writer
	return writer, nil
}

func (p *scanPlanner) ensurePackWriter(packID int) (*spillWriter, error) {
	if writer, ok := p.packWriters[packID]; ok {
		return writer, nil
	}
	path := p.packPath(packID)
	writer, err := openSpillWriter(path)
	if err != nil {
		return nil, err
	}
	p.packWriters[packID] = writer
	p.packFiles[packID] = path
	return writer, nil
}

func (p *scanPlanner) ensureLooseWriter() (*spillWriter, error) {
	if p.looseWriter != nil {
		return p.looseWriter, nil
	}
	path := filepath.Join(p.tempDir, "loose.bin")
	writer, err := openSpillWriter(path)
	if err != nil {
		return nil, err
	}
	p.looseWriter = writer
	p.looseFile = path
	return writer, nil
}

func (p *scanPlanner) appendCandidate(rec blobRecord) error {
	writer, err := p.ensureBucketWriter(rec.Blob[0])
	if err != nil {
		return err
	}
	return writeBlobRecord(writer.writer, rec)
}

func (p *scanPlanner) closeBucketWriters() error {
	var firstErr error
	for i := 0; i < len(p.bucketWriters); i++ {
		if err := p.bucketWriters[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		p.bucketWriters[i] = nil
	}
	return firstErr
}

func (p *scanPlanner) closePlanWriters() error {
	var firstErr error
	for id, writer := range p.packWriters {
		if err := writer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(p.packWriters, id)
	}
	if err := p.looseWriter.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	p.looseWriter = nil
	return firstErr
}

func (p *scanPlanner) prepare() error {
	if err := p.closeBucketWriters(); err != nil {
		return err
	}

	packIDByHandle := make(map[*mmap.ReaderAt]int, len(p.store.packs))
	for i := 0; i < len(p.store.packs); i++ {
		pf := p.store.packs[i]
		if pf == nil || pf.pack == nil {
			continue
		}
		packIDByHandle[pf.pack] = i
	}

	for i := 0; i < 256; i++ {
		path := p.bucketPath(byte(i))
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if err := p.consumeBucket(path, packIDByHandle); err != nil {
			return err
		}
	}

	return p.closePlanWriters()
}

// consumeBucket reads a single bucket file and performs dedup + classification.
//
// Dedup invariant: seenInBucket tracks blob OIDs encountered so far *within
// this bucket*. Because all records for a given first-byte land in the same
// bucket, the set guarantees that no blob OID is written to the plan files
// more than once. Cross-run dedup is handled by the SeenSet check that
// follows.
func (p *scanPlanner) consumeBucket(path string, packIDByHandle map[*mmap.ReaderAt]int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, scanSpillBufSize)
	seenInBucket := make(map[Hash]struct{}, 4096)
	for {
		rec, err := readBlobRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("read bucket %s: %w", path, err)
		}
		if _, ok := seenInBucket[rec.Blob]; ok {
			continue
		}
		seenInBucket[rec.Blob] = struct{}{}

		if p.seen != nil {
			ok, err := p.seen.Has(rec.Blob)
			if err != nil {
				return err
			}
			if ok {
				continue
			}
		}

		pack, off, ok := p.store.findPackedObject(rec.Blob)
		if !ok {
			writer, err := p.ensureLooseWriter()
			if err != nil {
				return err
			}
			if err := writeBlobRecord(writer.writer, rec); err != nil {
				return err
			}
			continue
		}

		packID, found := packIDByHandle[pack]
		if !found {
			return fmt.Errorf("packed object %s mapped to unknown pack handle", rec.Blob)
		}
		writer, err := p.ensurePackWriter(packID)
		if err != nil {
			return err
		}
		packedRec := packedBlobRecord{
			Offset: off,
			Blob:   rec.Blob,
			Commit: rec.Commit,
			Path:   rec.Path,
		}
		if err := writePackedBlobRecord(writer.writer, packedRec); err != nil {
			return err
		}
	}

	return nil
}

func (p *scanPlanner) execute(scanner BlobScanner) error {
	packIDs := make([]int, 0, len(p.packFiles))
	for packID := range p.packFiles {
		packIDs = append(packIDs, packID)
	}
	sort.Ints(packIDs)

	for _, packID := range packIDs {
		if packID < 0 || packID >= len(p.store.packs) {
			return fmt.Errorf("pack id %d out of range", packID)
		}
		pf := p.store.packs[packID]
		if pf == nil || pf.pack == nil {
			continue
		}
		if err := p.executePackedFile(packID, p.packFiles[packID], pf.pack, scanner); err != nil {
			return err
		}
	}

	if p.looseFile != "" {
		if err := p.executeLooseFile(p.looseFile, scanner); err != nil {
			return err
		}
	}
	return nil
}

func (p *scanPlanner) executePackedFile(packID int, planPath string, pack *mmap.ReaderAt, scanner BlobScanner) error {
	chunks, err := p.buildSortedPackChunks(planPath)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		defer os.Remove(chunk)
	}

	if len(chunks) == 0 {
		return nil
	}

	if err := p.scanSortedPackChunks(packID, pack, chunks, scanner); err != nil {
		return err
	}
	return nil
}

// buildSortedPackChunks performs the "sort" phase of the external sort. It
// reads a plan file of packedBlobRecords, accumulates them into memory chunks
// of at most scanPackSortChunkSize bytes, sorts each chunk by ascending
// packfile offset, and writes the sorted chunk to a temporary file. The
// returned paths are later merged by scanSortedPackChunks using a min-heap
// to produce a globally offset-ordered stream.
func (p *scanPlanner) buildSortedPackChunks(planPath string) ([]string, error) {
	f, err := os.Open(planPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, scanSpillBufSize)
	chunks := make([]string, 0, 4)

	records := make([]packedBlobRecord, 0, 4096)
	chunkBytes := 0
	flush := func() error {
		if len(records) == 0 {
			return nil
		}

		sort.Slice(records, func(i, j int) bool {
			a := records[i]
			b := records[j]
			if a.Offset != b.Offset {
				return a.Offset < b.Offset
			}
			if cmp := bytes.Compare(a.Blob[:], b.Blob[:]); cmp != 0 {
				return cmp < 0
			}
			if cmp := bytes.Compare(a.Commit[:], b.Commit[:]); cmp != 0 {
				return cmp < 0
			}
			return a.Path < b.Path
		})

		chunkPath := p.nextChunkPath()
		w, err := openSpillWriter(chunkPath)
		if err != nil {
			return err
		}
		for i := range records {
			if err := writePackedBlobRecord(w.writer, records[i]); err != nil {
				_ = w.Close()
				return err
			}
		}
		if err := w.Close(); err != nil {
			return err
		}
		chunks = append(chunks, chunkPath)

		records = records[:0]
		chunkBytes = 0
		return nil
	}

	for {
		rec, err := readPackedBlobRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("read packed plan %s: %w", planPath, err)
		}
		records = append(records, rec)
		chunkBytes += packedRecordApproxBytes(rec)
		if chunkBytes >= scanPackSortChunkSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}
	return chunks, nil
}

// packedChunkReader wraps an open sorted-chunk file with a buffered reader.
// It is used during the k-way merge phase to stream records from each chunk.
type packedChunkReader struct {
	file *os.File
	r    *bufio.Reader
}

func (c *packedChunkReader) Close() error {
	if c == nil || c.file == nil {
		return nil
	}
	err := c.file.Close()
	c.file = nil
	c.r = nil
	return err
}

// packedMergeItem pairs a record with the index of the chunk it came from,
// so the merge loop knows which chunk reader to advance after popping.
type packedMergeItem struct {
	record   packedBlobRecord
	chunkIdx int
}

// packedMergeHeap is a min-heap ordered by packedBlobRecord.Offset (with
// tie-breaking on Blob, Commit, and Path) used for the k-way merge of
// sorted chunk files. It implements container/heap.Interface.
type packedMergeHeap []packedMergeItem

func (h packedMergeHeap) Len() int { return len(h) }

func (h packedMergeHeap) Less(i, j int) bool {
	a := h[i].record
	b := h[j].record
	if a.Offset != b.Offset {
		return a.Offset < b.Offset
	}
	if cmp := bytes.Compare(a.Blob[:], b.Blob[:]); cmp != 0 {
		return cmp < 0
	}
	if cmp := bytes.Compare(a.Commit[:], b.Commit[:]); cmp != 0 {
		return cmp < 0
	}
	return a.Path < b.Path
}

func (h packedMergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *packedMergeHeap) Push(x any) {
	*h = append(*h, x.(packedMergeItem))
}

func (h *packedMergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// scanSortedPackChunks performs the "merge" phase of the external sort. It
// opens all pre-sorted chunk files, initialises a min-heap ordered by packfile
// offset, and streams records in globally ascending offset order. Records are
// accumulated into batches of scanPackFlushRecords (or larger, based on the
// decode batch size) and decoded in parallel before being passed to the
// BlobScanner.
func (p *scanPlanner) scanSortedPackChunks(
	packID int,
	pack *mmap.ReaderAt,
	chunkPaths []string,
	scanner BlobScanner,
) error {
	readers := make([]*packedChunkReader, len(chunkPaths))
	defer func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}()

	h := make(packedMergeHeap, 0, len(chunkPaths))
	for i := range chunkPaths {
		f, err := os.Open(chunkPaths[i])
		if err != nil {
			return err
		}
		reader := &packedChunkReader{file: f, r: bufio.NewReaderSize(f, scanSpillBufSize)}
		readers[i] = reader

		rec, err := readPackedBlobRecord(reader.r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return err
		}
		h = append(h, packedMergeItem{record: rec, chunkIdx: i})
	}
	heap.Init(&h)

	batchSize := scanPackDecodeBatchSize(scanPackDecodeWorkerCount(scanPackFlushRecords))
	if batchSize < scanPackFlushRecords {
		batchSize = scanPackFlushRecords
	}
	batch := make([]packedBlobRecord, 0, batchSize)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := scanPackedRecordsOrdered(p.store, packID, pack, batch, func(rec packedBlobRecord, data []byte, typ ObjectType) error {
			if typ != ObjBlob {
				return nil
			}
			meta := ScanMeta{
				Blob:   rec.Blob,
				Commit: rec.Commit,
				Path:   rec.Path,
			}
			if err := func() error {
				r := getBytesReader(data)
				err := scanner.ScanBlob(r, meta)
				putBytesReader(r)
				return err
			}(); err != nil {
				return fmt.Errorf("scan blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
			}
			if p.seen != nil {
				if err := p.seen.Put(rec.Blob); err != nil {
					return fmt.Errorf("mark seen %s: %w", rec.Blob, err)
				}
			}
			return nil
		})
		batch = batch[:0]
		return err
	}

	for h.Len() > 0 {
		item := heap.Pop(&h).(packedMergeItem)
		rec := item.record

		batch = append(batch, rec)
		if len(batch) >= batchSize {
			if err := flushBatch(); err != nil {
				return err
			}
		}

		next, err := readPackedBlobRecord(readers[item.chunkIdx].r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return err
		}
		heap.Push(&h, packedMergeItem{record: next, chunkIdx: item.chunkIdx})
	}

	if err := flushBatch(); err != nil {
		return err
	}

	return nil
}

func (p *scanPlanner) executeLooseFile(path string, scanner BlobScanner) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, scanSpillBufSize)
	for {
		rec, err := readBlobRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		data, typ, err := p.store.getNoCache(rec.Blob)
		if err != nil {
			return fmt.Errorf("load loose blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
		}
		if typ != ObjBlob {
			continue
		}

		meta := ScanMeta{
			Blob:   rec.Blob,
			Commit: rec.Commit,
			Path:   rec.Path,
		}
		if err := func() error {
			r := getBytesReader(data)
			err := scanner.ScanBlob(r, meta)
			putBytesReader(r)
			return err
		}(); err != nil {
			return fmt.Errorf("scan loose blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
		}
		if p.seen != nil {
			if err := p.seen.Put(rec.Blob); err != nil {
				return fmt.Errorf("mark seen %s: %w", rec.Blob, err)
			}
		}
	}

	return nil
}

// streamingPackExecutor provides a low-memory alternative to the full
// scanPlanner by buffering packed blob records per-pack and flushing them in
// offset-sorted order once a buffer reaches scanPackFlushRecords records or
// scanPackFlushBytes bytes. This avoids materialising the complete plan on
// disk at the cost of slightly less optimal I/O ordering (records are sorted
// within each flush window rather than globally across the entire pack).
//
// Loose objects are decoded and scanned immediately upon enqueue since they
// cannot benefit from offset sorting.
type streamingPackExecutor struct {
	hs      *HistoryScanner
	seen    SeenSet
	scanner BlobScanner

	// packIDByHandle maps mmap handles back to pack indices for O(1) lookup.
	packIDByHandle map[*mmap.ReaderAt]int

	// packBuffers accumulates packedBlobRecords per pack index. Each buffer
	// is flushed (sorted by offset, decoded, scanned) when it reaches a
	// threshold, then reused via slice reset (recs[:0]).
	packBuffers    map[int][]packedBlobRecord
	packBufferSize map[int]int
}

func newStreamingPackExecutor(hs *HistoryScanner, seen SeenSet, scanner BlobScanner) *streamingPackExecutor {
	packIDByHandle := make(map[*mmap.ReaderAt]int, len(hs.store.packs))
	for i := 0; i < len(hs.store.packs); i++ {
		pf := hs.store.packs[i]
		if pf == nil || pf.pack == nil {
			continue
		}
		packIDByHandle[pf.pack] = i
	}

	return &streamingPackExecutor{
		hs:             hs,
		seen:           seen,
		scanner:        scanner,
		packIDByHandle: packIDByHandle,
		packBuffers:    make(map[int][]packedBlobRecord, len(hs.store.packs)),
		packBufferSize: make(map[int]int, len(hs.store.packs)),
	}
}

func (e *streamingPackExecutor) enqueue(rec blobRecord) error {
	pack, off, ok := e.hs.store.findPackedObject(rec.Blob)
	if !ok {
		return e.scanLoose(rec)
	}

	packID, found := e.packIDByHandle[pack]
	if !found {
		return fmt.Errorf("packed object %s mapped to unknown pack handle", rec.Blob)
	}

	packed := packedBlobRecord{
		Offset: off,
		Blob:   rec.Blob,
		Commit: rec.Commit,
		Path:   rec.Path,
	}
	e.packBuffers[packID] = append(e.packBuffers[packID], packed)
	e.packBufferSize[packID] += packedRecordApproxBytes(packed)

	if len(e.packBuffers[packID]) >= scanPackFlushRecords || e.packBufferSize[packID] >= scanPackFlushBytes {
		return e.flushPack(packID)
	}
	return nil
}

func (e *streamingPackExecutor) finish() error {
	packIDs := make([]int, 0, len(e.packBuffers))
	for packID := range e.packBuffers {
		if len(e.packBuffers[packID]) == 0 {
			continue
		}
		packIDs = append(packIDs, packID)
	}
	sort.Ints(packIDs)
	for _, packID := range packIDs {
		if err := e.flushPack(packID); err != nil {
			return err
		}
	}
	return nil
}

// flushPack sorts the accumulated records for packID by ascending packfile
// offset and then decodes + scans them in that order. Sorting by offset is
// the central performance optimisation of the scan pipeline: it converts
// what would otherwise be random reads across a multi-GiB packfile into a
// sequential scan, dramatically improving throughput on both spinning disks
// (seek elimination) and SSDs (read-ahead and page-cache locality).
func (e *streamingPackExecutor) flushPack(packID int) error {
	recs := e.packBuffers[packID]
	if len(recs) == 0 {
		return nil
	}

	if packID < 0 || packID >= len(e.hs.store.packs) {
		return fmt.Errorf("pack id %d out of range", packID)
	}
	pf := e.hs.store.packs[packID]
	if pf == nil || pf.pack == nil {
		return fmt.Errorf("pack %d is not available", packID)
	}

	// Sort by ascending offset for sequential I/O (see rationale above).
	sort.Slice(recs, func(i, j int) bool {
		a := recs[i]
		b := recs[j]
		if a.Offset != b.Offset {
			return a.Offset < b.Offset
		}
		if cmp := bytes.Compare(a.Blob[:], b.Blob[:]); cmp != 0 {
			return cmp < 0
		}
		if cmp := bytes.Compare(a.Commit[:], b.Commit[:]); cmp != 0 {
			return cmp < 0
		}
		return a.Path < b.Path
	})

	if err := scanPackedRecordsOrdered(e.hs.store, packID, pf.pack, recs, func(rec packedBlobRecord, data []byte, typ ObjectType) error {
		if typ != ObjBlob {
			return nil
		}
		meta := ScanMeta{
			Blob:   rec.Blob,
			Commit: rec.Commit,
			Path:   rec.Path,
		}
		r := getBytesReader(data)
		if err := e.scanner.ScanBlob(r, meta); err != nil {
			putBytesReader(r)
			return fmt.Errorf("scan blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
		}
		putBytesReader(r)
		if e.seen != nil {
			if err := e.seen.Put(rec.Blob); err != nil {
				return fmt.Errorf("mark seen %s: %w", rec.Blob, err)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	e.packBuffers[packID] = recs[:0]
	e.packBufferSize[packID] = 0
	return nil
}

func (e *streamingPackExecutor) scanLoose(rec blobRecord) error {
	data, typ, err := e.hs.store.getNoCache(rec.Blob)
	if err != nil {
		return fmt.Errorf("load loose blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
	}
	if typ != ObjBlob {
		return nil
	}

	meta := ScanMeta{
		Blob:   rec.Blob,
		Commit: rec.Commit,
		Path:   rec.Path,
	}
	r := getBytesReader(data)
	if err := e.scanner.ScanBlob(r, meta); err != nil {
		putBytesReader(r)
		return fmt.Errorf("scan loose blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
	}
	putBytesReader(r)
	if e.seen != nil {
		if err := e.seen.Put(rec.Blob); err != nil {
			return fmt.Errorf("mark seen %s: %w", rec.Blob, err)
		}
	}
	return nil
}

func (hs *HistoryScanner) walkBlobCandidates(visit func(blobRecord) error) error {
	if visit == nil {
		return nil
	}
	return hs.walkCommitsFromRefs(func(c commitInfo) error {
		parentTree, err := hs.firstParentTree(c)
		if err != nil {
			return err
		}
		return walkDiff(hs.store, parentTree, c.TreeOID, "", func(path string, oldOID, newOID Hash, mode uint32) error {
			if !isBlobMode(mode) {
				return nil
			}
			if newOID.IsZero() || newOID == oldOID {
				return nil
			}

			rec := blobRecord{
				Blob:   newOID,
				Commit: c.OID,
				Path:   filepath.ToSlash(path),
			}
			return visit(rec)
		})
	})
}

func (hs *HistoryScanner) collectInMemoryScanPlan(seen SeenSet) (*inMemoryScanPlan, error) {
	plan := &inMemoryScanPlan{
		packedByID: make(map[int][]packedBlobRecord, len(hs.store.packs)),
		loose:      make([]blobRecord, 0, 1024),
	}
	packIDByHandle := make(map[*mmap.ReaderAt]int, len(hs.store.packs))
	for i := 0; i < len(hs.store.packs); i++ {
		pf := hs.store.packs[i]
		if pf == nil || pf.pack == nil {
			continue
		}
		packIDByHandle[pf.pack] = i
	}

	scheduled := make(map[Hash]struct{}, 2048)
	var (
		totalBytes uint64
		totalBlobs int
	)

	err := hs.walkBlobCandidates(func(rec blobRecord) error {
		if _, ok := scheduled[rec.Blob]; ok {
			return nil
		}
		scheduled[rec.Blob] = struct{}{}

		if seen != nil {
			ok, err := seen.Has(rec.Blob)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		}

		totalBlobs++
		totalBytes += uint64(blobRecordHeaderBytes + len(rec.Path))
		if totalBlobs > scanFastPathMaxBlobs || totalBytes > scanFastPathMaxBytes {
			return errScanPlanOverflow
		}

		pack, off, ok := hs.store.findPackedObject(rec.Blob)
		if !ok {
			plan.loose = append(plan.loose, rec)
			return nil
		}

		packID, found := packIDByHandle[pack]
		if !found {
			return fmt.Errorf("packed object %s mapped to unknown pack handle", rec.Blob)
		}
		plan.packedByID[packID] = append(plan.packedByID[packID], packedBlobRecord{
			Offset: off,
			Blob:   rec.Blob,
			Commit: rec.Commit,
			Path:   rec.Path,
		})
		totalBytes += 8
		return nil
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (hs *HistoryScanner) executeInMemoryScanPlan(plan *inMemoryScanPlan, seen SeenSet, scanner BlobScanner) error {
	if plan == nil {
		return nil
	}

	packIDs := make([]int, 0, len(plan.packedByID))
	for packID := range plan.packedByID {
		packIDs = append(packIDs, packID)
	}
	sort.Ints(packIDs)

	for _, packID := range packIDs {
		if packID < 0 || packID >= len(hs.store.packs) {
			return fmt.Errorf("pack id %d out of range", packID)
		}
		pf := hs.store.packs[packID]
		if pf == nil || pf.pack == nil {
			continue
		}

		recs := plan.packedByID[packID]
		sort.Slice(recs, func(i, j int) bool {
			a := recs[i]
			b := recs[j]
			if a.Offset != b.Offset {
				return a.Offset < b.Offset
			}
			if cmp := bytes.Compare(a.Blob[:], b.Blob[:]); cmp != 0 {
				return cmp < 0
			}
			if cmp := bytes.Compare(a.Commit[:], b.Commit[:]); cmp != 0 {
				return cmp < 0
			}
			return a.Path < b.Path
		})
		plan.packedByID[packID] = recs

		if err := scanPackedRecordsOrdered(hs.store, packID, pf.pack, recs, func(rec packedBlobRecord, data []byte, typ ObjectType) error {
			if typ != ObjBlob {
				return nil
			}
			meta := ScanMeta{
				Blob:   rec.Blob,
				Commit: rec.Commit,
				Path:   rec.Path,
			}
			if err := func() error {
				r := getBytesReader(data)
				err := scanner.ScanBlob(r, meta)
				putBytesReader(r)
				return err
			}(); err != nil {
				return fmt.Errorf("scan blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
			}
			if seen != nil {
				if err := seen.Put(rec.Blob); err != nil {
					return fmt.Errorf("mark seen %s: %w", rec.Blob, err)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	for i := range plan.loose {
		rec := plan.loose[i]
		data, typ, err := hs.store.getNoCache(rec.Blob)
		if err != nil {
			return fmt.Errorf("load loose blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
		}
		if typ != ObjBlob {
			continue
		}

		meta := ScanMeta{
			Blob:   rec.Blob,
			Commit: rec.Commit,
			Path:   rec.Path,
		}
		if err := func() error {
			r := getBytesReader(data)
			err := scanner.ScanBlob(r, meta)
			putBytesReader(r)
			return err
		}(); err != nil {
			return fmt.Errorf("scan loose blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
		}
		if seen != nil {
			if err := seen.Put(rec.Blob); err != nil {
				return fmt.Errorf("mark seen %s: %w", rec.Blob, err)
			}
		}
	}

	return nil
}

func (hs *HistoryScanner) scanViaSpillPlanner(seen SeenSet, scanner BlobScanner) error {
	planner, err := newScanPlanner(hs.store, seen)
	if err != nil {
		return err
	}
	defer planner.Close()

	if err := hs.walkBlobCandidates(func(rec blobRecord) error {
		return planner.appendCandidate(rec)
	}); err != nil {
		return err
	}
	if err := planner.prepare(); err != nil {
		return err
	}
	return planner.execute(scanner)
}

// scanBlobsStreaming executes blob-centric scans in streaming mode with
// bounded per-pack buffers. Candidates are deduped by OID for the run.
func (hs *HistoryScanner) scanBlobsStreaming(seen SeenSet, scanner BlobScanner) error {
	if scanner == nil {
		return fmt.Errorf("scanner is nil")
	}

	planned := make(map[Hash]struct{}, 2048)
	exec := newStreamingPackExecutor(hs, seen, scanner)

	if err := hs.walkBlobCandidates(func(rec blobRecord) error {
		if _, ok := planned[rec.Blob]; ok {
			return nil
		}
		if seen != nil {
			ok, err := seen.Has(rec.Blob)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		}

		planned[rec.Blob] = struct{}{}
		return exec.enqueue(rec)
	}); err != nil {
		return err
	}

	return exec.finish()
}

// planScanJobs builds pack-sorted blob scan jobs with per-run OID dedupe.
// It is internal and only used by package tests/benchmarks.
func (hs *HistoryScanner) planScanJobs(seen SeenSet) (map[*mmap.ReaderAt][]ScanJob, error) {
	jobsByPack := make(map[*mmap.ReaderAt][]ScanJob, 16)
	scheduled := make(map[Hash]struct{}, 1024)

	err := hs.walkCommitsFromRefs(func(c commitInfo) error {
		parentTree, err := hs.firstParentTree(c)
		if err != nil {
			return err
		}
		return walkDiff(hs.store, parentTree, c.TreeOID, "", func(path string, oldOID, newOID Hash, mode uint32) error {
			if !isBlobMode(mode) {
				return nil
			}
			if newOID.IsZero() || newOID == oldOID {
				return nil
			}
			if _, ok := scheduled[newOID]; ok {
				return nil
			}
			if seen != nil {
				ok, err := seen.Has(newOID)
				if err != nil {
					return err
				}
				if ok {
					return nil
				}
			}

			p, off, ok := hs.store.findPackedObject(newOID)
			if !ok {
				return nil
			}

			scheduled[newOID] = struct{}{}
			jobsByPack[p] = append(jobsByPack[p], ScanJob{
				Blob:   newOID,
				Commit: c.OID,
				Path:   filepath.ToSlash(path),
				Pack:   p,
				Offset: off,
			})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	for p, jobs := range jobsByPack {
		sort.Slice(jobs, func(i, j int) bool { return jobs[i].Offset < jobs[j].Offset })
		jobsByPack[p] = jobs
	}

	return jobsByPack, nil
}
