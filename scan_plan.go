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
	"sort"

	"golang.org/x/exp/mmap"
)

// SeenSet tracks globally scanned blobs.
type SeenSet interface {
	Has(oid Hash) (bool, error)
	Put(oid Hash) error
}

// ScanJob is one unit of blob-centric scan work.
type ScanJob struct {
	Blob   Hash
	Commit Hash
	Path   string

	Pack   *mmap.ReaderAt
	Offset uint64
}

// ScanMeta carries attribution and identity for a blob scan.
type ScanMeta struct {
	Blob   Hash
	Commit Hash
	Path   string
}

// BlobScanner consumes blob content plus metadata for each planned blob.
type BlobScanner interface {
	ScanBlob(r io.Reader, meta ScanMeta) error
}

const (
	scanSpillBufSize      = 1 << 20
	scanPackSortChunkSize = 32 << 20
	scanMaxPathBytes      = 1 << 20
	scanFastPathMaxBytes  = 768 << 20
	scanFastPathMaxBlobs  = 2_000_000
	scanPackFlushRecords  = 4096
	scanPackFlushBytes    = 8 << 20

	blobRecordHeaderBytes   = 20 + 20 + 4
	packedRecordHeaderBytes = 8 + blobRecordHeaderBytes
)

var errScanPlanOverflow = errors.New("scan in-memory plan overflow")

type blobRecord struct {
	Blob   Hash
	Commit Hash
	Path   string
}

type packedBlobRecord struct {
	Offset uint64
	Blob   Hash
	Commit Hash
	Path   string
}

type inMemoryScanPlan struct {
	packedByID map[int][]packedBlobRecord
	loose      []blobRecord
}

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

type scanPlanner struct {
	store *store
	seen  SeenSet

	tempDir string

	bucketWriters [256]*spillWriter
	packWriters   map[int]*spillWriter
	looseWriter   *spillWriter

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

type packedMergeItem struct {
	record   packedBlobRecord
	chunkIdx int
}

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

	for h.Len() > 0 {
		item := heap.Pop(&h).(packedMergeItem)
		rec := item.record

		data, typ, err := p.store.getPackedObjectNoCache(pack, rec.Offset, rec.Blob)
		if err != nil {
			return fmt.Errorf("scan pack %d offset %d blob %s: %w", packID, rec.Offset, rec.Blob, err)
		}
		if typ == ObjBlob {
			meta := ScanMeta{
				Blob:   rec.Blob,
				Commit: rec.Commit,
				Path:   rec.Path,
			}
			if err := scanner.ScanBlob(bytes.NewReader(data), meta); err != nil {
				return fmt.Errorf("scan blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
			}
			if p.seen != nil {
				if err := p.seen.Put(rec.Blob); err != nil {
					return fmt.Errorf("mark seen %s: %w", rec.Blob, err)
				}
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
		if err := scanner.ScanBlob(bytes.NewReader(data), meta); err != nil {
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

type streamingPackExecutor struct {
	hs      *HistoryScanner
	seen    SeenSet
	scanner BlobScanner

	packIDByHandle map[*mmap.ReaderAt]int
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

	for i := range recs {
		if err := e.scanPacked(packID, pf.pack, recs[i]); err != nil {
			return err
		}
	}

	e.packBuffers[packID] = recs[:0]
	e.packBufferSize[packID] = 0
	return nil
}

func (e *streamingPackExecutor) scanPacked(packID int, pack *mmap.ReaderAt, rec packedBlobRecord) error {
	data, typ, err := e.hs.store.getPackedObjectNoCache(pack, rec.Offset, rec.Blob)
	if err != nil {
		return fmt.Errorf("scan pack %d offset %d blob %s: %w", packID, rec.Offset, rec.Blob, err)
	}
	if typ != ObjBlob {
		return nil
	}

	meta := ScanMeta{
		Blob:   rec.Blob,
		Commit: rec.Commit,
		Path:   rec.Path,
	}
	if err := e.scanner.ScanBlob(bytes.NewReader(data), meta); err != nil {
		return fmt.Errorf("scan blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
	}
	if e.seen != nil {
		if err := e.seen.Put(rec.Blob); err != nil {
			return fmt.Errorf("mark seen %s: %w", rec.Blob, err)
		}
	}
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
	if err := e.scanner.ScanBlob(bytes.NewReader(data), meta); err != nil {
		return fmt.Errorf("scan loose blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
	}
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

		for i := range recs {
			rec := recs[i]
			data, typ, err := hs.store.getPackedObjectNoCache(pf.pack, rec.Offset, rec.Blob)
			if err != nil {
				return fmt.Errorf("scan pack %d offset %d blob %s: %w", packID, rec.Offset, rec.Blob, err)
			}
			if typ != ObjBlob {
				continue
			}

			meta := ScanMeta{
				Blob:   rec.Blob,
				Commit: rec.Commit,
				Path:   rec.Path,
			}
			if err := scanner.ScanBlob(bytes.NewReader(data), meta); err != nil {
				return fmt.Errorf("scan blob %s for %s:%s: %w", rec.Blob, rec.Commit, rec.Path, err)
			}
			if seen != nil {
				if err := seen.Put(rec.Blob); err != nil {
					return fmt.Errorf("mark seen %s: %w", rec.Blob, err)
				}
			}
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
		if err := scanner.ScanBlob(bytes.NewReader(data), meta); err != nil {
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
