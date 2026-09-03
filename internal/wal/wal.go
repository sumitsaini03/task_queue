// Package wal implements a hand-rolled write-ahead log for task
// state durability. Every state transition is appended to the log
// before being acted on. Periodic snapshots + compaction keep
// disk usage bounded.
//
// # Binary Record Format
//
// Each record in the WAL has the following layout:
//
//	+----------+----------+----------+----------+---------+----------+
//	| magic(2) | len(4)   | seq(8)   | crc32(4) | type(1) | data(N)  |
//	+----------+----------+----------+----------+---------+----------+
//
// - magic: 2-byte magic number (0xWA, 0x4C) for record boundary detection
// - len:   4-byte little-endian uint32, total byte count of seq+crc32+type+data
// - seq:   8-byte little-endian uint64 sequence number (monotonically increasing)
// - crc32: 4-byte CRC32-C checksum of type+data
// - type:  1-byte entry type
// - data:  variable-length JSON-encoded payload
//
// # Durability Guarantee
//
// With SyncPolicy "every": a successful Append() call guarantees the record
// is durable on disk (fsync'd). The queue MUST NOT acknowledge an enqueue
// to the caller until Append returns successfully.
//
// With SyncPolicy "batch": records are buffered and fsync'd periodically.
// A crash may lose up to one batch interval of records.
//
// With SyncPolicy "none": records are written but fsync is left to the OS.
// Fastest but weakest durability.
//
// # Recovery Behavior
//
// On Replay:
//   - Valid records are yielded to the callback in sequence order.
//   - A truncated final record (incomplete magic/len/data) is silently truncated
//     and the WAL is repaired by truncating the file to the last valid record.
//   - Corruption in the MIDDLE of the WAL (valid magic+len but bad CRC) causes
//     Replay to return an error — it does NOT silently skip corrupt records.
package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record format constants.
var walMagic = [2]byte{0xAA, 0x4C} // "WAL" marker

const (
	magicSize  = 2
	lenSize    = 4
	seqSize    = 8
	crcSize    = 4
	typeSize   = 1
	headerSize = magicSize + lenSize // magic + length prefix
	minBody    = seqSize + crcSize + typeSize
)

// EntryType represents the kind of WAL entry.
type EntryType uint8

const (
	EntryEnqueue  EntryType = iota + 1 // Task added to queue
	EntryStart                         // Worker picked up task
	EntryComplete                      // Task finished successfully
	EntryFail                          // Task failed (may retry)
	EntryRetry                         // Task scheduled for retry
	EntryDead                          // Task moved to DLQ
	EntryCancel                        // Task cancelled
	EntrySnapshot                      // Snapshot marker
)

func (e EntryType) String() string {
	switch e {
	case EntryEnqueue:
		return "enqueue"
	case EntryStart:
		return "start"
	case EntryComplete:
		return "complete"
	case EntryFail:
		return "fail"
	case EntryRetry:
		return "retry"
	case EntryDead:
		return "dead"
	case EntryCancel:
		return "cancel"
	case EntrySnapshot:
		return "snapshot"
	default:
		return "unknown"
	}
}

// Entry is a single record in the write-ahead log.
type Entry struct {
	Seq       uint64    `json:"seq"`
	Type      EntryType `json:"type"`
	TaskID    string    `json:"task_id"`
	Timestamp int64     `json:"ts"`
	Data      []byte    `json:"data,omitempty"` // Full task JSON on enqueue, error on fail
}

// SyncPolicy controls when fsync is called.
type SyncPolicy int

const (
	SyncEvery SyncPolicy = iota // fsync after every append
	SyncBatch                   // fsync periodically (caller manages)
	SyncNone                    // no explicit fsync
)

// ParseSyncPolicy converts a string to SyncPolicy.
func ParseSyncPolicy(s string) SyncPolicy {
	switch s {
	case "batch":
		return SyncBatch
	case "none":
		return SyncNone
	default:
		return SyncEvery
	}
}

// Errors
var (
	ErrCorrupted    = errors.New("wal: corrupted record detected")
	ErrClosed       = errors.New("wal: log is closed")
	ErrInvalidEntry = errors.New("wal: invalid entry")
)

// WAL is an append-only log backed by a file on disk.
type WAL struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	path   string
	seq    uint64 // last written sequence number
	size   int64  // current file size
	closed bool
	policy SyncPolicy
	crc    *crc32.Table
}

// Open opens or creates a WAL file at the given path.
func Open(path string, policy SyncPolicy) (*WAL, error) {
	cleanPath := filepath.Clean(path)
	dir := filepath.Dir(cleanPath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}

	// #nosec G302, G304 -- WAL file path is validated by config; permissions 0600 restrict access to owner
	f, err := os.OpenFile(cleanPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}

	// Seek to end for appending
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: seek: %w", err)
	}

	w := &WAL{
		file:   f,
		writer: bufio.NewWriterSize(f, 64*1024), // 64KB write buffer
		path:   path,
		size:   size,
		policy: policy,
		crc:    crc32.MakeTable(crc32.Castagnoli),
	}

	return w, nil
}

// Append writes an entry to the WAL. With SyncEvery policy, the entry
// is fsync'd before this method returns. Returns the sequence number
// assigned to the entry.
func (w *WAL) Append(entry *Entry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, ErrClosed
	}

	// Assign sequence number
	w.seq++
	entry.Seq = w.seq
	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixNano()
	}

	// Encode the record
	buf, err := w.encodeRecord(entry)
	if err != nil {
		w.seq-- // rollback
		return 0, fmt.Errorf("wal: encode: %w", err)
	}

	// Write
	n, err := w.writer.Write(buf)
	if err != nil {
		w.seq-- // rollback
		return 0, fmt.Errorf("wal: write: %w", err)
	}
	w.size += int64(n)

	// Sync based on policy
	if w.policy == SyncEvery {
		if err := w.writer.Flush(); err != nil {
			return 0, fmt.Errorf("wal: flush: %w", err)
		}
		if err := w.file.Sync(); err != nil {
			return 0, fmt.Errorf("wal: sync: %w", err)
		}
	}

	return entry.Seq, nil
}

// Flush flushes the write buffer to the OS. Does not fsync.
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.writer.Flush()
}

// Sync flushes the write buffer and fsyncs to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	return w.file.Sync()
}

// Close flushes, syncs, and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true

	var errs []error
	if err := w.writer.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("wal: flush on close: %w", err))
	}
	if err := w.file.Sync(); err != nil {
		errs = append(errs, fmt.Errorf("wal: sync on close: %w", err))
	}
	if err := w.file.Close(); err != nil {
		errs = append(errs, fmt.Errorf("wal: close: %w", err))
	}
	return errors.Join(errs...)
}

// LastSeq returns the last written sequence number.
func (w *WAL) LastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// Size returns the current WAL file size in bytes.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Path returns the WAL file path.
func (w *WAL) Path() string {
	return w.path
}

// SetSeq sets the sequence counter (used after snapshot recovery).
func (w *WAL) SetSeq(seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq = seq
}

// encodeRecord serializes an Entry into the binary wire format.
func (w *WAL) encodeRecord(entry *Entry) ([]byte, error) {
	// Encode the data portion: type + JSON payload
	payload, err := json.Marshal(entry)
	if err != nil {
		return nil, err
	}

	bodyLen := seqSize + crcSize + typeSize + len(payload)
	totalLen := headerSize + bodyLen
	buf := make([]byte, totalLen)

	// Magic
	buf[0] = walMagic[0]
	buf[1] = walMagic[1]

	// Length (of body: seq + crc + type + data)
	if bodyLen < 0 || uint64(bodyLen) > math.MaxUint32 {
		return nil, errors.New("wal: body length exceeds uint32 limit")
	}
	// #nosec G115 -- bodyLen is checked against math.MaxUint32 above
	binary.LittleEndian.PutUint32(buf[magicSize:magicSize+lenSize], uint32(bodyLen))

	// Sequence number
	binary.LittleEndian.PutUint64(buf[headerSize:headerSize+seqSize], entry.Seq)

	// Type byte + payload (we'll compute CRC over these)
	typeAndPayload := make([]byte, typeSize+len(payload))
	typeAndPayload[0] = byte(entry.Type)
	copy(typeAndPayload[1:], payload)

	// CRC32-C over type+payload
	checksum := crc32.Checksum(typeAndPayload, w.crc)
	binary.LittleEndian.PutUint32(buf[headerSize+seqSize:headerSize+seqSize+crcSize], checksum)

	// Type + payload
	copy(buf[headerSize+seqSize+crcSize:], typeAndPayload)

	return buf, nil
}

// Replay reads all valid entries from the WAL file and calls fn for each.
// It handles:
// - Valid records: passed to fn in order
// - Truncated tail: silently truncated, WAL repaired
// - Mid-file corruption: returns ErrCorrupted
//
// After Replay, the WAL is positioned at the end ready for new appends.
// The WAL's sequence counter is set to the last valid sequence number.
func Replay(path string, fn func(entry *Entry) error) (lastSeq uint64, err error) {
	cleanPath := filepath.Clean(path)
	// #nosec G302, G304 -- WAL file path is validated by config; permissions 0600 restrict access to owner
	f, err := os.OpenFile(cleanPath, os.O_RDWR, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("wal: open for replay: %w", err)
	}
	defer f.Close()

	crcTab := crc32.MakeTable(crc32.Castagnoli)
	reader := bufio.NewReader(f)
	var validEnd int64 // file offset of the end of the last valid record
	var recordCount int

	for {
		recordStart := validEnd

		// Read magic
		magicBuf := make([]byte, magicSize)
		_, err := io.ReadFull(reader, magicBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Clean end or truncated magic — truncate to last valid
			break
		}
		if err != nil {
			return 0, fmt.Errorf("wal: read magic: %w", err)
		}

		if magicBuf[0] != walMagic[0] || magicBuf[1] != walMagic[1] {
			if recordCount == 0 {
				return 0, fmt.Errorf("wal: invalid file (bad magic at start)")
			}
			// Corruption in middle — error, don't silently skip
			return 0, fmt.Errorf("%w: bad magic at offset %d", ErrCorrupted, recordStart)
		}

		// Read length
		lenBuf := make([]byte, lenSize)
		_, err = io.ReadFull(reader, lenBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Truncated length — truncate to last valid
			break
		}
		if err != nil {
			return 0, fmt.Errorf("wal: read len: %w", err)
		}
		bodyLen := binary.LittleEndian.Uint32(lenBuf)

		// Sanity check length
		if bodyLen < uint32(minBody) || bodyLen > 64*1024*1024 {
			if recordCount == 0 {
				return 0, fmt.Errorf("wal: invalid file (bad length at start)")
			}
			return 0, fmt.Errorf("%w: invalid body length %d at offset %d", ErrCorrupted, bodyLen, recordStart)
		}

		// Read body
		body := make([]byte, bodyLen)
		_, err = io.ReadFull(reader, body)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Truncated body — this is the torn-write case
			break
		}
		if err != nil {
			return 0, fmt.Errorf("wal: read body: %w", err)
		}

		// Parse body: seq(8) + crc(4) + type(1) + data(N)
		seq := binary.LittleEndian.Uint64(body[0:seqSize])
		storedCRC := binary.LittleEndian.Uint32(body[seqSize : seqSize+crcSize])
		typeAndPayload := body[seqSize+crcSize:]

		// Verify CRC
		computedCRC := crc32.Checksum(typeAndPayload, crcTab)
		if storedCRC != computedCRC {
			// Is this the last record? Check if we're at EOF
			peekBuf := make([]byte, 1)
			_, peekErr := reader.Read(peekBuf)
			if peekErr == io.EOF {
				// Corrupt final record — treat as truncated write
				break
			}
			// Corruption in the middle
			return 0, fmt.Errorf("%w: CRC mismatch at offset %d (stored=%08x computed=%08x)",
				ErrCorrupted, recordStart, storedCRC, computedCRC)
		}

		// Decode entry
		entryType := EntryType(typeAndPayload[0])
		var entry Entry
		if err := json.Unmarshal(typeAndPayload[1:], &entry); err != nil {
			return 0, fmt.Errorf("%w: json decode error at offset %d: %v",
				ErrCorrupted, recordStart, err)
		}
		entry.Type = entryType
		entry.Seq = seq

		// Call handler
		if err := fn(&entry); err != nil {
			return seq, fmt.Errorf("wal: handler error at seq %d: %w", seq, err)
		}

		lastSeq = seq
		validEnd = recordStart + int64(headerSize) + int64(bodyLen)
		recordCount++
	}

	// Truncate file to last valid record to repair torn writes
	info, err := f.Stat()
	if err != nil {
		return lastSeq, fmt.Errorf("wal: stat for truncate: %w", err)
	}
	if info.Size() > validEnd {
		if err := f.Truncate(validEnd); err != nil {
			return lastSeq, fmt.Errorf("wal: truncate: %w", err)
		}
	}

	return lastSeq, nil
}
