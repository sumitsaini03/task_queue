package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func testDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

func walPath(dir string) string {
	return filepath.Join(dir, "test.wal")
}

// --- Basic append and replay ---

func TestWAL_AppendAndReplay(t *testing.T) {
	dir := testDir(t)
	path := walPath(dir)

	w, err := Open(path, SyncEvery)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Append entries
	for i := 0; i < 100; i++ {
		_, err := w.Append(&Entry{
			Type:   EntryEnqueue,
			TaskID: fmt.Sprintf("task-%d", i),
			Data:   []byte(fmt.Sprintf(`{"id":"task-%d"}`, i)),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if w.LastSeq() != 100 {
		t.Errorf("expected seq 100, got %d", w.LastSeq())
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Replay
	var replayed int
	lastSeq, err := Replay(path, func(entry *Entry) error {
		replayed++
		expected := fmt.Sprintf("task-%d", replayed-1)
		if entry.TaskID != expected {
			t.Errorf("entry %d: expected %s, got %s", replayed, expected, entry.TaskID)
		}
		if entry.Type != EntryEnqueue {
			t.Errorf("entry %d: expected enqueue, got %v", replayed, entry.Type)
		}
		if entry.Seq != uint64(replayed) {
			t.Errorf("entry %d: expected seq %d, got %d", replayed, replayed, entry.Seq)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != 100 {
		t.Errorf("expected 100 replayed, got %d", replayed)
	}
	if lastSeq != 100 {
		t.Errorf("expected lastSeq 100, got %d", lastSeq)
	}
}

// --- All entry types ---

func TestWAL_AllEntryTypes(t *testing.T) {
	dir := testDir(t)
	path := walPath(dir)

	types := []EntryType{
		EntryEnqueue, EntryStart, EntryComplete,
		EntryFail, EntryRetry, EntryDead, EntryCancel,
	}

	w, err := Open(path, SyncEvery)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, et := range types {
		w.Append(&Entry{Type: et, TaskID: "t1", Data: []byte(`{}`)})
	}
	w.Close()

	var idx int
	Replay(path, func(entry *Entry) error {
		if entry.Type != types[idx] {
			t.Errorf("entry %d: expected type %v, got %v", idx, types[idx], entry.Type)
		}
		idx++
		return nil
	})
	if idx != len(types) {
		t.Errorf("expected %d entries, got %d", len(types), idx)
	}
}

// --- Empty WAL replay ---

func TestWAL_ReplayEmpty(t *testing.T) {
	dir := testDir(t)
	path := walPath(dir)

	w, _ := Open(path, SyncEvery)
	w.Close()

	var count int
	lastSeq, err := Replay(path, func(entry *Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("replay empty: %v", err)
	}
	if count != 0 || lastSeq != 0 {
		t.Errorf("expected 0/0, got %d/%d", count, lastSeq)
	}
}

// --- Nonexistent WAL replay ---

func TestWAL_ReplayNonexistent(t *testing.T) {
	lastSeq, err := Replay("/nonexistent/path/wal.bin", func(e *Entry) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error for nonexistent, got: %v", err)
	}
	if lastSeq != 0 {
		t.Errorf("expected 0, got %d", lastSeq)
	}
}

// --- Truncated tail recovery ---

func TestWAL_TruncatedTailRecovery(t *testing.T) {
	dir := testDir(t)
	path := walPath(dir)

	// Write 10 valid entries
	w, _ := Open(path, SyncEvery)
	for i := 0; i < 10; i++ {
		w.Append(&Entry{
			Type:   EntryEnqueue,
			TaskID: fmt.Sprintf("task-%d", i),
			Data:   []byte(`{}`),
		})
	}
	w.Close()

	// Truncate file mid-record (remove last 5 bytes)
	info, _ := os.Stat(path)
	os.Truncate(path, info.Size()-5)

	// Replay should recover 9 entries and repair the file
	var count int
	lastSeq, err := Replay(path, func(entry *Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("replay truncated: %v", err)
	}
	if count != 9 {
		t.Errorf("expected 9 recovered entries, got %d", count)
	}
	if lastSeq != 9 {
		t.Errorf("expected lastSeq 9, got %d", lastSeq)
	}

	// File should be repaired — re-open and append should work
	w2, _ := Open(path, SyncEvery)
	w2.SetSeq(lastSeq)
	seq, err := w2.Append(&Entry{Type: EntryEnqueue, TaskID: "new", Data: []byte(`{}`)})
	if err != nil {
		t.Fatalf("append after repair: %v", err)
	}
	if seq != 10 {
		t.Errorf("expected seq 10, got %d", seq)
	}
	w2.Close()

	// Full replay should have 10 entries
	var finalCount int
	Replay(path, func(entry *Entry) error {
		finalCount++
		return nil
	})
	if finalCount != 10 {
		t.Errorf("expected 10 total entries after repair, got %d", finalCount)
	}
}

// --- Truncated magic recovery ---

func TestWAL_TruncatedMagicRecovery(t *testing.T) {
	dir := testDir(t)
	path := walPath(dir)

	w, _ := Open(path, SyncEvery)
	w.Append(&Entry{Type: EntryEnqueue, TaskID: "t1", Data: []byte(`{}`)})
	w.Close()

	// Append just 1 byte of a new magic (simulating crash during write)
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	f.Write([]byte{0xAA})
	f.Close()

	var count int
	_, err := Replay(path, func(entry *Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("expected recovery, got error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 entry, got %d", count)
	}
}

// --- Mid-file corruption detection ---

func TestWAL_MidFileCorruption(t *testing.T) {
	dir := testDir(t)
	path := walPath(dir)

	w, _ := Open(path, SyncEvery)
	for i := 0; i < 5; i++ {
		w.Append(&Entry{Type: EntryEnqueue, TaskID: fmt.Sprintf("t%d", i), Data: []byte(`{}`)})
	}
	w.Close()

	// Corrupt a byte in the middle of the file (in entry 3's CRC area)
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	info, _ := f.Stat()
	// Corrupt at roughly 40% into the file
	corruptOffset := info.Size() * 40 / 100
	f.Seek(corruptOffset, 0)
	f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	f.Close()

	_, err := Replay(path, func(entry *Entry) error { return nil })
	if err == nil {
		t.Fatal("expected corruption error, got nil")
	}
	if !isCorrupted(err) {
		t.Errorf("expected ErrCorrupted, got: %v", err)
	}
}

func isCorrupted(err error) bool {
	return err != nil && (err == ErrCorrupted || contains(err.Error(), "corrupted") || contains(err.Error(), "CRC mismatch"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && searchStr(s, sub)
}

func searchStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// --- Sequence number continuity ---

func TestWAL_SequenceNumbers(t *testing.T) {
	dir := testDir(t)
	path := walPath(dir)

	w, _ := Open(path, SyncEvery)
	for i := 0; i < 50; i++ {
		seq, err := w.Append(&Entry{Type: EntryEnqueue, TaskID: "t", Data: []byte(`{}`)})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if seq != uint64(i+1) {
			t.Errorf("append %d: expected seq %d, got %d", i, i+1, seq)
		}
	}
	w.Close()
}

// --- Concurrent append safety ---

func TestWAL_ConcurrentAppend(t *testing.T) {
	dir := testDir(t)
	path := walPath(dir)

	w, _ := Open(path, SyncEvery)

	const goroutines = 8
	const perGoroutine = 100

	done := make(chan bool, goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			for i := 0; i < perGoroutine; i++ {
				_, err := w.Append(&Entry{
					Type:   EntryEnqueue,
					TaskID: fmt.Sprintf("g%d-t%d", id, i),
					Data:   []byte(`{}`),
				})
				if err != nil {
					t.Errorf("goroutine %d append %d: %v", id, i, err)
				}
			}
			done <- true
		}(g)
	}

	for g := 0; g < goroutines; g++ {
		<-done
	}
	w.Close()

	// Replay should have all entries
	var count int
	seqSeen := make(map[uint64]bool)
	Replay(path, func(entry *Entry) error {
		count++
		if seqSeen[entry.Seq] {
			t.Errorf("duplicate seq %d", entry.Seq)
		}
		seqSeen[entry.Seq] = true
		return nil
	})

	expected := goroutines * perGoroutine
	if count != expected {
		t.Errorf("expected %d entries, got %d", expected, count)
	}
}

// --- Close behavior ---

func TestWAL_AppendAfterClose(t *testing.T) {
	dir := testDir(t)
	path := walPath(dir)

	w, _ := Open(path, SyncEvery)
	w.Close()

	_, err := w.Append(&Entry{Type: EntryEnqueue, TaskID: "t", Data: []byte(`{}`)})
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

// --- Sync policies ---

func TestWAL_SyncPolicies(t *testing.T) {
	for _, policy := range []SyncPolicy{SyncEvery, SyncBatch, SyncNone} {
		t.Run(fmt.Sprintf("policy-%d", policy), func(t *testing.T) {
			dir := testDir(t)
			path := walPath(dir)

			w, err := Open(path, policy)
			if err != nil {
				t.Fatalf("open: %v", err)
			}

			for i := 0; i < 10; i++ {
				w.Append(&Entry{Type: EntryEnqueue, TaskID: "t", Data: []byte(`{}`)})
			}

			if policy == SyncBatch {
				w.Sync() // explicit sync for batch policy
			}
			w.Close()

			var count int
			Replay(path, func(e *Entry) error { count++; return nil })
			if count != 10 {
				t.Errorf("expected 10, got %d", count)
			}
		})
	}
}

// --- Large WAL ---

func TestWAL_LargeWAL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large WAL test in short mode")
	}

	dir := testDir(t)
	path := walPath(dir)

	w, _ := Open(path, SyncNone) // use SyncNone for speed
	const n = 100000
	for i := 0; i < n; i++ {
		w.Append(&Entry{
			Type:   EntryEnqueue,
			TaskID: fmt.Sprintf("task-%d", i),
			Data:   []byte(fmt.Sprintf(`{"id":"task-%d","payload":"data-%d"}`, i, i)),
		})
	}
	w.Sync()
	w.Close()

	var count int
	lastSeq, _ := Replay(path, func(e *Entry) error {
		count++
		return nil
	})
	if count != n {
		t.Errorf("expected %d, got %d", n, count)
	}
	if lastSeq != n {
		t.Errorf("expected lastSeq %d, got %d", n, lastSeq)
	}
}

// --- Corrupt final record (bad CRC at end) ---

func TestWAL_CorruptFinalRecord(t *testing.T) {
	dir := testDir(t)
	path := walPath(dir)

	w, _ := Open(path, SyncEvery)
	for i := 0; i < 5; i++ {
		w.Append(&Entry{Type: EntryEnqueue, TaskID: fmt.Sprintf("t%d", i), Data: []byte(`{}`)})
	}
	w.Close()

	// Read file, corrupt the CRC of the last record
	data, _ := os.ReadFile(path)
	// Find the last magic marker
	lastMagicIdx := -1
	for i := len(data) - 2; i >= 0; i-- {
		if data[i] == walMagic[0] && data[i+1] == walMagic[1] {
			lastMagicIdx = i
			break
		}
	}
	if lastMagicIdx < 0 {
		t.Fatal("could not find last magic")
	}
	// CRC is at offset: magic(2) + len(4) + seq(8) = 14 bytes after magic
	crcOffset := lastMagicIdx + magicSize + lenSize + seqSize
	binary.LittleEndian.PutUint32(data[crcOffset:], 0xDEADBEEF)
	os.WriteFile(path, data, 0644)

	// Replay should recover 4 entries (truncate the corrupt last one)
	var count int
	_, err := Replay(path, func(e *Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("expected silent truncation of corrupt final record, got: %v", err)
	}
	if count != 4 {
		t.Errorf("expected 4 recovered, got %d", count)
	}
}

// --- Benchmark ---

func BenchmarkWAL_Append(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")

	w, err := Open(path, SyncNone)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	entry := &Entry{
		Type:   EntryEnqueue,
		TaskID: "bench-task",
		Data:   []byte(`{"id":"bench","payload":"benchmark data"}`),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.Seq = 0 // reset for re-encoding
		w.Append(entry)
	}
}

func BenchmarkWAL_AppendSync(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")

	w, err := Open(path, SyncEvery)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	entry := &Entry{
		Type:   EntryEnqueue,
		TaskID: "bench-task",
		Data:   []byte(`{"id":"bench","payload":"benchmark data"}`),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.Seq = 0
		w.Append(entry)
	}
}
