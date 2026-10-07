package pgzip

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// readBackHeader decompresses gz and returns the parsed header and payload.
func readBackHeader(t *testing.T, gz []byte) (Header, []byte) {
	t.Helper()
	r, err := NewReader(bytes.NewReader(gz))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Reader.Close: %v", err)
	}
	return r.Header, data
}

// TestFreezeHeaderSnapshot verifies that after FreezeHeader, modifying the
// Writer.Header fields (including the Extra slice contents) does not affect
// the header that is written out.
func TestFreezeHeaderSnapshot(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	modTime := time.Date(2020, 3, 1, 12, 0, 0, 0, time.UTC)
	w.Name = "frozen-name"
	w.Comment = "frozen-comment"
	w.Extra = []byte("frozen-extra")
	w.ModTime = modTime
	if err := w.FreezeHeader(); err != nil {
		t.Fatalf("FreezeHeader: %v", err)
	}

	// Mutate all header fields after freezing.
	w.Name = "changed-name"
	w.Comment = "changed-comment"
	w.Extra[0] = 'X' // mutate the frozen Extra backing array
	w.Extra = []byte("changed-extra")
	w.ModTime = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	w.OS = 3

	payload := []byte("hello frozen header")
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	hdr, data := readBackHeader(t, buf.Bytes())
	if string(data) != string(payload) {
		t.Errorf("payload = %q, want %q", data, payload)
	}
	if hdr.Name != "frozen-name" {
		t.Errorf("Name = %q, want %q", hdr.Name, "frozen-name")
	}
	if hdr.Comment != "frozen-comment" {
		t.Errorf("Comment = %q, want %q", hdr.Comment, "frozen-comment")
	}
	if string(hdr.Extra) != "frozen-extra" {
		t.Errorf("Extra = %q, want %q", hdr.Extra, "frozen-extra")
	}
	if !hdr.ModTime.Equal(modTime) {
		t.Errorf("ModTime = %v, want %v", hdr.ModTime, modTime)
	}
	if hdr.OS != 255 {
		t.Errorf("OS = %d, want %d", hdr.OS, 255)
	}
}

// TestFreezeHeaderReset verifies that Reset clears the frozen snapshot and
// that a new snapshot can be frozen afterwards.
func TestFreezeHeaderReset(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Name = "first"
	if err := w.FreezeHeader(); err != nil {
		t.Fatalf("FreezeHeader: %v", err)
	}
	if _, err := w.Write([]byte("one")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hdr, _ := readBackHeader(t, buf.Bytes())
	if hdr.Name != "first" {
		t.Errorf("first Name = %q, want %q", hdr.Name, "first")
	}

	// Reset clears the frozen snapshot: without a new freeze the live
	// Header fields are used again (original semantics).
	buf.Reset()
	w.Reset(&buf)
	w.Name = "second-live"
	if _, err := w.Write([]byte("two")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hdr, _ = readBackHeader(t, buf.Bytes())
	if hdr.Name != "second-live" {
		t.Errorf("after Reset Name = %q, want %q", hdr.Name, "second-live")
	}

	// Freeze again after Reset.
	buf.Reset()
	w.Reset(&buf)
	w.Name = "third"
	if err := w.FreezeHeader(); err != nil {
		t.Fatalf("FreezeHeader after Reset: %v", err)
	}
	w.Name = "third-changed"
	if _, err := w.Write([]byte("three")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hdr, _ = readBackHeader(t, buf.Bytes())
	if hdr.Name != "third" {
		t.Errorf("re-frozen Name = %q, want %q", hdr.Name, "third")
	}
}

// TestFreezeHeaderInvalid verifies that an invalid header fails at
// FreezeHeader time, before any byte has been written to the output.
func TestFreezeHeaderInvalid(t *testing.T) {
	cases := []struct {
		name  string
		setup func(w *Writer)
	}{
		{"NUL in Name", func(w *Writer) { w.Name = "bad\x00name" }},
		{"non-Latin-1 Name", func(w *Writer) { w.Name = "日本語" }},
		{"non-Latin-1 Comment", func(w *Writer) { w.Comment = "€" }},
		{"Extra too large", func(w *Writer) { w.Extra = make([]byte, 0x10000) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			c.setup(w)
			if err := w.FreezeHeader(); err == nil {
				t.Fatal("FreezeHeader: expected error, got nil")
			}
			if buf.Len() != 0 {
				t.Fatalf("output length = %d, want 0 (no bytes may be written)", buf.Len())
			}
		})
	}
}

// TestFreezeHeaderAfterWrite verifies that freezing after the header has
// been written is rejected.
func TestFreezeHeaderAfterWrite(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write([]byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.FreezeHeader(); err == nil {
		t.Fatal("FreezeHeader after Write: expected error, got nil")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestUnfrozenSemanticsUnchanged verifies that without FreezeHeader the
// original lazy behavior is kept: header fields are read on first Write,
// and an invalid header is only reported then.
func TestUnfrozenSemanticsUnchanged(t *testing.T) {
	// Valid header set just before the first write still applies.
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Name = "late-name"
	if _, err := w.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hdr, _ := readBackHeader(t, buf.Bytes())
	if hdr.Name != "late-name" {
		t.Errorf("Name = %q, want %q", hdr.Name, "late-name")
	}

	// Invalid header without freeze: error surfaces on Write, as before.
	buf.Reset()
	w = NewWriter(&buf)
	w.Name = "bad\x00name"
	if _, err := w.Write([]byte("x")); err == nil {
		t.Fatal("Write with invalid unfrozen header: expected error, got nil")
	}
}

// TestFreezeHeaderTempFile demonstrates the feature with a small temporary
// file fixture: freeze, mutate, write to disk, read back and verify.
func TestFreezeHeaderTempFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frozen.gz")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	w := NewWriter(f)
	w.Name = "on-disk-name"
	w.Extra = []byte{0xde, 0xad, 0xbe, 0xef}
	if err := w.FreezeHeader(); err != nil {
		t.Fatalf("FreezeHeader: %v", err)
	}
	w.Name = "ignored"
	w.Extra = nil
	if _, err := w.Write([]byte("file payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("file Close: %v", err)
	}

	rf, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rf.Close()
	r, err := NewReader(rf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != "file payload" {
		t.Errorf("payload = %q, want %q", data, "file payload")
	}
	if r.Name != "on-disk-name" {
		t.Errorf("Name = %q, want %q", r.Name, "on-disk-name")
	}
	if !bytes.Equal(r.Extra, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("Extra = %x, want %x", r.Extra, []byte{0xde, 0xad, 0xbe, 0xef})
	}
}
