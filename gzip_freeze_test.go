// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgzip

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// readGzipHeader decompresses buf and returns the decoded header and payload.
func readGzipHeader(t *testing.T, buf *bytes.Buffer) (Header, string) {
	t.Helper()
	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	payload, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Reader.Close: %v", err)
	}
	return r.Header, string(payload)
}

// TestFreezeHeaderSnapshot verifies that after FreezeHeader, mutating
// Name/Extra/Comment/ModTime does not change what gets written.
func TestFreezeHeaderSnapshot(t *testing.T) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)

	w.Name = "frozen-name"
	w.Comment = "frozen-comment"
	w.Extra = []byte("frozen-extra")
	w.ModTime = time.Unix(123456789, 0)
	if err := w.FreezeHeader(); err != nil {
		t.Fatalf("FreezeHeader: %v", err)
	}

	// Mutate everything after freezing, before the first byte is written.
	w.Name = "changed-name"
	w.Comment = "changed-comment"
	w.Extra = []byte("changed-extra")
	w.ModTime = time.Unix(987654321, 0)

	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	h, payload := readGzipHeader(t, buf)
	if payload != "payload" {
		t.Fatalf("payload = %q, want %q", payload, "payload")
	}
	if h.Name != "frozen-name" {
		t.Fatalf("Name = %q, want %q", h.Name, "frozen-name")
	}
	if h.Comment != "frozen-comment" {
		t.Fatalf("Comment = %q, want %q", h.Comment, "frozen-comment")
	}
	if string(h.Extra) != "frozen-extra" {
		t.Fatalf("Extra = %q, want %q", h.Extra, "frozen-extra")
	}
	if h.ModTime.Unix() != 123456789 {
		t.Fatalf("ModTime = %d, want %d", h.ModTime.Unix(), 123456789)
	}
}

// TestFreezeHeaderBackingArray checks that mutating the backing array of
// the slice passed to Extra after freezing has no effect on output.
func TestFreezeHeaderBackingArray(t *testing.T) {
	extra := []byte("abcdef")
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	w.Name = "n"
	w.Extra = extra
	if err := w.FreezeHeader(); err != nil {
		t.Fatalf("FreezeHeader: %v", err)
	}
	for i := range extra {
		extra[i] = 0xff
	}
	if _, err := w.Write([]byte("hi")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h, _ := readGzipHeader(t, buf)
	if string(h.Extra) != "abcdef" {
		t.Fatalf("Extra = %q, want abcdef (frozen copy must be independent)", h.Extra)
	}
}

// TestFreezeHeaderReset verifies freeze/Reset correspondence: Reset clears
// the frozen snapshot, and the Header can be configured and frozen again.
func TestFreezeHeaderReset(t *testing.T) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	w.Name = "first"
	w.Extra = []byte("extra-1")
	w.ModTime = time.Unix(111, 0)
	if err := w.FreezeHeader(); err != nil {
		t.Fatalf("FreezeHeader #1: %v", err)
	}
	w.Name = "ignored"
	if _, err := w.Write([]byte("one")); err != nil {
		t.Fatalf("Write #1: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	h, _ := readGzipHeader(t, buf)
	if h.Name != "first" || string(h.Extra) != "extra-1" || h.ModTime.Unix() != 111 {
		t.Fatalf("first stream header = %+v", h)
	}

	// Reset: the old frozen snapshot must be gone.
	buf2 := new(bytes.Buffer)
	w.Reset(buf2)
	w.Name = "second"
	w.Extra = []byte("extra-2")
	w.ModTime = time.Unix(222, 0)
	if err := w.FreezeHeader(); err != nil {
		t.Fatalf("FreezeHeader #2 after Reset: %v", err)
	}
	w.Name = "also-ignored"
	if _, err := w.Write([]byte("two")); err != nil {
		t.Fatalf("Write #2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close #2: %v", err)
	}
	h2, payload2 := readGzipHeader(t, buf2)
	if payload2 != "two" {
		t.Fatalf("second payload = %q", payload2)
	}
	if h2.Name != "second" || string(h2.Extra) != "extra-2" || h2.ModTime.Unix() != 222 {
		t.Fatalf("second stream header = %+v, want Name=second Extra=extra-2 ModTime=222", h2)
	}
}

// TestFreezeHeaderInvalid ensures invalid headers are rejected before any
// output byte is produced, and that the writer recovers after Reset.
func TestFreezeHeaderInvalid(t *testing.T) {
	cases := []struct {
		name   string
		modify func(h *Header)
	}{
		{"NameNUL", func(h *Header) { h.Name = "bad\x00name" }},
		{"NameNonLatin1", func(h *Header) { h.Name = "日本語" }},
		{"CommentNUL", func(h *Header) { h.Comment = "bad\x00comment" }},
		{"CommentNonLatin1", func(h *Header) { h.Comment = "café—wait" }},
		{"ExtraTooLarge", func(h *Header) { h.Extra = make([]byte, 0x10000) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			w := NewWriter(buf)
			w.Name = "ok"
			tc.modify(&w.Header)

			if err := w.FreezeHeader(); err == nil {
				t.Fatalf("FreezeHeader succeeded for %s, want error", tc.name)
			}
			// No byte may have reached the output before the failure.
			if buf.Len() != 0 {
				t.Fatalf("%s: %d bytes written before error, want 0", tc.name, buf.Len())
			}
			// Subsequent operations must keep failing and emit nothing.
			if n, err := w.Write([]byte("data")); err == nil {
				t.Fatalf("%s: Write after invalid freeze succeeded, n=%d", tc.name, n)
			}
			if err := w.Flush(); err == nil {
				t.Fatalf("%s: Flush after invalid freeze succeeded", tc.name)
			}
			if err := w.Close(); err == nil {
				t.Fatalf("%s: Close after invalid freeze succeeded", tc.name)
			}
			if buf.Len() != 0 {
				t.Fatalf("%s: output length = %d, want 0", tc.name, buf.Len())
			}

			// Reset clears the error state; a valid header works again.
			buf.Reset()
			w.Reset(buf)
			w.Name = "recovered"
			if err := w.FreezeHeader(); err != nil {
				t.Fatalf("%s: FreezeHeader after Reset: %v", tc.name, err)
			}
			if _, err := w.Write([]byte("ok")); err != nil {
				t.Fatalf("%s: Write after Reset: %v", tc.name, err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("%s: Close after Reset: %v", tc.name, err)
			}
			h, payload := readGzipHeader(t, buf)
			if h.Name != "recovered" || payload != "ok" {
				t.Fatalf("%s: after Reset got Name=%q payload=%q", tc.name, h.Name, payload)
			}
		})
	}
}

// TestFreezeHeaderLate verifies that freezing after the header has been
// written, or after Close, is rejected.
func TestFreezeHeaderLate(t *testing.T) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	if _, err := w.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.FreezeHeader(); err == nil {
		t.Fatal("FreezeHeader after Write succeeded, want error")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.FreezeHeader(); err == nil {
		t.Fatal("FreezeHeader after Close succeeded, want error")
	}
}

// TestUnfrozenKeepsSemantics ensures that without FreezeHeader the writer
// still reads Header fields lazily at first write (pre-existing behavior).
func TestUnfrozenKeepsSemantics(t *testing.T) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	w.Name = "lazy"
	w.Extra = []byte("lazy-extra")
	// Without calling FreezeHeader, the last assignment must be what
	// shows up on the wire.
	w.Name = "lazy-final"
	w.Extra = []byte("lazy-extra-final")
	if _, err := w.Write([]byte("p")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h, _ := readGzipHeader(t, buf)
	if h.Name != "lazy-final" || string(h.Extra) != "lazy-extra-final" {
		t.Fatalf("unfrozen header = %+v, want lazy-final/lazy-extra-final", h)
	}
}

// TestFreezeHeaderFileFixture demonstrates the feature end to end using a
// small temporary-file fixture (in t.TempDir(), no extra disk footprint).
func TestFreezeHeaderFileFixture(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "frozen.gz")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	w := NewWriter(f)
	w.Name = "archived.txt"
	w.Extra = []byte("EXTRA")
	w.ModTime = time.Unix(1700000000, 0)
	if err := w.FreezeHeader(); err != nil {
		t.Fatalf("FreezeHeader: %v", err)
	}
	// Post-freeze tampering must not reach the file.
	w.Name = "tampered.txt"
	w.Extra = []byte("TAMPERED")
	if _, err := w.Write([]byte("file fixture payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close file: %v", err)
	}

	// No bytes are emitted for invalid headers, even straight to a file.
	badPath := filepath.Join(dir, "bad.gz")
	bf, err := os.Create(badPath)
	if err != nil {
		t.Fatalf("Create bad: %v", err)
	}
	bw := NewWriter(bf)
	bw.Name = "bad\x00"
	if freezeErr := bw.FreezeHeader(); freezeErr == nil {
		t.Fatal("FreezeHeader invalid file header succeeded")
	}
	if _, werr := bw.Write([]byte("nope")); werr == nil {
		t.Fatal("Write to file after invalid freeze succeeded")
	}
	if cerr := bw.Close(); cerr == nil {
		t.Fatal("Close after invalid freeze succeeded")
	}
	if err := bf.Close(); err != nil {
		t.Fatalf("Close bad file: %v", err)
	}
	fi, err := os.Stat(badPath)
	if err != nil {
		t.Fatalf("Stat bad file: %v", err)
	}
	if fi.Size() != 0 {
		t.Fatalf("invalid header wrote %d bytes to file, want 0", fi.Size())
	}

	// Read the valid file back and assert the frozen header survived.
	rf, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rf.Close()
	r, err := NewReader(rf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	payload, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(payload) != "file fixture payload" {
		t.Fatalf("file payload = %q", payload)
	}
	if r.Name != "archived.txt" || string(r.Extra) != "EXTRA" || r.ModTime.Unix() != 1700000000 {
		t.Fatalf("file header = %q %q %d, want archived.txt EXTRA 1700000000",
			r.Name, r.Extra, r.ModTime.Unix())
	}
}
