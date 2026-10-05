// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgzip

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"testing"
)

// TestManifestTwoMembers verifies that a concatenated gzip stream with two
// members is reported as two members, not one, and that both are complete
// with a valid CRC.
func TestManifestTwoMembers(t *testing.T) {
	data := []byte("hello world\n")
	var buf bytes.Buffer
	for range 2 {
		w := NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{}, data...), data...)
	if !bytes.Equal(out, want) {
		t.Fatalf("output = %q, want %q", out, want)
	}

	m := r.Manifest()
	if len(m.Members) != 2 {
		t.Fatalf("got %d members, want 2", len(m.Members))
	}
	for i, mem := range m.Members {
		if !mem.Complete {
			t.Errorf("member %d: not complete", i)
		}
		if !mem.CRCValid {
			t.Errorf("member %d: CRC invalid", i)
		}
		if mem.UncompressedSize != int64(len(data)) {
			t.Errorf("member %d: size = %d, want %d", i, mem.UncompressedSize, len(data))
		}
		if mem.CompressedEnd <= mem.CompressedStart {
			t.Errorf("member %d: invalid range [%d,%d)", i, mem.CompressedStart, mem.CompressedEnd)
		}
	}
	if m.Members[0].CompressedStart != 0 {
		t.Errorf("member 0: start = %d, want 0", m.Members[0].CompressedStart)
	}
	if m.Members[0].CompressedEnd != m.Members[1].CompressedStart {
		t.Errorf("member 0 end %d != member 1 start %d", m.Members[0].CompressedEnd, m.Members[1].CompressedStart)
	}
	if m.Trailing != nil {
		t.Errorf("unexpected trailing data: %+v", m.Trailing)
	}
}

// TestManifestBadCRCSecond verifies that a CRC failure in the second member
// only marks the second member as failed; the first member remains complete
// and auditable.
func TestManifestBadCRCSecond(t *testing.T) {
	data := []byte("hello world\n")
	var buf bytes.Buffer
	for range 2 {
		w := NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Corrupt the CRC of the second member (the first 4 bytes of its
	// trailer, which is the last 8 bytes of the stream).
	b := buf.Bytes()
	b[len(b)-8] ^= 0xff

	r, err := NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	out, err := io.ReadAll(r)
	if err != ErrChecksum {
		t.Fatalf("got err = %v, want ErrChecksum", err)
	}
	want := append(append([]byte{}, data...), data...)
	if !bytes.Equal(out, want) {
		t.Fatalf("output = %q, want %q", out, want)
	}

	m := r.Manifest()
	if len(m.Members) != 2 {
		t.Fatalf("got %d members, want 2", len(m.Members))
	}
	if !m.Members[0].Complete || !m.Members[0].CRCValid {
		t.Errorf("member 0: complete = %v, crc = %v, want true/true", m.Members[0].Complete, m.Members[0].CRCValid)
	}
	if !m.Members[1].Complete {
		t.Errorf("member 1: complete = %v, want true", m.Members[1].Complete)
	}
	if m.Members[1].CRCValid {
		t.Errorf("member 1: CRC valid, want invalid")
	}
	if m.Members[1].UncompressedSize != int64(len(data)) {
		t.Errorf("member 1: size = %d, want %d", m.Members[1].UncompressedSize, len(data))
	}
}

// TestManifestTrailing verifies that trailing non-member data after the last
// gzip member is reported separately and does not produce a phantom member.
func TestManifestTrailing(t *testing.T) {
	data := []byte("hello world\n")
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	buf.WriteString("garbage!!!") // 10 bytes of trailing non-member data

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	out, err := io.ReadAll(r)
	if err != ErrHeader {
		t.Fatalf("got err = %v, want ErrHeader", err)
	}
	if !bytes.Equal(out, data) {
		t.Fatalf("output = %q, want %q", out, data)
	}

	m := r.Manifest()
	if len(m.Members) != 1 {
		t.Fatalf("got %d members, want 1", len(m.Members))
	}
	if !m.Members[0].Complete || !m.Members[0].CRCValid {
		t.Errorf("member 0: complete = %v, crc = %v, want true/true", m.Members[0].Complete, m.Members[0].CRCValid)
	}
	if m.Trailing == nil {
		t.Fatal("expected trailing data, got nil")
	}
	if m.Trailing.Start != m.Members[0].CompressedEnd {
		t.Errorf("trailing start %d != member end %d", m.Trailing.Start, m.Members[0].CompressedEnd)
	}
	if m.Trailing.End != int64(len(buf.Bytes())) {
		t.Errorf("trailing end %d, want %d", m.Trailing.End, len(buf.Bytes()))
	}
}

// TestManifestTrailingLong verifies that trailing data longer than the 10-byte
// gzip header probe is fully reported (the manifest drains the reader).
func TestManifestTrailingLong(t *testing.T) {
	data := []byte("hello world\n")
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	buf.WriteString("this is a longer trailing non-member payload")

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := io.ReadAll(r); err != ErrHeader {
		t.Fatalf("got err = %v, want ErrHeader", err)
	}

	m := r.Manifest()
	if len(m.Members) != 1 {
		t.Fatalf("got %d members, want 1", len(m.Members))
	}
	if m.Trailing == nil {
		t.Fatal("expected trailing data, got nil")
	}
	if m.Trailing.End != int64(len(buf.Bytes())) {
		t.Errorf("trailing end %d, want %d", m.Trailing.End, len(buf.Bytes()))
	}
}

// TestManifestTruncated verifies that a truncated member is reported as
// incomplete while members preceding it remain complete and auditable.
func TestManifestTruncated(t *testing.T) {
	data := []byte("hello world\n")
	var buf bytes.Buffer
	for range 2 {
		w := NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Truncate the stream partway through the second member.
	b := buf.Bytes()
	truncated := b[:len(b)-5]

	r, err := NewReader(bytes.NewReader(truncated))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = io.ReadAll(r)
	if err == nil {
		t.Fatal("expected error for truncated stream, got nil")
	}

	m := r.Manifest()
	if len(m.Members) != 2 {
		t.Fatalf("got %d members, want 2", len(m.Members))
	}
	// The first member is complete and auditable.
	if !m.Members[0].Complete || !m.Members[0].CRCValid {
		t.Errorf("member 0: complete = %v, crc = %v, want true/true", m.Members[0].Complete, m.Members[0].CRCValid)
	}
	// The second member is truncated: incomplete, CRC not verified.
	if m.Members[1].Complete {
		t.Errorf("member 1: complete = true, want false")
	}
	if m.Members[1].CRCValid {
		t.Errorf("member 1: CRC valid, want false")
	}
	if m.Members[1].CompressedEnd != 0 {
		t.Errorf("member 1: compressed end = %d, want 0 (unknown)", m.Members[1].CompressedEnd)
	}
}

// TestManifestReset verifies that Reset clears the manifest state.
func TestManifestReset(t *testing.T) {
	data := []byte("hello world\n")
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var r Reader
	if err := r.Reset(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, &r); err != nil {
		t.Fatal(err)
	}
	m := r.Manifest()
	if len(m.Members) != 1 {
		t.Fatalf("got %d members, want 1", len(m.Members))
	}

	// Reset with a fresh stream; the manifest should reflect only the new stream.
	if err := r.Reset(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, &r); err != nil {
		t.Fatal(err)
	}
	m = r.Manifest()
	if len(m.Members) != 1 {
		t.Fatalf("after reset: got %d members, want 1", len(m.Members))
	}
}

// ExampleReader_Manifest demonstrates auditing a concatenated gzip stream
// with two members. Each member is reported separately with its compressed
// range, decompressed length, and CRC verification result.
func ExampleReader_Manifest() {
	// Build a concatenated gzip stream with two members.
	var buf bytes.Buffer
	for range 2 {
		w := NewWriter(&buf)
		if _, err := w.Write([]byte("hello")); err != nil {
			log.Fatal(err)
		}
		if err := w.Close(); err != nil {
			log.Fatal(err)
		}
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	// Read the concatenated output.
	out, err := io.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("output:", string(out))

	// Audit the manifest: two members, each complete with a valid CRC.
	m := r.Manifest()
	fmt.Println("members:", len(m.Members))
	for i, mem := range m.Members {
		fmt.Printf("member %d: bytes [%d,%d) size=%d crc_valid=%v complete=%v\n",
			i, mem.CompressedStart, mem.CompressedEnd, mem.UncompressedSize, mem.CRCValid, mem.Complete)
	}

	// Output:
	// output: hellohello
	// members: 2
	// member 0: bytes [0,35) size=5 crc_valid=true complete=true
	// member 1: bytes [35,70) size=5 crc_valid=true complete=true
}

// TestManifestEmptyMember verifies that an empty gzip member is reported as
// complete with a valid CRC and zero decompressed length.
func TestManifestEmptyMember(t *testing.T) {
	// gzip of empty data (echo -n | gzip -c).
	emptyGzip := []byte{
		0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
		0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	r, err := NewReader(bytes.NewReader(emptyGzip))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
	m := r.Manifest()
	if len(m.Members) != 1 {
		t.Fatalf("got %d members, want 1", len(m.Members))
	}
	mem := m.Members[0]
	if !mem.Complete || !mem.CRCValid {
		t.Errorf("member: complete = %v, crc = %v, want true/true", mem.Complete, mem.CRCValid)
	}
	if mem.UncompressedSize != 0 {
		t.Errorf("member: size = %d, want 0", mem.UncompressedSize)
	}
}

// TestManifestHeaderFields verifies that header fields (name, comment) are
// exposed through the manifest's embedded Header, the same fields exposed by
// the Reader.
func TestManifestHeaderFields(t *testing.T) {
	data := []byte("hello world\n")
	var buf bytes.Buffer
	w, err := NewWriterLevel(&buf, 1)
	if err != nil {
		t.Fatal(err)
	}
	w.Name = "test.txt"
	w.Comment = "a comment"
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
	m := r.Manifest()
	if len(m.Members) != 1 {
		t.Fatalf("got %d members, want 1", len(m.Members))
	}
	mem := m.Members[0]
	if mem.Name != "test.txt" {
		t.Errorf("member name = %q, want %q", mem.Name, "test.txt")
	}
	if mem.Comment != "a comment" {
		t.Errorf("member comment = %q, want %q", mem.Comment, "a comment")
	}
}

// plainReader wraps a byte slice without implementing io.ByteReader, so the
// Reader must buffer through bufio. This exercises the countingReader on the
// bufio path.
type plainReader struct {
	data []byte
	pos  int
}

func (p *plainReader) Read(b []byte) (int, error) {
	if p.pos >= len(p.data) {
		return 0, io.EOF
	}
	n := copy(b, p.data[p.pos:])
	p.pos += n
	return n, nil
}

// TestManifestBufioPath verifies that the manifest is correct when the input
// reader does not implement io.ByteReader (the bufio buffering path).
func TestManifestBufioPath(t *testing.T) {
	data := []byte("hello world\n")
	var buf bytes.Buffer
	for range 2 {
		w := NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}

	r, err := NewReader(&plainReader{data: buf.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{}, data...), data...)
	if !bytes.Equal(out, want) {
		t.Fatalf("output = %q, want %q", out, want)
	}
	m := r.Manifest()
	if len(m.Members) != 2 {
		t.Fatalf("got %d members, want 2", len(m.Members))
	}
	for i, mem := range m.Members {
		if !mem.Complete || !mem.CRCValid {
			t.Errorf("member %d: complete = %v, crc = %v, want true/true", i, mem.Complete, mem.CRCValid)
		}
	}
	if m.Members[0].CompressedStart != 0 {
		t.Errorf("member 0: start = %d, want 0", m.Members[0].CompressedStart)
	}
	if m.Members[0].CompressedEnd != m.Members[1].CompressedStart {
		t.Errorf("member 0 end %d != member 1 start %d", m.Members[0].CompressedEnd, m.Members[1].CompressedStart)
	}
}
