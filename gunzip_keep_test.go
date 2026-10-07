// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgzip

import (
	"bytes"
	stdgz "compress/gzip"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

// resetKeepGzip returns the gzip-compressed form of s, built with the
// standard library so the fixture stays tiny and self contained.
func resetKeepGzip(t *testing.T, s string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := stdgz.NewWriter(&buf)
	if _, err := io.WriteString(w, s); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// waitForGoroutines polls until the number of live goroutines drops to at
// most want, or fails the test after a short deadline.
func waitForGoroutines(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if n := runtime.NumGoroutine(); n <= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine count did not settle: got %d, want <= %d", runtime.NumGoroutine(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestResetKeepAcceptance follows the acceptance scenario: read one byte from
// the old stream, offer a bad header (which must be rejected while the old
// stream survives), then offer a valid new stream and verify the switch and
// that the rejected candidate reclaimed its resources.
func TestResetKeepAcceptance(t *testing.T) {
	const oldPayload = "old-stream"
	const newPayload = "new-stream"
	oldGz := resetKeepGzip(t, oldPayload)
	newGz := resetKeepGzip(t, newPayload)
	bad := []byte{0x00, 0x00, 0x00, 0x00, 'n', 'o', 't', 'g', 'z', 'i', 'p'}

	z, err := NewReaderN(bytes.NewReader(oldGz), 1<<10, 2)
	if err != nil {
		t.Fatalf("NewReaderN: %v", err)
	}
	defer z.Close()

	// 1. Read exactly one byte of the old stream first.
	first := make([]byte, 1)
	if n, err := io.ReadFull(z, first); err != nil || n != 1 {
		t.Fatalf("first read: n=%d err=%v", n, err)
	}
	if first[0] != oldPayload[0] {
		t.Fatalf("first byte = %q, want %q", first[0], oldPayload[0])
	}

	// 2. A bad header must be rejected without touching the active reader.
	before := runtime.NumGoroutine()
	if err := z.ResetKeep(bytes.NewReader(bad)); err != ErrHeader {
		t.Fatalf("bad header: got %v, want %v", err, ErrHeader)
	}
	waitForGoroutines(t, before)

	// The old stream must still serve its remaining bytes.
	rest, err := io.ReadAll(z)
	if err != nil {
		t.Fatalf("read old rest: %v", err)
	}
	if got := string(first) + string(rest); got != oldPayload {
		t.Fatalf("old stream = %q, want %q", got, oldPayload)
	}

	// 3. A valid candidate replaces the active reader.
	if err := z.ResetKeep(bytes.NewReader(newGz)); err != nil {
		t.Fatalf("valid reset: %v", err)
	}
	got, err := io.ReadAll(z)
	if err != nil {
		t.Fatalf("read new stream: %v", err)
	}
	if string(got) != newPayload {
		t.Fatalf("new stream = %q, want %q", got, newPayload)
	}
}

// TestResetKeepRejectedCandidateReclaimed checks the internal cleanup path:
// rejected candidates must return all block buffers, stop any background read
// and not keep goroutines alive.
func TestResetKeepRejectedCandidateReclaimed(t *testing.T) {
	const payload = "abcdefgh"
	gz := resetKeepGzip(t, payload)
	bad := []byte{0xde, 0xad, 0xbe, 0xef, 0, 0, 0, 0, 0, 0}

	// Candidate rejected during header parsing must not escape at all.
	c, err := newResetCandidate(bytes.NewReader(bad), 2, 1<<10)
	if err != ErrHeader {
		t.Fatalf("bad candidate: got %v, want %v", err, ErrHeader)
	}
	if c != nil {
		t.Fatalf("rejected candidate leaked: %#v", c)
	}

	// Repeated rejected resets of an active reader must not leak its
	// resources either.
	z, err := NewReaderN(bytes.NewReader(gz), 1<<10, 2)
	if err != nil {
		t.Fatalf("NewReaderN: %v", err)
	}
	defer z.Close()
	if _, err := io.ReadFull(z, make([]byte, 1)); err != nil {
		t.Fatalf("prime read: %v", err)
	}
	// Snapshot the active plumbing; a rejected candidate must not replace
	// any of it. bytes.Reader implements flate.Reader, so makeReader passes
	// it through unchanged and z.r is the reader itself.
	oldR, oldDecomp := z.r, z.decompressor
	oldRA, oldPool := z.readAhead, z.blockPool
	baseline := runtime.NumGoroutine()
	for range 50 {
		if err := z.ResetKeep(bytes.NewReader(bad)); err != ErrHeader {
			t.Fatalf("got %v, want %v", err, ErrHeader)
		}
	}
	waitForGoroutines(t, baseline)
	if z.r != oldR || z.decompressor != oldDecomp || z.readAhead != oldRA || z.blockPool != oldPool {
		t.Fatal("rejected reset replaced active reader state")
	}
	if _, err := io.Copy(io.Discard, z); err != nil {
		t.Fatalf("old stream after rejected resets: %v", err)
	}

	// Force-start a candidate's read-ahead and make sure discard stops it
	// and reclaims every buffer.
	c2, err := newResetCandidate(bytes.NewReader(gz), 2, 1<<10)
	if err != nil {
		t.Fatalf("valid candidate: %v", err)
	}
	if _, err := io.ReadFull(c2, make([]byte, 1)); err != nil {
		t.Fatalf("candidate read: %v", err)
	}
	if !c2.readAheadStarted.Load() {
		t.Fatal("candidate read-ahead not started")
	}
	before := runtime.NumGoroutine()
	c2.discard()
	if c2.blockPool != nil {
		t.Fatalf("candidate pool retained after discard: %#v", c2.blockPool)
	}
	if c2.closeReader == nil {
		t.Fatal("candidate closeReader missing after discard")
	}
	select {
	case <-c2.closeReader:
	default:
		t.Fatal("candidate background read not stopped after discard")
	}
	waitForGoroutines(t, before-1)
}

// TestResetKeepRejectionMemoryFixture is the small in-memory resource fixture:
// thousands of rejected candidates must not leave any live heap growth behind.
func TestResetKeepRejectionMemoryFixture(t *testing.T) {
	const payload = "fixture-payload"
	gz := resetKeepGzip(t, payload)
	bad := []byte{0x01, 0x02, 0x03, 0x04, 0, 0, 0, 0, 0, 0}

	z, err := NewReaderN(bytes.NewReader(gz), 1<<10, 2)
	if err != nil {
		t.Fatalf("NewReaderN: %v", err)
	}
	defer z.Close()
	if _, err := io.ReadFull(z, make([]byte, 1)); err != nil {
		t.Fatalf("prime read: %v", err)
	}

	debug.SetGCPercent(100)
	runtime.GC()
	var start runtime.MemStats
	runtime.ReadMemStats(&start)

	const iterations = 2000
	for range iterations {
		if err := z.ResetKeep(bytes.NewReader(bad)); err != ErrHeader {
			t.Fatalf("got %v, want %v", err, ErrHeader)
		}
	}

	runtime.GC()
	var end runtime.MemStats
	runtime.ReadMemStats(&end)
	growth := int64(end.HeapAlloc) - int64(start.HeapAlloc)
	t.Logf("%d rejected candidates: live heap growth %d bytes (pool %d blocks x %d bytes each)",
		iterations, growth, 2, 1<<10)
	// A single leaked pool would be ~2 KiB; allow generous runtime noise but
	// require that candidates were reclaimed (2000 leaks would be ~4 MiB).
	if growth > 2<<20 {
		t.Fatalf("rejected candidates retained %d bytes of live heap", growth)
	}
	if _, err := io.Copy(io.Discard, z); err != nil {
		t.Fatalf("old stream after fixture: %v", err)
	}
}

// ExampleReader_ResetKeep is the short end-to-end regression example for the
// non-destructive reset adapter.
func ExampleReader_ResetKeep() {
	gzipString := func(s string) []byte {
		var buf bytes.Buffer
		w := stdgz.NewWriter(&buf)
		io.WriteString(w, s)
		w.Close()
		return buf.Bytes()
	}

	z, err := NewReader(bytes.NewReader(gzipString("old-stream")))
	if err != nil {
		panic(err)
	}

	// Consume one byte of the active stream.
	p := make([]byte, 1)
	z.Read(p)
	fmt.Printf("old first byte: %s\n", p)

	// A bad candidate header is rejected; the old stream keeps working.
	bad := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if err := z.ResetKeep(bytes.NewReader(bad)); err != nil {
		fmt.Printf("bad candidate: %v\n", err)
	}
	rest, _ := io.ReadAll(z)
	fmt.Printf("old remainder: %s\n", rest)

	// A valid candidate replaces the active reader.
	if err := z.ResetKeep(bytes.NewReader(gzipString("new-stream"))); err != nil {
		panic(err)
	}
	out, _ := io.ReadAll(z)
	fmt.Printf("switched stream: %s\n", out)
	// Output:
	// old first byte: o
	// bad candidate: gzip: invalid header
	// old remainder: ld-stream
	// switched stream: new-stream
}

// ExampleReader_Reset pins the original Reset behavior as a short regression.
func ExampleReader_Reset() {
	gzipString := func(s string) []byte {
		var buf bytes.Buffer
		w := stdgz.NewWriter(&buf)
		io.WriteString(w, s)
		w.Close()
		return buf.Bytes()
	}

	var z Reader
	if err := z.Reset(bytes.NewReader(gzipString("first"))); err != nil {
		panic(err)
	}
	first, _ := io.ReadAll(&z)
	fmt.Printf("first: %s\n", first)

	// Default Reset tears down the old stream and validates the new one;
	// a valid switch makes the reused reader serve the new stream.
	if err := z.Reset(bytes.NewReader(gzipString("second"))); err != nil {
		panic(err)
	}
	second, _ := io.ReadAll(&z)
	fmt.Printf("second: %s\n", second)
	// Output:
	// first: first
	// second: second
}
