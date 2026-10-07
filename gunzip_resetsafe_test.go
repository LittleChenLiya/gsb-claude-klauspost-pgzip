// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgzip

import (
	"bytes"
	stdgzip "compress/gzip"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"runtime"
	"testing"
	"time"
)

// gzBytes returns the gzip compression of s using the standard library.
// Fixtures stay tiny: every test stream is well under 1 KiB.
func gzBytes(s string) []byte {
	var b bytes.Buffer
	w := stdgzip.NewWriter(&b)
	io.WriteString(w, s)
	if err := w.Close(); err != nil {
		panic(err)
	}
	return b.Bytes()
}

var (
	resetSafeBadHeader = []byte("not a gzip header")
	resetSafeOldStream = "OLD-STREAM-REMAINDER"
	resetSafeNewStream = "NEW-STREAM"
)

// TestResetSafeBadHeaderKeepsOldStream is the acceptance test:
// read one byte from the old stream, hand ResetSafe a bad header and then a
// valid new stream, and check the old remaining bytes, the switch result and
// the reclamation of candidate resources. No large data is used.
func TestResetSafeBadHeaderKeepsOldStream(t *testing.T) {
	z, err := NewReader(bytes.NewReader(gzBytes(resetSafeOldStream)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer z.Close()

	// 1. Read exactly one byte from the old stream.
	one := make([]byte, 1)
	if n, err := z.Read(one); err != nil || n != 1 {
		t.Fatalf("first read = (%d, %v), want (1, nil)", n, err)
	}
	if string(one) != resetSafeOldStream[:1] {
		t.Fatalf("first byte = %q, want %q", one, resetSafeOldStream[:1])
	}

	// Snapshot internal resource pointers so we can prove the active stream
	// survived the rejected attempt.
	oldPool := z.blockPool
	oldDecomp := z.decompressor
	oldDigest := z.digest

	goroutinesBefore := runtime.NumGoroutine()

	// 2. Bad header: the candidate must be rejected and the active reader
	// must be untouched.
	if err := z.ResetSafe(bytes.NewReader(resetSafeBadHeader)); !errors.Is(err, ErrHeader) {
		t.Fatalf("ResetSafe(bad header) err = %v, want ErrHeader", err)
	}
	if z.blockPool != oldPool {
		t.Error("active blockPool was replaced although the candidate header was rejected")
	}
	if z.decompressor != oldDecomp {
		t.Error("active decompressor was replaced although the candidate header was rejected")
	}
	if z.digest != oldDigest {
		t.Error("active digest was replaced although the candidate header was rejected")
	}

	// 3. Old stream still reads: the remainder must be exactly the bytes
	// following the first byte, then io.EOF.
	rest, err := io.ReadAll(z)
	if err != nil {
		t.Fatalf("read old remainder: %v", err)
	}
	if got := string(one) + string(rest); got != resetSafeOldStream {
		t.Fatalf("old stream = %q, want %q", got, resetSafeOldStream)
	}
	if _, err := z.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("old stream after drain err = %v, want io.EOF", err)
	}

	// No background goroutine from the rejected candidate may survive.
	if !waitGoroutinesAtMost(goroutinesBefore, 500*time.Millisecond) {
		t.Errorf("goroutine count after rejected ResetSafe exceeded %d", goroutinesBefore)
	}

	// 4. A valid new stream now switches the active reader.
	if err := z.ResetSafe(bytes.NewReader(gzBytes(resetSafeNewStream))); err != nil {
		t.Fatalf("ResetSafe(valid header): %v", err)
	}
	if z.blockPool == oldPool {
		t.Error("blockPool was not replaced after a successful ResetSafe")
	}
	got, err := io.ReadAll(z)
	if err != nil {
		t.Fatalf("read new stream: %v", err)
	}
	if string(got) != resetSafeNewStream {
		t.Fatalf("new stream = %q, want %q", got, resetSafeNewStream)
	}
}

// TestResetSafeRejectsEmpty confirms an empty (truncated) candidate is
// rejected without disturbing the active stream.
func TestResetSafeRejectsEmpty(t *testing.T) {
	z, err := NewReader(bytes.NewReader(gzBytes(resetSafeOldStream)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer z.Close()

	oldPool := z.blockPool
	if err := z.ResetSafe(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("ResetSafe(empty) err = %v, want io.EOF", err)
	}
	if z.blockPool != oldPool {
		t.Error("active blockPool replaced after empty candidate")
	}
	got, err := io.ReadAll(z)
	if err != nil || string(got) != resetSafeOldStream {
		t.Fatalf("old stream after empty candidate = %q, %v", got, err)
	}
}

// TestResetSafeReclaimsCandidate is a focused resource check on a candidate
// whose background read-ahead has already started when it is rejected: the
// goroutine must be stopped, the decompressor closed and every block buffer
// reclaimed (pool drained).
func TestResetSafeReclaimsCandidate(t *testing.T) {
	cand := new(Reader)
	cand.blocks = 2
	cand.blockSize = 1024
	cand.r = makeReader(bytes.NewReader(gzBytes("candidate")))
	cand.digest = crc32.NewIEEE()
	cand.multistream.Store(true)
	cand.blockPool = make(chan []byte, cand.blocks)
	for i := 0; i < cand.blocks; i++ {
		cand.blockPool <- make([]byte, cand.blockSize)
	}
	if err := cand.readHeader(true); err != nil {
		t.Fatalf("candidate header: %v", err)
	}

	// Start the background read-ahead so reclamation has real work to do.
	if cand.readAheadStarted.CompareAndSwap(false, true) {
		cand.doReadAhead()
	}
	one := make([]byte, 1)
	if _, err := cand.Read(one); err != nil {
		t.Fatalf("candidate read: %v", err)
	}

	before := runtime.NumGoroutine()
	cand.releaseCandidate()
	// The reclaimed read-ahead goroutine must be gone: compare against the
	// count after taking one read (which includes that goroutine).
	if !waitGoroutinesAtMost(before-1, 500*time.Millisecond) {
		t.Errorf("candidate read-ahead goroutine was not reclaimed (count %d)", before)
	}
	if got := len(cand.blockPool); got != 0 {
		t.Errorf("candidate blockPool length = %d, want 0 (buffers reclaimed)", got)
	}
	if cand.decompressor != nil {
		t.Error("candidate decompressor was not closed")
	}
}

// TestDrainBlockPool is the minimal check that the buffer-reclamation helper
// empties the channel it is given.
func TestDrainBlockPool(t *testing.T) {
	const n = 4
	p := make(chan []byte, n)
	for i := 0; i < n; i++ {
		p <- make([]byte, 64)
	}
	if len(p) != n {
		t.Fatalf("setup: pool length = %d, want %d", len(p), n)
	}
	drainBlockPool(p)
	if len(p) != 0 {
		t.Fatalf("after drain pool length = %d, want 0", len(p))
	}
}

// waitGoroutinesAtMost reports whether the live goroutine count drops to at
// most want within the deadline. It yields the runtime to let the killed
// read-ahead goroutine observe its closed signal.
func waitGoroutinesAtMost(want int, deadline time.Duration) bool {
	deadlineDone := time.Now().Add(deadline)
	for time.Now().Before(deadlineDone) {
		if runtime.NumGoroutine() <= want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
		runtime.Gosched()
	}
	return runtime.NumGoroutine() <= want
}

// ExampleReader_Reset is a short regression example for the original Reset
// interface. It pins down the historical behavior: after Reset is handed a
// bad header the old stream has already been discarded, so continuing to
// read from it fails. ResetSafe changes that; this example stays as the
// regression anchor for the original behavior.
func ExampleReader_Reset() {
	z, err := NewReader(bytes.NewReader(gzBytes("OLD-DATA-REMAINDER")))
	if err != nil {
		panic(err)
	}
	defer z.Close()

	one := make([]byte, 1)
	if _, err := z.Read(one); err != nil {
		panic(err)
	}
	fmt.Printf("old first byte: %s\n", one)

	err = z.Reset(bytes.NewReader([]byte("not a gzip header")))
	fmt.Printf("Reset bad header: %v\n", err)

	// The original Reset semantics destroy the old state even though the new
	// header is invalid: subsequent reads cannot recover it.
	rest, err := io.ReadAll(z)
	fmt.Printf("old stream after bad Reset: %q (%v)\n", rest, err)
	// Output:
	// old first byte: O
	// Reset bad header: gzip: invalid header
	// old stream after bad Reset: "" (unexpected EOF)
}

// ExampleReader_ResetSafe shows the adapter wired into the same data flow:
// a rejected candidate leaves the active stream readable, and a later valid
// candidate performs the switch.
func ExampleReader_ResetSafe() {
	z, err := NewReader(bytes.NewReader(gzBytes("OLD-STREAM")))
	if err != nil {
		panic(err)
	}
	defer z.Close()

	one := make([]byte, 1)
	if _, err := z.Read(one); err != nil {
		panic(err)
	}

	err = z.ResetSafe(bytes.NewReader([]byte("not a gzip header")))
	fmt.Printf("bad candidate: %v\n", err)

	rest, err := io.ReadAll(z)
	if err != nil {
		panic(err)
	}
	fmt.Printf("old stream still readable: %s\n", string(one)+string(rest))

	if err := z.ResetSafe(bytes.NewReader(gzBytes("NEW-STREAM"))); err != nil {
		panic(err)
	}
	switched, err := io.ReadAll(z)
	if err != nil {
		panic(err)
	}
	fmt.Printf("after valid candidate: %s\n", switched)
	// Output:
	// bad candidate: gzip: invalid header
	// old stream still readable: OLD-STREAM
	// after valid candidate: NEW-STREAM
}
