package pgzip

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a bytes.Buffer that is safe for concurrent use.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.buf.Bytes()...)
}

func (s *syncBuffer) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Len()
}

// fakeCompressor is a test hook for Writer.compressBlockHook that lets the
// test control when each block finishes compressing and whether it fails.
//
// Blocks are identified by their input: block i must be filled with bytes
// of value byte(i+1). The empty final block (produced by Close) passes
// through ungated.
type fakeCompressor struct {
	mu        sync.Mutex
	gates     map[int]chan struct{} // block identity -> release channel
	errs      map[int]error         // block identity -> injected error
	completed chan int              // receives the identity of completed blocks
}

func newFakeCompressor() *fakeCompressor {
	return &fakeCompressor{
		gates:     make(map[int]chan struct{}),
		errs:      make(map[int]error),
		completed: make(chan int, 16),
	}
}

// gate returns (creating if needed) the release channel for a block.
func (f *fakeCompressor) gate(id int) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.gates[id]
	if !ok {
		g = make(chan struct{})
		f.gates[id] = g
	}
	return g
}

// release allows the block with the given identity to finish compressing.
func (f *fakeCompressor) release(id int) { close(f.gate(id)) }

// fail makes the block with the given identity return err.
func (f *fakeCompressor) fail(id int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs[id] = err
}

// hook is the compressBlockHook implementation.
func (f *fakeCompressor) hook(p, prevTail []byte, closed bool) ([]byte, error) {
	if len(p) == 0 {
		// Empty final block from Close: pass through.
		return []byte{}, nil
	}
	id := int(p[0])
	<-f.gate(id)
	f.mu.Lock()
	err := f.errs[id]
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	out := []byte(fmt.Sprintf("block-%d-len-%d;", id, len(p)))
	f.completed <- id
	return out, nil
}

// blockPayload returns a full block filled with the identity byte of block id.
func blockPayload(id, blockSize int) []byte {
	return bytes.Repeat([]byte{byte(id)}, blockSize)
}

// collectReceipts returns a receipt function appending to a slice.
// It must only be read after Close/Flush has returned.
func collectReceipts(dst *[]BlockReceipt) func(BlockReceipt) {
	return func(r BlockReceipt) { *dst = append(*dst, r) }
}

const receiptTestBlockSize = tailSize + 1

// TestBlockReceiptOutOfOrder verifies that even if block 2 finishes
// compressing before block 1, it is not written (and no receipt is
// produced) before block 1, and that receipts preserve each block's input
// identity and output range.
func TestBlockReceiptOutOfOrder(t *testing.T) {
	out := &syncBuffer{}
	fc := newFakeCompressor()

	z := NewWriter(out)
	z.SetConcurrency(receiptTestBlockSize, 4)
	z.compressBlockHook = fc.hook
	var receipts []BlockReceipt
	z.SetBlockReceiptFunc(collectReceipts(&receipts))

	// Three full blocks with identities 1, 2, 3.
	for id := 1; id <= 3; id++ {
		if _, err := z.Write(blockPayload(id, receiptTestBlockSize)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// Block 2 (id 2) finishes first...
	fc.release(2)
	if got := <-fc.completed; got != 2 {
		t.Fatalf("expected block 2 to complete first, got %d", got)
	}
	// ...but it must not be written before block 1: only the 10 byte
	// header may be present, and no receipt may have been delivered.
	time.Sleep(50 * time.Millisecond)
	if n := out.Len(); n != 10 {
		t.Fatalf("block 2 overtook block 1: %d bytes written, want 10 (header only)", n)
	}
	if len(receipts) != 0 {
		t.Fatalf("receipt delivered out of order: %+v", receipts)
	}

	// Now release blocks 1 and 3 and close.
	fc.release(1)
	fc.release(3)
	if err := z.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Receipts: 3 data blocks + the empty final block, in input order.
	if len(receipts) != 4 {
		t.Fatalf("got %d receipts, want 4: %+v", len(receipts), receipts)
	}
	wantInput := []int{receiptTestBlockSize, receiptTestBlockSize, receiptTestBlockSize, 0}
	offset := int64(10) // header size
	for i, r := range receipts {
		if r.Index != i {
			t.Errorf("receipt %d: Index = %d, want %d", i, r.Index, i)
		}
		if r.Input != wantInput[i] {
			t.Errorf("receipt %d: Input = %d, want %d", i, r.Input, wantInput[i])
		}
		if r.Offset != offset {
			t.Errorf("receipt %d: Offset = %d, want %d", i, r.Offset, offset)
		}
		offset += int64(r.Length)
	}
	// The output range of the receipts must cover exactly the block output.
	if got, want := out.Len(), int(offset)+8; got != want { // +8: gzip trailer
		t.Fatalf("output size = %d, want %d", got, want)
	}
	// Blocks must appear in the output in input order.
	body := string(out.Bytes()[10 : out.Len()-8])
	want := "block-1-"
	if body[:len(want)] != want {
		t.Fatalf("output does not start with block 1: %q", body)
	}
	idx2 := bytes.Index([]byte(body), []byte("block-2-"))
	idx3 := bytes.Index([]byte(body), []byte("block-3-"))
	if idx2 < 0 || idx3 < 0 || idx3 < idx2 {
		t.Fatalf("blocks out of order in output: %q", body)
	}
}

// TestBlockReceiptFailureNoOvertake verifies that when block 1 fails, the
// later block 2 is never committed: it is not written to the underlying
// writer and no commit receipt is produced for it. It also verifies that
// Close returns the worker error and still reclaims all workers exactly
// once.
func TestBlockReceiptFailureNoOvertake(t *testing.T) {
	out := &syncBuffer{}
	fc := newFakeCompressor()
	errBoom := errors.New("boom")
	fc.fail(1, errBoom)

	z := NewWriter(out)
	z.SetConcurrency(receiptTestBlockSize, 4)
	z.compressBlockHook = fc.hook
	var receipts []BlockReceipt
	z.SetBlockReceiptFunc(collectReceipts(&receipts))

	for id := 1; id <= 3; id++ {
		z.Write(blockPayload(id, receiptTestBlockSize))
	}
	// Blocks 2 and 3 finish fine, block 1 fails.
	fc.release(2)
	fc.release(3)
	fc.release(1)

	err := z.Close()
	if !errors.Is(err, errBoom) {
		t.Fatalf("Close error = %v, want %v", err, errBoom)
	}

	// No block may have been committed: block 1 failed, and blocks 2 and 3
	// must not overtake it.
	if len(receipts) != 0 {
		t.Fatalf("got commit receipts after failure: %+v", receipts)
	}
	if n := out.Len(); n != 10 {
		t.Fatalf("%d bytes written after failure, want 10 (header only)", n)
	}

	// Workers must have been reclaimed by Close (exactly once).
	waited := make(chan struct{})
	go func() {
		z.wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		t.Fatal("workers were not reclaimed by Close")
	}

	// A second Close must be a no-op and must not panic or block.
	if err := z.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestFlushBoundaryBarrier verifies that Flush waits exactly up to the
// block boundary of its own call: it blocks until all blocks submitted
// before the call are committed, and it does not wait for future,
// unrelated writes.
func TestFlushBoundaryBarrier(t *testing.T) {
	out := &syncBuffer{}
	fc := newFakeCompressor()

	z := NewWriter(out)
	z.SetConcurrency(receiptTestBlockSize, 4)
	z.compressBlockHook = fc.hook
	var receipts []BlockReceipt
	z.SetBlockReceiptFunc(collectReceipts(&receipts))

	// Partial block: stays buffered until Flush.
	if _, err := z.Write(blockPayload(1, 100)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	flushDone := make(chan error, 1)
	go func() { flushDone <- z.Flush() }()

	// Barrier: while block 1 is gated, Flush must not return.
	select {
	case err := <-flushDone:
		t.Fatalf("Flush returned before its block boundary was committed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Release block 1: Flush must now return, and the receipt for block 1
	// must have been delivered before Flush returned.
	fc.release(1)
	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("Flush: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Flush did not return after its boundary block completed")
	}
	if len(receipts) != 1 || receipts[0].Index != 0 || receipts[0].Input != 100 {
		t.Fatalf("receipts after Flush = %+v, want exactly block 0 with 100 input bytes", receipts)
	}

	// Flush must not wait for future, unrelated writes: it already
	// returned, while the next block (written only now) is still gated.
	if _, err := z.Write(blockPayload(2, 100)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	flush2Done := make(chan error, 1)
	go func() { flush2Done <- z.Flush() }()
	select {
	case err := <-flush2Done:
		t.Fatalf("second Flush returned before its own boundary was committed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	fc.release(2)
	select {
	case err := <-flush2Done:
		if err != nil {
			t.Fatalf("second Flush: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second Flush did not return after its boundary block completed")
	}
	if len(receipts) != 2 || receipts[1].Index != 1 {
		t.Fatalf("receipts after second Flush = %+v, want blocks 0 and 1", receipts)
	}

	if err := z.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestBlockReceiptThreeBlocks is a three-block example using the real
// compressor: it verifies that receipts are delivered in input order with
// contiguous output ranges, that Close reclaims the workers, and that the
// resulting stream round-trips.
func TestBlockReceiptThreeBlocks(t *testing.T) {
	out := &syncBuffer{}
	z := NewWriter(out)
	z.SetConcurrency(receiptTestBlockSize, 2)
	var receipts []BlockReceipt
	z.SetBlockReceiptFunc(collectReceipts(&receipts))

	var want []byte
	for id := 1; id <= 3; id++ {
		b := blockPayload(id, receiptTestBlockSize)
		want = append(want, b...)
		if _, err := z.Write(b); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := z.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Three data blocks plus the empty final block.
	if len(receipts) != 4 {
		t.Fatalf("got %d receipts, want 4: %+v", len(receipts), receipts)
	}
	offset := int64(10) // header
	for i, r := range receipts {
		if r.Index != i {
			t.Errorf("receipt %d: Index = %d, want %d", i, r.Index, i)
		}
		if r.Offset != offset {
			t.Errorf("receipt %d: Offset = %d, want %d (non-contiguous output)", i, r.Offset, offset)
		}
		if i < 3 && r.Input != receiptTestBlockSize {
			t.Errorf("receipt %d: Input = %d, want %d", i, r.Input, receiptTestBlockSize)
		}
		offset += int64(r.Length)
	}
	// Receipts cover everything between header and the 8 byte trailer.
	if got, want := int64(out.Len()), offset+8; got != want {
		t.Fatalf("output size = %d, want %d", got, want)
	}

	// Workers were reclaimed by Close.
	waited := make(chan struct{})
	go func() {
		z.wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		t.Fatal("workers were not reclaimed by Close")
	}

	// The stream must round-trip.
	r, err := NewReader(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(want))
	}
}
