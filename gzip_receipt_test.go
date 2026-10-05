package pgzip

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

type fakeCompressor struct {
	dst       io.Writer
	input     []byte
	barriers  map[string]chan struct{}
	errs      map[string]error
	dones     map[string]chan struct{}
	flushed   chan struct{}
	flushOnce sync.Once
	mu        sync.Mutex
}

func (f *fakeCompressor) Write(p []byte) (int, error) {
	f.mu.Lock()
	f.input = append(f.input, p...)
	f.mu.Unlock()
	return f.dst.Write(append([]byte("C"), p...))
}

func (f *fakeCompressor) Flush() error {
	f.mu.Lock()
	input := string(f.input)
	barrier := f.barriers[input]
	err := f.errs[input]
	done := f.dones[input]
	f.mu.Unlock()
	if barrier != nil {
		<-barrier
	}
	if done != nil {
		close(done)
	}
	if f.flushed != nil {
		f.flushOnce.Do(func() { close(f.flushed) })
	}
	return err
}

func (f *fakeCompressor) Close() error { return nil }

func (f *fakeCompressor) ResetDict(dst io.Writer, dict []byte) {
	f.mu.Lock()
	f.dst = dst
	f.input = nil
	f.flushed = make(chan struct{})
	f.flushOnce = sync.Once{}
	f.mu.Unlock()
}

type fakeWriter struct {
	mu     sync.Mutex
	writes [][]byte
	fail   bool
	block  chan struct{}
}

func (w *fakeWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.writes = append(w.writes, append([]byte(nil), p...))
	fail := w.fail
	block := w.block
	w.mu.Unlock()
	if block != nil {
		<-block
	}
	if fail {
		return 0, errors.New("fake writer error")
	}
	return len(p), nil
}

func (w *fakeWriter) snapshotWrites() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([][]byte, len(w.writes))
	for i, b := range w.writes {
		out[i] = append([]byte(nil), b...)
	}
	return out
}

func setupFakeCompressors(z *Writer, barriers map[string]chan struct{}, errs map[string]error, dones map[string]chan struct{}) {
	z.dictFlatePool.New = func() any {
		return &fakeCompressor{
			barriers: barriers,
			errs:     errs,
			dones:    dones,
			flushed:  make(chan struct{}),
		}
	}
}

func blockData(i, blockSize, nBlocks int) []byte {
	start := i * blockSize
	end := start + blockSize
	if end > blockSize*nBlocks {
		end = blockSize * nBlocks
	}
	data := make([]byte, end-start)
	for j := range data {
		data[j] = byte('A' + i)
	}
	return data
}

func inputKey(data []byte) string { return string(data) }

func collectReceipts(t *testing.T, ch <-chan BlockReceipt, n int) []BlockReceipt {
	t.Helper()
	receipts := make([]BlockReceipt, 0, n)
	for i := 0; i < n; i++ {
		select {
		case r, ok := <-ch:
			if !ok {
				t.Fatalf("receipt channel closed after %d receipts, want %d", i, n)
			}
			receipts = append(receipts, r)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for receipt %d of %d", i+1, n)
		}
	}
	return receipts
}

func TestBlockReceiptsThreeBlocks(t *testing.T) {
	const blockSize = 20000
	const nBlocks = 3

	fw := &fakeWriter{}
	z := NewWriter(fw)
	if err := z.SetConcurrency(blockSize, 4); err != nil {
		t.Fatal(err)
	}

	barriers := make(map[string]chan struct{})
	dones := make(map[string]chan struct{})
	for i := 0; i < nBlocks; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		barriers[key] = make(chan struct{})
		dones[key] = make(chan struct{})
	}
	setupFakeCompressors(z, barriers, nil, dones)
	receiptsCh := z.BlockReceipts()

	for i := 0; i < nBlocks; i++ {
		if _, err := z.Write(blockData(i, blockSize, nBlocks)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < nBlocks; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		close(barriers[key])
	}

	if err := z.Flush(); err != nil {
		t.Fatal(err)
	}

	receipts := collectReceipts(t, receiptsCh, nBlocks)

	for i := 0; i < nBlocks; i++ {
		wantInput := blockData(i, blockSize, nBlocks)
		if receipts[i].Seq != uint64(i) {
			t.Errorf("receipt %d: Seq = %d, want %d", i, receipts[i].Seq, i)
		}
		if receipts[i].InputOffset != int64(i*blockSize) {
			t.Errorf("receipt %d: InputOffset = %d, want %d", i, receipts[i].InputOffset, i*blockSize)
		}
		if receipts[i].InputLen != len(wantInput) {
			t.Errorf("receipt %d: InputLen = %d, want %d", i, receipts[i].InputLen, len(wantInput))
		}
		if receipts[i].Err != nil {
			t.Errorf("receipt %d: unexpected error %v", i, receipts[i].Err)
		}
	}

	headerLen := 10
	wantOutputOffset := int64(headerLen)
	for i := 0; i < nBlocks; i++ {
		wantLen := 1 + len(blockData(i, blockSize, nBlocks))
		if receipts[i].OutputOffset != wantOutputOffset {
			t.Errorf("receipt %d: OutputOffset = %d, want %d", i, receipts[i].OutputOffset, wantOutputOffset)
		}
		if receipts[i].OutputLen != wantLen {
			t.Errorf("receipt %d: OutputLen = %d, want %d", i, receipts[i].OutputLen, wantLen)
		}
		wantOutputOffset += int64(wantLen)
	}

	writes := fw.snapshotWrites()
	if len(writes) < nBlocks+1 {
		t.Fatalf("writer got %d writes, want at least %d", len(writes), nBlocks+1)
	}
	for i := 0; i < nBlocks; i++ {
		blockWrite := writes[i+1]
		want := append([]byte("C"), blockData(i, blockSize, nBlocks)...)
		if !bytes.Equal(blockWrite, want) {
			t.Errorf("writer write %d: got %q, want %q", i+1, blockWrite, want)
		}
	}

	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBlockReceiptsOutOfOrderCompletion(t *testing.T) {
	const blockSize = 20000
	const nBlocks = 3

	fw := &fakeWriter{}
	z := NewWriter(fw)
	if err := z.SetConcurrency(blockSize, 4); err != nil {
		t.Fatal(err)
	}

	barriers := make(map[string]chan struct{})
	dones := make(map[string]chan struct{})
	for i := 0; i < nBlocks; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		barriers[key] = make(chan struct{})
		dones[key] = make(chan struct{})
	}
	setupFakeCompressors(z, barriers, nil, dones)
	receiptsCh := z.BlockReceipts()

	for i := 0; i < nBlocks; i++ {
		if _, err := z.Write(blockData(i, blockSize, nBlocks)); err != nil {
			t.Fatal(err)
		}
	}

	key2 := inputKey(blockData(2, blockSize, nBlocks))
	close(barriers[key2])
	select {
	case <-dones[key2]:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for block 2 to flush")
	}

	writes := fw.snapshotWrites()
	for _, w := range writes {
		if bytes.HasPrefix(w, []byte("CC")) {
			t.Fatalf("block 2 was written before block 1: %q", w)
		}
	}

	for i := 0; i < nBlocks; i++ {
		if i == 2 {
			continue
		}
		key := inputKey(blockData(i, blockSize, nBlocks))
		close(barriers[key])
	}

	if err := z.Flush(); err != nil {
		t.Fatal(err)
	}

	receipts := collectReceipts(t, receiptsCh, nBlocks)
	for i := 0; i < nBlocks; i++ {
		if receipts[i].Seq != uint64(i) {
			t.Errorf("receipt %d: Seq = %d, want %d", i, receipts[i].Seq, i)
		}
	}

	writes = fw.snapshotWrites()
	var block1Idx, block2Idx = -1, -1
	for i, w := range writes {
		if bytes.Equal(w, append([]byte("C"), blockData(1, blockSize, nBlocks)...)) {
			block1Idx = i
		}
		if bytes.Equal(w, append([]byte("C"), blockData(2, blockSize, nBlocks)...)) {
			block2Idx = i
		}
	}
	if block1Idx < 0 {
		t.Error("block 1 was never written")
	}
	if block2Idx < 0 {
		t.Error("block 2 was never written")
	}
	if block1Idx >= 0 && block2Idx >= 0 && block1Idx >= block2Idx {
		t.Errorf("block 2 written at index %d before block 1 at index %d", block2Idx, block1Idx)
	}

	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBlockReceiptsFailureBlocksLater(t *testing.T) {
	const blockSize = 20000
	const nBlocks = 3

	fw := &fakeWriter{}
	z := NewWriter(fw)
	if err := z.SetConcurrency(blockSize, 4); err != nil {
		t.Fatal(err)
	}

	barriers := make(map[string]chan struct{})
	errs := make(map[string]error)
	for i := 0; i < nBlocks; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		barriers[key] = make(chan struct{})
	}
	errs[inputKey(blockData(1, blockSize, nBlocks))] = errors.New("fake compression error")
	setupFakeCompressors(z, barriers, errs, nil)
	receiptsCh := z.BlockReceipts()

	for i := 0; i < nBlocks; i++ {
		if _, err := z.Write(blockData(i, blockSize, nBlocks)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < nBlocks; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		close(barriers[key])
	}

	if err := z.Flush(); err == nil {
		t.Fatal("Flush should have returned an error")
	}

	var receipts []BlockReceipt
	for {
		select {
		case r, ok := <-receiptsCh:
			if !ok {
				goto done
			}
			receipts = append(receipts, r)
		case <-time.After(100 * time.Millisecond):
			goto done
		}
	}
done:
	// Block 0 succeeds and gets a receipt. Block 1 fails and gets a
	// failure receipt. Block 2 is skipped and gets no receipt.
	if len(receipts) != 2 {
		t.Fatalf("got %d receipts, want 2 (block 0 success, block 1 failure)", len(receipts))
	}
	if receipts[0].Seq != 0 {
		t.Errorf("receipt 0 Seq = %d, want 0", receipts[0].Seq)
	}
	if receipts[0].Err != nil {
		t.Errorf("receipt 0 has unexpected error: %v", receipts[0].Err)
	}
	if receipts[1].Seq != 1 {
		t.Errorf("receipt 1 Seq = %d, want 1", receipts[1].Seq)
	}
	if receipts[1].Err == nil {
		t.Error("receipt for failed block has nil Err")
	}

	writes := fw.snapshotWrites()
	for _, w := range writes {
		if bytes.Equal(w, append([]byte("C"), blockData(2, blockSize, nBlocks)...)) {
			t.Error("block 2 was written after block 1 failed")
		}
		if bytes.Equal(w, append([]byte("C"), blockData(3, blockSize, nBlocks)...)) {
			t.Error("block 3 was written after block 1 failed")
		}
	}

	if err := z.Close(); err == nil {
		t.Fatal("Close should have returned an error")
	}
}

func TestBlockReceiptsFlushBarrier(t *testing.T) {
	const blockSize = 20000
	const nBlocks = 3

	fw := &fakeWriter{}
	z := NewWriter(fw)
	if err := z.SetConcurrency(blockSize, 4); err != nil {
		t.Fatal(err)
	}

	barriers := make(map[string]chan struct{})
	dones := make(map[string]chan struct{})
	for i := 0; i < nBlocks; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		barriers[key] = make(chan struct{})
		dones[key] = make(chan struct{})
	}
	setupFakeCompressors(z, barriers, nil, dones)

	for i := 0; i < 2; i++ {
		if _, err := z.Write(blockData(i, blockSize, nBlocks)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 2; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		close(barriers[key])
	}

	partial := blockData(2, blockSize, nBlocks)
	if _, err := z.Write(partial); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- z.Flush()
	}()

	select {
	case <-done:
		t.Fatal("Flush returned before the boundary block completed")
	case <-time.After(100 * time.Millisecond):
	}

	key := inputKey(partial)
	close(barriers[key])

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Flush returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not return after the boundary block completed")
	}

	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBlockReceiptsCloseReapsWorkers(t *testing.T) {
	const blockSize = 20000
	const nBlocks = 2

	fw := &fakeWriter{}
	z := NewWriter(fw)
	if err := z.SetConcurrency(blockSize, 4); err != nil {
		t.Fatal(err)
	}

	barriers := make(map[string]chan struct{})
	errs := make(map[string]error)
	for i := 0; i < nBlocks; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		barriers[key] = make(chan struct{})
	}
	errs[inputKey(blockData(0, blockSize, nBlocks))] = errors.New("fake compression error")
	setupFakeCompressors(z, barriers, errs, nil)

	for i := 0; i < nBlocks; i++ {
		if _, err := z.Write(blockData(i, blockSize, nBlocks)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < nBlocks; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		close(barriers[key])
	}

	if err := z.Close(); err == nil {
		t.Fatal("Close should have returned an error")
	}
	if err := z.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

func TestBlockReceiptsNotOptedIn(t *testing.T) {
	const blockSize = 20000
	const nBlocks = 2

	fw := &fakeWriter{}
	z := NewWriter(fw)
	if err := z.SetConcurrency(blockSize, 4); err != nil {
		t.Fatal(err)
	}

	barriers := make(map[string]chan struct{})
	for i := 0; i < nBlocks; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		barriers[key] = make(chan struct{})
	}
	setupFakeCompressors(z, barriers, nil, nil)

	for i := 0; i < nBlocks; i++ {
		if _, err := z.Write(blockData(i, blockSize, nBlocks)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < nBlocks; i++ {
		key := inputKey(blockData(i, blockSize, nBlocks))
		close(barriers[key])
	}

	if err := z.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}

	writes := fw.snapshotWrites()
	for i := 0; i < nBlocks; i++ {
		blockWrite := writes[i+1]
		want := append([]byte("C"), blockData(i, blockSize, nBlocks)...)
		if !bytes.Equal(blockWrite, want) {
			t.Errorf("writer write %d: got %q, want %q", i+1, blockWrite, want)
		}
	}
}
