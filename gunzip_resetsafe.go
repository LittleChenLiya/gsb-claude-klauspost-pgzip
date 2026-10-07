// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgzip

import (
	"hash/crc32"
	"io"
)

// ResetSafe is a Reset adapter that preserves the state of the active Reader
// until the header of the new input has been validated.
//
// Unlike Reset, which discards the current stream before parsing the new
// header, ResetSafe first builds a candidate Reader from r and parses its
// gzip header. The candidate only replaces the active stream when the header
// check succeeds. If the header is invalid (ErrHeader, io.EOF on an empty
// input, or any other read error), the active Reader is left untouched and
// remains fully readable: its current stream, position, digest, block pool
// and background read-ahead are all preserved.
//
// On rejection every resource allocated for the candidate (its block buffers
// and any decompressor/background read-ahead) is reclaimed before returning,
// so a failed attempt neither corrupts the live stream nor leaks goroutines
// or memory.
//
// On success the semantics match Reset: z becomes equivalent to the result
// of NewReader(r), and the previous stream is discarded. The plain Reset
// method keeps its original behavior.
func (z *Reader) ResetSafe(r io.Reader) error {
	// Build the candidate with the same block geometry as the active Reader,
	// accounting for uninitialized values exactly like NewReaderN/Reset.
	blocks, blockSize := z.blocks, z.blockSize
	if blocks <= 0 {
		blocks = defaultBlocks
	}
	if blockSize <= 512 {
		blockSize = defaultBlockSize
	}

	cand := new(Reader)
	cand.blocks = blocks
	cand.blockSize = blockSize
	cand.r = makeReader(r)
	cand.digest = crc32.NewIEEE()
	cand.multistream.Store(true)
	cand.blockPool = make(chan []byte, blocks)
	for i := 0; i < blocks; i++ {
		cand.blockPool <- make([]byte, blockSize)
	}

	// Validate the new header on the candidate. The active Reader z is not
	// touched until this succeeds.
	if err := cand.readHeader(true); err != nil {
		cand.releaseCandidate()
		return err
	}

	// Header is valid: stop the old background read-ahead, then install the
	// candidate as the active stream. killReadAhead returns blocks to the
	// *current* pool, so it must run before the pool is swapped.
	z.killReadAhead()
	oldPool := z.blockPool

	z.Header = cand.Header
	z.r = cand.r
	z.decompressor = cand.decompressor
	z.digest = cand.digest
	z.size = cand.size
	z.flg = cand.flg
	z.buf = cand.buf
	z.err = nil
	z.closeErr = nil
	z.lastBlock = false
	z.roff = 0
	z.current = nil
	z.readAhead = nil
	z.closeReader = nil
	z.multistream.Store(true)
	z.readAheadStarted.Store(false)
	z.blocks = cand.blocks
	z.blockSize = cand.blockSize
	z.blockPool = cand.blockPool

	// Detach the candidate so the garbage collector can reclaim it, and
	// release the previous stream's block buffers.
	cand.blockPool = nil
	cand.decompressor = nil
	drainBlockPool(oldPool)
	return nil
}

// releaseCandidate reclaims all resources held by a candidate Reader whose
// header failed validation (or which is otherwise discarded): it stops any
// background read-ahead, closes the decompressor and drains the block pool.
func (z *Reader) releaseCandidate() {
	z.killReadAhead()
	if z.decompressor != nil {
		_ = z.decompressor.Close()
		z.decompressor = nil
	}
	drainBlockPool(z.blockPool)
	z.blockPool = nil
}

// drainBlockPool empties p, dropping every buffer back to the garbage
// collector. It returns once all slots have been collected.
func drainBlockPool(p chan []byte) {
	for {
		select {
		case <-p:
		default:
			return
		}
	}
}
