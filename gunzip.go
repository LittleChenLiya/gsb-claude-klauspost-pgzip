// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pgzip implements reading and writing of gzip format compressed files,
// as specified in RFC 1952.
//
// This is a drop in replacement for "compress/gzip".
// This will split compression into blocks that are compressed in parallel.
// This can be useful for compressing big amounts of data.
// The gzip decompression has not been modified, but remains in the package,
// so you can use it as a complete replacement for "compress/gzip".
//
// See more at https://github.com/klauspost/pgzip
package pgzip

import (
	"bufio"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/flate"
)

const (
	gzipID1     = 0x1f
	gzipID2     = 0x8b
	gzipDeflate = 8
	flagText    = 1 << 0
	flagHdrCrc  = 1 << 1
	flagExtra   = 1 << 2
	flagName    = 1 << 3
	flagComment = 1 << 4
)

func makeReader(r io.Reader) flate.Reader {
	if rr, ok := r.(flate.Reader); ok {
		return rr
	}
	return bufio.NewReader(r)
}

var (
	// ErrChecksum is returned when reading GZIP data that has an invalid checksum.
	ErrChecksum = errors.New("gzip: invalid checksum")
	// ErrHeader is returned when reading GZIP data that has an invalid header.
	ErrHeader = errors.New("gzip: invalid header")
)

// The gzip file stores a header giving metadata about the compressed file.
// That header is exposed as the fields of the Writer and Reader structs.
type Header struct {
	Comment string    // comment
	Extra   []byte    // "extra data"
	ModTime time.Time // modification time
	Name    string    // file name
	OS      byte      // operating system type
}

// Member describes a single gzip member in a concatenated gzip stream.
// A concatenated stream is reported as one Member per member, rather than
// as a single member. The header fields are exposed through the embedded
// Header, the same fields exposed by the Reader.
type Member struct {
	Header

	// CompressedStart is the byte offset in the input where this member
	// begins (the first byte of the gzip header).
	CompressedStart int64

	// CompressedEnd is the byte offset immediately after this member's
	// 8-byte CRC/size trailer. It is zero if the member was truncated
	// before its trailer could be read.
	CompressedEnd int64

	// UncompressedSize is the number of uncompressed bytes produced by
	// this member. It is set even for truncated members, reflecting the
	// bytes decompressed before truncation.
	UncompressedSize int64

	// CRC32 is the checksum stored in the member's trailer. It is zero if
	// the member was truncated before its trailer could be read.
	CRC32 uint32

	// StoredSize is the uncompressed size stored in the member's trailer.
	// It is zero if the member was truncated before its trailer could be
	// read.
	StoredSize uint32

	// CRCValid reports whether the CRC32 and uncompressed size stored in
	// the trailer match the data that was decompressed. A truncated member
	// is reported as not valid.
	CRCValid bool

	// Complete reports whether the member was fully read, including its
	// trailer. A truncated member has Complete == false, while members
	// preceding it remain complete and auditable.
	Complete bool
}

// Trailing describes bytes after the last gzip member that are not the
// start of another gzip member.
type Trailing struct {
	// Start is the byte offset where the trailing data begins (immediately
	// after the last member's trailer).
	Start int64

	// End is the byte offset immediately after the trailing data. It may be
	// under-reported if the readahead goroutine has not yet drained the
	// underlying reader; see Manifest for details.
	End int64
}

// Manifest is an audit report of the gzip members found in the input
// stream. A concatenated gzip stream is reported as one Member per member.
type Manifest struct {
	// Members are the gzip members read so far, in order. The returned
	// slice is a copy; it is safe to retain.
	Members []Member

	// Trailing describes bytes after the last member that are not the
	// start of another gzip member. It is nil if there is no trailing data.
	Trailing *Trailing
}

// countingReader wraps a flate.Reader and counts the number of bytes
// consumed through Read and ReadByte. This allows the compressed byte
// range of each gzip member to be tracked without re-decompressing.
type countingReader struct {
	r flate.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReader) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}

// A Reader is an io.Reader that can be read to retrieve
// uncompressed data from a gzip-format compressed file.
//
// In general, a gzip file can be a concatenation of gzip files,
// each with its own header.  Reads from the Reader
// return the concatenation of the uncompressed data of each.
// Only the first header is recorded in the Reader fields.
//
// Gzip files store a length and checksum of the uncompressed data.
// The Reader will return a ErrChecksum when Read
// reaches the end of the uncompressed data if it does not
// have the expected length or checksum.  Clients should treat data
// returned by Read as tentative until they receive the io.EOF
// marking the end of the data.
type Reader struct {
	Header
	r            flate.Reader
	decompressor io.ReadCloser
	digest       hash.Hash32
	size         uint32
	flg          byte
	buf          [512]byte
	err          error
	closeErr     chan error
	multistream  atomic.Bool

	readAhead   chan read
	roff        int // read offset
	current     []byte
	closeReader chan struct{}
	lastBlock   bool
	blockSize   int
	blocks      int

	readAheadStarted atomic.Bool // Indication if readahead has been started
	mu               sync.Mutex  // Lock for channels during killReadAhead

	blockPool chan []byte

	// Manifest tracking. cr counts the bytes consumed from the input so
	// that each member's compressed range can be reported without
	// re-decompressing. curHeader holds the header of the member currently
	// being decompressed. memberStart is the input offset where that member
	// begins. members and trailing are the audit report built during normal
	// decompression. raDone is closed when the readahead goroutine exits.
	cr          *countingReader
	curHeader   Header
	memberStart int64
	manifestMu  sync.Mutex
	members     []Member
	trailing    *Trailing
	raDone      chan struct{}
}

type read struct {
	b   []byte
	err error
}

// NewReader creates a new Reader reading the given reader.
// The implementation buffers input and may read more data than necessary from r.
// It is the caller's responsibility to call Close on the Reader when done.
func NewReader(r io.Reader) (*Reader, error) {
	z := new(Reader)
	z.blocks = defaultBlocks
	z.blockSize = defaultBlockSize
	z.cr = &countingReader{r: makeReader(r)}
	z.r = z.cr
	z.digest = crc32.NewIEEE()
	z.multistream.Store(true)
	z.blockPool = make(chan []byte, z.blocks)
	for i := 0; i < z.blocks; i++ {
		z.blockPool <- make([]byte, z.blockSize)
	}
	if err := z.readHeader(true); err != nil {
		return nil, err
	}
	z.memberStart = 0

	return z, nil
}

// NewReaderN creates a new Reader reading the given reader.
// The implementation buffers input and may read more data than necessary from r.
// It is the caller's responsibility to call Close on the Reader when done.
//
// With this you can control the approximate size of your blocks,
// as well as how many blocks you want to have prefetched.
//
// Default values for this is blockSize = 250000, blocks = 16,
// meaning up to 16 blocks of maximum 250000 bytes will be
// prefetched.
func NewReaderN(r io.Reader, blockSize, blocks int) (*Reader, error) {
	z := new(Reader)
	z.blocks = blocks
	z.blockSize = blockSize
	z.cr = &countingReader{r: makeReader(r)}
	z.r = z.cr
	z.digest = crc32.NewIEEE()
	z.multistream.Store(true)

	// Account for too small values
	if z.blocks <= 0 {
		z.blocks = defaultBlocks
	}
	if z.blockSize <= 512 {
		z.blockSize = defaultBlockSize
	}
	z.blockPool = make(chan []byte, z.blocks)
	for i := 0; i < z.blocks; i++ {
		z.blockPool <- make([]byte, z.blockSize)
	}
	if err := z.readHeader(true); err != nil {
		return nil, err
	}
	z.memberStart = 0
	return z, nil
}

// Reset discards the Reader z's state and makes it equivalent to the
// result of its original state from NewReader, but reading from r instead.
// This permits reusing a Reader rather than allocating a new one.
func (z *Reader) Reset(r io.Reader) error {
	z.killReadAhead()
	z.cr = &countingReader{r: makeReader(r)}
	z.r = z.cr
	z.digest = crc32.NewIEEE()
	z.size = 0
	z.err = nil
	z.lastBlock = false
	z.current = nil
	z.roff = 0
	z.multistream.Store(true)
	z.readAheadStarted.Store(false)

	// Clear manifest state so the new stream is reported independently.
	z.manifestMu.Lock()
	z.members = nil
	z.trailing = nil
	z.manifestMu.Unlock()
	z.curHeader = Header{}
	z.memberStart = 0

	// Account for uninitialized values
	if z.blocks <= 0 {
		z.blocks = defaultBlocks
	}
	if z.blockSize <= 512 {
		z.blockSize = defaultBlockSize
	}

	if z.blockPool == nil {
		z.blockPool = make(chan []byte, z.blocks)
		for i := 0; i < z.blocks; i++ {
			z.blockPool <- make([]byte, z.blockSize)
		}
	}
	if err := z.readHeader(true); err != nil {
		return err
	}
	z.memberStart = 0
	return nil
}

// Multistream controls whether the reader supports multistream files.
//
// If enabled (the default), the Reader expects the input to be a sequence
// of individually gzipped data streams, each with its own header and
// trailer, ending at EOF. The effect is that the concatenation of a sequence
// of gzipped files is treated as equivalent to the gzip of the concatenation
// of the sequence. This is standard behavior for gzip readers.
//
// Calling Multistream(false) disables this behavior; disabling the behavior
// can be useful when reading file formats that distinguish individual gzip
// data streams or mix gzip data streams with other data streams.
// In this mode, when the Reader reaches the end of the data stream,
// Read returns io.EOF. If the underlying reader implements io.ByteReader,
// it will be left positioned just after the gzip stream.
// To start the next stream, call z.Reset(r) followed by z.Multistream(false).
// If there is no next stream, z.Reset(r) will return io.EOF.
func (z *Reader) Multistream(ok bool) {
	z.multistream.Store(ok)
}

// GZIP (RFC 1952) is little-endian, unlike ZLIB (RFC 1950).
func get4(p []byte) uint32 {
	return uint32(p[0]) | uint32(p[1])<<8 | uint32(p[2])<<16 | uint32(p[3])<<24
}

func (z *Reader) readString() (string, error) {
	var err error
	needconv := false
	for i := 0; ; i++ {
		if i >= len(z.buf) {
			return "", ErrHeader
		}
		z.buf[i], err = z.r.ReadByte()
		if err != nil {
			return "", err
		}
		if z.buf[i] > 0x7f {
			needconv = true
		}
		if z.buf[i] == 0 {
			// GZIP (RFC 1952) specifies that strings are NUL-terminated ISO 8859-1 (Latin-1).
			if needconv {
				s := make([]rune, 0, i)
				for _, v := range z.buf[0:i] {
					s = append(s, rune(v))
				}
				return string(s), nil
			}
			return string(z.buf[0:i]), nil
		}
	}
}

func (z *Reader) read2() (uint32, error) {
	_, err := io.ReadFull(z.r, z.buf[0:2])
	if err != nil {
		return 0, err
	}
	return uint32(z.buf[0]) | uint32(z.buf[1])<<8, nil
}

func (z *Reader) readHeader(save bool) error {
	_, err := io.ReadFull(z.r, z.buf[0:10])
	if err != nil {
		return err
	}
	if z.buf[0] != gzipID1 || z.buf[1] != gzipID2 || z.buf[2] != gzipDeflate {
		return ErrHeader
	}
	z.flg = z.buf[3]
	z.size = 0
	h := Header{
		ModTime: time.Unix(int64(get4(z.buf[4:8])), 0),
		OS:      z.buf[9],
	}
	if save {
		z.ModTime = h.ModTime
		z.OS = h.OS
	}
	z.digest.Reset()
	z.digest.Write(z.buf[0:10])

	if z.flg&flagExtra != 0 {
		n, err := z.read2()
		if err != nil {
			return err
		}
		data := make([]byte, n)
		if _, err = io.ReadFull(z.r, data); err != nil {
			return err
		}
		h.Extra = data
		if save {
			z.Extra = data
		}
	}

	var s string
	if z.flg&flagName != 0 {
		if s, err = z.readString(); err != nil {
			return err
		}
		h.Name = s
		if save {
			z.Name = s
		}
	}

	if z.flg&flagComment != 0 {
		if s, err = z.readString(); err != nil {
			return err
		}
		h.Comment = s
		if save {
			z.Comment = s
		}
	}

	if z.flg&flagHdrCrc != 0 {
		n, err := z.read2()
		if err != nil {
			return err
		}
		sum := z.digest.Sum32() & 0xFFFF
		if n != sum {
			return ErrHeader
		}
	}

	z.digest.Reset()
	if r, ok := z.decompressor.(flate.Resetter); ok {
		if err := r.Reset(z.r, nil); err != nil {
			return err
		}
	} else {
		z.decompressor = flate.NewReader(z.r)
	}
	// Capture the header of the member about to be decompressed, so the
	// manifest can report it even after the next member's header is read.
	z.curHeader = h
	return nil
}

func (z *Reader) killReadAhead() error {
	if !z.readAheadStarted.Load() {
		return nil
	}

	z.mu.Lock()
	defer z.mu.Unlock()
	if z.closeReader != nil {
		close(z.closeReader)
	}

	// Wait for decompressor to be closed and return error, if any.
	e, ok := <-z.closeErr

	for blk := range z.readAhead {
		if blk.b != nil {
			z.blockPool <- blk.b
		}
	}
	if cap(z.current) > 0 {
		z.blockPool <- z.current
		z.current = nil
	}
	if !ok {
		// Channel is closed, so if there was any error it has already been returned.
		return nil
	}
	return e
}

// Starts readahead.
// Will return on error (including io.EOF)
// or when z.closeReader is closed.
func (z *Reader) doReadAhead() {

	if z.blocks <= 0 {
		z.blocks = defaultBlocks
	}
	if z.blockSize <= 512 {
		z.blockSize = defaultBlockSize
	}
	ra := make(chan read, z.blocks)
	z.readAhead = ra
	closeReader := make(chan struct{}, 0)
	z.closeReader = closeReader
	z.lastBlock = false
	closeErr := make(chan error, 1)
	z.closeErr = closeErr
	z.size = 0
	z.roff = 0
	z.current = nil
	raDone := make(chan struct{})
	z.raDone = raDone
	decomp := z.decompressor

	go func() {
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			closeErr <- decomp.Close()
			close(closeErr)
			close(ra)
			close(raDone)
		}()

		// We hold a local reference to digest, since
		// it way be changed by reset.
		digest := z.digest
		for {
			var buf []byte
			select {
			case buf = <-z.blockPool:
			case <-closeReader:
				return
			}
			buf = buf[0:z.blockSize]
			// Try to fill the buffer
			n, err := io.ReadFull(decomp, buf)
			if err == io.ErrUnexpectedEOF {
				if n > 0 {
					err = nil
				} else {
					// If we got zero bytes, we need to establish if
					// we reached end of stream or truncated stream.
					_, err = decomp.Read([]byte{})
					if err == io.EOF {
						err = nil
					} else {
						// Truncated member: the decompressor ran out of
						// input before reaching the end of the member.
						// Members preceding this one remain complete and
						// auditable.
						z.recordMember(Member{
							Header:           z.curHeader,
							CompressedStart:  z.memberStart,
							UncompressedSize: int64(z.size),
							CRCValid:         false,
							Complete:         false,
						})
					}
				}
			}
			if n < len(buf) {
				buf = buf[0:n]
			}
			wg.Wait()
			wg.Go(func() {
				digest.Write(buf)
			})
			z.size += uint32(n)

			// If we return any error, out digest must be ready
			if err != nil {
				wg.Wait()
			}
			if err == io.EOF {
				// Finished member; read checksum + size.
				if _, err = io.ReadFull(z.r, z.buf[0:8]); err == nil {
					memberEnd := z.cr.n
					crc32, isize := get4(z.buf[0:4]), get4(z.buf[4:8])
					sum := z.digest.Sum32()
					crcOK := sum == crc32 && isize == z.size
					m := Member{
						Header:           z.curHeader,
						CompressedStart:  z.memberStart,
						CompressedEnd:    memberEnd,
						UncompressedSize: int64(z.size),
						CRC32:            crc32,
						StoredSize:       isize,
						CRCValid:         crcOK,
						Complete:         true,
					}
					if !crcOK {
						err = ErrChecksum
						z.recordMember(m)
					} else if !z.multistream.Load() {
						err = io.EOF
						z.recordMember(m)
					} else {
						// Record the completed member before reading the
						// next header, since readHeader overwrites curHeader.
						z.recordMember(m)
						z.memberStart = memberEnd
						err = z.readHeader(false)
						if err != nil && err != io.EOF {
							// readHeader failed with ErrHeader or
							// io.ErrUnexpectedEOF: the bytes after the last
							// member are not another gzip member. Report
							// them separately as trailing data.
							z.setTrailing(memberEnd, z.cr.n)
						}
					}
				} else {
					// Trailer could not be read: truncated member.
					z.recordMember(Member{
						Header:           z.curHeader,
						CompressedStart:  z.memberStart,
						UncompressedSize: int64(z.size),
						CRCValid:         false,
						Complete:         false,
					})
				}
			}
			if err == nil && len(buf) == 0 {
				z.blockPool <- buf
				continue
			}
			select {
			case z.readAhead <- read{b: buf, err: err}:
			case <-closeReader:
				// Sent on close, we don't care about the next results
				z.blockPool <- buf
				return
			}
			if err != nil {
				return
			}
		}
	}()
}

func (z *Reader) Read(p []byte) (n int, err error) {
	if z.err != nil {
		return 0, z.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	// Already drained (e.g. via WriteTo/io.Copy). Returning here avoids
	// blocking on a full blockPool when current is nil.
	if z.lastBlock && len(z.current) == 0 {
		return 0, io.EOF
	}

	if z.readAheadStarted.CompareAndSwap(false, true) {
		z.doReadAhead()
	}

	for {
		if len(z.current) == 0 && !z.lastBlock {
			read := <-z.readAhead
			if read.err != nil {
				// If not nil, the reader will have exited
				z.closeReader = nil

				if read.err != io.EOF {
					if cap(read.b) > 0 {
						z.blockPool <- read.b
					}
					z.err = read.err
					return
				}
				if read.err == io.EOF {
					z.lastBlock = true
					err = nil
				}
			}
			z.current = read.b
			z.roff = 0
		}
		avail := z.current[z.roff:]
		if len(p) >= len(avail) {
			// If len(p) >= len(current), return all content of current
			n = copy(p, avail)
			if z.current != nil {
				z.blockPool <- z.current
				z.current = nil
			}
			if z.lastBlock {
				err = io.EOF
				break
			}
		} else {
			// We copy as much as there is space for
			n = copy(p, avail)
			z.roff += n
		}
		return
	}
	return n, err
}

func (z *Reader) WriteTo(w io.Writer) (n int64, err error) {
	if z.readAheadStarted.CompareAndSwap(false, true) {
		z.doReadAhead()
	}

	total := int64(0)
	avail := z.current[z.roff:]
	if len(avail) != 0 {
		n, err := w.Write(avail)
		if n != len(avail) {
			return total, io.ErrShortWrite
		}
		total += int64(n)
		if err != nil {
			return total, err
		}
		z.blockPool <- z.current
		z.current = nil
	}
	for z.err == nil && !z.lastBlock {
		// Read from input
		read := <-z.readAhead
		if read.err != nil {
			// If not nil, the reader will have exited
			z.closeReader = nil

			if read.err != io.EOF {
				if cap(read.b) > 0 {
					z.blockPool <- read.b
				}
				z.err = read.err
				return total, z.err
			}
			if read.err == io.EOF {
				z.lastBlock = true
			}
		}
		// Write what we got (even empty final block).
		if len(read.b) > 0 {
			n, err := w.Write(read.b)
			if n != len(read.b) {
				if cap(read.b) > 0 {
					z.blockPool <- read.b
				}
				return total, io.ErrShortWrite
			}
			total += int64(n)
			if err != nil {
				if cap(read.b) > 0 {
					z.blockPool <- read.b
				}
				return total, err
			}
		}
		// Put block back when it came from the pool.
		if cap(read.b) > 0 {
			z.blockPool <- read.b
		}
	}
	// WriterTo must not return io.EOF; io.Copy surfaces it to callers (#38).
	if z.err == io.EOF {
		return total, nil
	}
	return total, z.err
}

// Close closes the Reader. It does not close the underlying io.Reader.
func (z *Reader) Close() error {
	return z.killReadAhead()
}

// recordMember appends a member to the manifest.
func (z *Reader) recordMember(m Member) {
	z.manifestMu.Lock()
	z.members = append(z.members, m)
	z.manifestMu.Unlock()
}

// setTrailing records the byte range of trailing non-member data.
func (z *Reader) setTrailing(start, end int64) {
	z.manifestMu.Lock()
	z.trailing = &Trailing{Start: start, End: end}
	z.manifestMu.Unlock()
}

// Manifest returns an audit report of the gzip members read so far.
// A concatenated gzip stream is reported as one Member per member, rather
// than as a single member. The report is built during normal decompression;
// the underlying data is not re-decompressed to compute it.
//
// If the stream is truncated, the last member is reported as incomplete
// (Complete == false), while members preceding it remain complete and
// auditable. Trailing bytes after the last member that are not the start of
// another gzip member are reported separately as Trailing.
//
// The returned slice and Trailing are copies; it is safe to retain them.
func (z *Reader) Manifest() Manifest {
	z.manifestMu.Lock()
	defer z.manifestMu.Unlock()

	// If the readahead goroutine has exited, drain any remaining trailing
	// bytes to find the exact end of the non-member data. This is lazy and
	// only happens when the caller explicitly asks for the manifest, so the
	// default Read behavior is unchanged.
	if z.trailing != nil && z.raDone != nil {
		select {
		case <-z.raDone:
			if z.cr != nil {
				_, _ = io.Copy(io.Discard, z.cr)
				z.trailing.End = z.cr.n
			}
		default:
		}
	}

	members := make([]Member, len(z.members))
	copy(members, z.members)
	m := Manifest{Members: members}
	if z.trailing != nil {
		t := *z.trailing
		m.Trailing = &t
	}
	return m
}
