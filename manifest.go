// Copyright 2026 The pgzip Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgzip

import (
	"github.com/klauspost/compress/flate"
)

// MemberInfo describes a single gzip member found in a (possibly
// concatenated) gzip stream.
//
// A concatenated gzip file is NOT reported as a single member:
// every member gets its own MemberInfo entry, recorded while the
// stream is being decompressed. The manifest is therefore available
// without re-decompressing the input.
type MemberInfo struct {
	// Index is the 0-based position of the member in the stream.
	Index int
	// Offset is the byte offset of the first header byte of the
	// member in the compressed input stream.
	Offset int64
	// CompressedSize is the number of compressed bytes consumed by
	// this member, including header and trailer.
	// For a Truncated member it is the number of bytes consumed
	// before the stream ended or failed.
	CompressedSize int64
	// Size is the number of decompressed bytes produced by this member.
	// For a Truncated member it is the partial count produced before
	// the stream ended or failed.
	Size uint32
	// CRC32 is the checksum stored in the member trailer.
	// It is 0 if the trailer was never reached (Truncated).
	CRC32 uint32
	// CRCOK reports whether the decompressed data matched the CRC-32
	// and ISIZE stored in the member trailer.
	CRCOK bool
	// Truncated reports whether the member ended before a complete,
	// valid trailer was read (truncated stream or corrupt deflate data).
	// Members before a truncated member remain fully auditable.
	Truncated bool

	// Name and Comment are only exposed when the corresponding
	// header flags allowed them; HasName/HasComment report their presence.
	// No other header fields are exposed in the manifest.
	Name       string
	Comment    string
	HasName    bool
	HasComment bool
}

// Manifest is an explicit per-member listing of a gzip stream.
// It is built incrementally by the Reader while decompressing,
// so obtaining it never requires re-decompressing the input.
type Manifest struct {
	// Members holds one entry per gzip member encountered,
	// in the order they appear in the stream.
	Members []MemberInfo
	// Trailing is the number of bytes after the last complete member
	// that do not form a valid gzip member (trailing non-member data).
	// It is 0 when the stream ended cleanly at a member boundary.
	Trailing int64
}

// Manifest returns the member manifest recorded by the Reader so far.
//
// The manifest is populated while the Reader decompresses the stream;
// it is complete once Read has returned io.EOF or an error.
// Each gzip member of a concatenated stream is listed separately with
// its compressed range, decompressed length and CRC verification result.
// A truncated final member is reported as incomplete, while all members
// before it remain auditable. Bytes after the last complete member that
// do not form a gzip member are reported separately as Trailing.
//
// Calling Manifest does not change the Reader's default output.
func (z *Reader) Manifest() Manifest {
	z.manifestMu.Lock()
	defer z.manifestMu.Unlock()
	out := Manifest{Trailing: z.trailing}
	out.Members = append([]MemberInfo(nil), z.members...)
	return out
}

// countReader counts the bytes consumed from a flate.Reader,
// so compressed member ranges can be recorded exactly as the
// decompressor and header parser consume them.
type countReader struct {
	r flate.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countReader) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}

// startMember records the offset at which a member header starts.
func (z *Reader) startMember() {
	z.memberStart = z.cr.n
}

// addMember appends a fresh member entry for the header just parsed.
func (z *Reader) addMember(m MemberInfo) {
	z.manifestMu.Lock()
	m.Index = len(z.members)
	m.Offset = z.memberStart
	z.members = append(z.members, m)
	z.curMember = m.Index
	z.manifestMu.Unlock()
}

// finishMember records trailer verification results for the current member.
func (z *Reader) finishMember(crc uint32, ok bool) {
	z.manifestMu.Lock()
	defer z.manifestMu.Unlock()
	if z.curMember < 0 || z.curMember >= len(z.members) {
		return
	}
	m := &z.members[z.curMember]
	m.CRC32 = crc
	m.CRCOK = ok
	m.Size = z.size
	m.CompressedSize = z.cr.n - m.Offset
}

// truncateMember marks the current member as incomplete.
// Members recorded before it remain fully auditable.
func (z *Reader) truncateMember() {
	z.manifestMu.Lock()
	defer z.manifestMu.Unlock()
	if z.curMember < 0 || z.curMember >= len(z.members) {
		return
	}
	m := &z.members[z.curMember]
	m.Truncated = true
	m.CRCOK = false
	m.Size = z.size
	m.CompressedSize = z.cr.n - m.Offset
}

// setTrailing records bytes after the last complete member that
// do not form a valid gzip member.
func (z *Reader) setTrailing() {
	z.manifestMu.Lock()
	z.trailing = z.cr.n - z.memberStart
	z.manifestMu.Unlock()
}
