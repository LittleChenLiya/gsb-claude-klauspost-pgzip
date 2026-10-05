package pgzip

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

// gzipMember compresses s as a single gzip member with the given
// header name/comment and returns the compressed bytes.
func gzipMember(t *testing.T, name, comment, s string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Name = name
	w.Comment = comment
	if _, err := w.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// Two small members concatenated must decompress to the same data as
// before, and the manifest must list two separate members.
func TestManifestTwoMembers(t *testing.T) {
	m1 := gzipMember(t, "first.txt", "member one", "hello ")
	m2 := gzipMember(t, "second.txt", "", "world")
	cat := append(append([]byte{}, m1...), m2...)

	r, err := NewReader(bytes.NewReader(cat))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// Default Reader output is unchanged: concatenation of both members.
	if string(got) != "hello world" {
		t.Fatalf("got %q, want %q", got, "hello world")
	}

	mf := r.Manifest()
	if len(mf.Members) != 2 {
		t.Fatalf("manifest has %d members, want 2", len(mf.Members))
	}
	if mf.Trailing != 0 {
		t.Fatalf("trailing = %d, want 0", mf.Trailing)
	}
	for i, want := range []struct {
		off   int64
		csize int64
		size  uint32
		name  string
	}{
		{0, int64(len(m1)), 6, "first.txt"},
		{int64(len(m1)), int64(len(m2)), 5, "second.txt"},
	} {
		m := mf.Members[i]
		if m.Index != i || m.Offset != want.off || m.CompressedSize != want.csize || m.Size != want.size {
			t.Errorf("member %d: got index=%d offset=%d csize=%d size=%d, want index=%d offset=%d csize=%d size=%d",
				i, m.Index, m.Offset, m.CompressedSize, m.Size, i, want.off, want.csize, want.size)
		}
		if !m.CRCOK || m.Truncated {
			t.Errorf("member %d: CRCOK=%v Truncated=%v, want CRCOK=true Truncated=false", i, m.CRCOK, m.Truncated)
		}
		if m.CRC32 == 0 {
			t.Errorf("member %d: CRC32 not recorded", i)
		}
		if !m.HasName || m.Name != want.name {
			t.Errorf("member %d: name = %q (has=%v), want %q", i, m.Name, m.HasName, want.name)
		}
	}
	// Header fields are only exposed per allowed flags:
	// member 0 has a comment, member 1 does not.
	if !mf.Members[0].HasComment || mf.Members[0].Comment != "member one" {
		t.Errorf("member 0: comment = %q (has=%v), want %q", mf.Members[0].Comment, mf.Members[0].HasComment, "member one")
	}
	if mf.Members[1].HasComment || mf.Members[1].Comment != "" {
		t.Errorf("member 1: comment = %q (has=%v), want absent", mf.Members[1].Comment, mf.Members[1].HasComment)
	}
	// The compressed ranges cover the whole input exactly.
	if mf.Members[0].CompressedSize+mf.Members[1].CompressedSize != int64(len(cat)) {
		t.Errorf("member ranges %d+%d do not cover input size %d",
			mf.Members[0].CompressedSize, mf.Members[1].CompressedSize, len(cat))
	}
}

// A bad CRC in the second member must only mark the second member as
// failed; the first member stays complete and auditable.
func TestManifestSecondMemberBadCRC(t *testing.T) {
	m1 := gzipMember(t, "good", "", "first member payload")
	m2 := gzipMember(t, "bad", "", "second member payload")
	cat := append(append([]byte{}, m1...), m2...)
	// Corrupt the CRC-32 stored in the trailer of the second member.
	cat[len(cat)-8] ^= 0xff

	r, err := NewReader(bytes.NewReader(cat))
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(r)
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("ReadAll err = %v, want ErrChecksum", err)
	}
	r.Close()

	mf := r.Manifest()
	if len(mf.Members) != 2 {
		t.Fatalf("manifest has %d members, want 2", len(mf.Members))
	}
	first, second := mf.Members[0], mf.Members[1]
	if !first.CRCOK || first.Truncated {
		t.Errorf("first member: CRCOK=%v Truncated=%v, want complete and verified", first.CRCOK, first.Truncated)
	}
	if first.Size != uint32(len("first member payload")) {
		t.Errorf("first member size = %d", first.Size)
	}
	if second.CRCOK {
		t.Errorf("second member: CRCOK=true, want false")
	}
	if second.Truncated {
		t.Errorf("second member: Truncated=true, want false (trailer was read, CRC failed)")
	}
	if second.CompressedSize != int64(len(m2)) {
		t.Errorf("second member compressed size = %d, want %d", second.CompressedSize, len(m2))
	}
}

// A truncated member is reported as incomplete; members before it
// remain auditable.
func TestManifestTruncatedMember(t *testing.T) {
	m1 := gzipMember(t, "complete", "", "complete member")
	m2 := gzipMember(t, "truncated", "", "truncated member payload")
	// Cut the second member in half.
	cat := append(append([]byte{}, m1...), m2[:len(m2)/2]...)

	r, err := NewReader(bytes.NewReader(cat))
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(r)
	if err == nil {
		t.Fatal("ReadAll err = nil, want truncation error")
	}
	r.Close()

	mf := r.Manifest()
	if len(mf.Members) != 2 {
		t.Fatalf("manifest has %d members, want 2", len(mf.Members))
	}
	first, second := mf.Members[0], mf.Members[1]
	if !first.CRCOK || first.Truncated {
		t.Errorf("first member: CRCOK=%v Truncated=%v, want complete and verified", first.CRCOK, first.Truncated)
	}
	if first.CompressedSize != int64(len(m1)) {
		t.Errorf("first member compressed size = %d, want %d", first.CompressedSize, len(m1))
	}
	if !second.Truncated {
		t.Errorf("second member: Truncated=false, want true")
	}
	if second.CRCOK {
		t.Errorf("second member: CRCOK=true, want false")
	}
	if second.Offset != int64(len(m1)) {
		t.Errorf("second member offset = %d, want %d", second.Offset, len(m1))
	}
}

// Trailing bytes that do not form a gzip member are reported
// separately, not as a member.
func TestManifestTrailingNonMember(t *testing.T) {
	m1 := gzipMember(t, "only", "", "payload")
	trailing := []byte("this is not a gzip member")
	cat := append(append([]byte{}, m1...), trailing...)

	r, err := NewReader(bytes.NewReader(cat))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	// Default Reader behavior is unchanged: trailing garbage is an error.
	if !errors.Is(err, ErrHeader) {
		t.Fatalf("ReadAll err = %v, want ErrHeader", err)
	}
	if string(got) != "payload" {
		t.Fatalf("got %q, want %q", got, "payload")
	}
	r.Close()

	mf := r.Manifest()
	if len(mf.Members) != 1 {
		t.Fatalf("manifest has %d members, want 1", len(mf.Members))
	}
	if !mf.Members[0].CRCOK || mf.Members[0].Truncated {
		t.Errorf("member 0: CRCOK=%v Truncated=%v, want complete and verified", mf.Members[0].CRCOK, mf.Members[0].Truncated)
	}
	if mf.Trailing <= 0 {
		t.Errorf("trailing = %d, want > 0", mf.Trailing)
	}
	if mf.Members[0].Offset+mf.Members[0].CompressedSize+mf.Trailing > int64(len(cat)) {
		t.Errorf("member range %d + trailing %d exceeds input size %d",
			mf.Members[0].CompressedSize, mf.Trailing, len(cat))
	}
}

// ExampleReader_Manifest concatenates two small gzip members and lists
// them individually, without treating the file as a single member.
func ExampleReader_Manifest() {
	var buf bytes.Buffer
	for i, s := range []string{"hello ", "world"} {
		w := NewWriter(&buf)
		w.Name = fmt.Sprintf("part-%d", i)
		w.Write([]byte(s))
		w.Close()
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		panic(err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	r.Close()
	fmt.Printf("data: %q\n", data)
	for _, m := range r.Manifest().Members {
		fmt.Printf("member %d: offset=%d compressed=%d size=%d name=%q crcOK=%v truncated=%v\n",
			m.Index, m.Offset, m.CompressedSize, m.Size, m.Name, m.CRCOK, m.Truncated)
	}
	// Output:
	// data: "hello world"
	// member 0: offset=0 compressed=43 size=6 name="part-0" crcOK=true truncated=false
	// member 1: offset=43 compressed=42 size=5 name="part-1" crcOK=true truncated=false
}
