package packet

import (
	"bytes"
	"testing"

	"github.com/go-mysql-org/go-mysql/compress"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// newReadTestConn builds a Conn whose read side is fed from the given raw bytes,
// simulating frames as they would arrive from a MySQL server. No net.Conn is
// needed because readTimeout is 0, so SetReadDeadline is never called.
func newReadTestConn(stream []byte, compression uint8) *Conn {
	c := new(Conn)
	c.reader = bytes.NewReader(stream)
	c.copyNBuf = make([]byte, DefaultBufferSize)
	c.Compression = compression
	return c
}

// mysqlPacket builds a single MySQL protocol packet (4-byte header + payload).
func mysqlPacket(seq byte, payload []byte) []byte {
	n := len(payload)
	b := make([]byte, 4+n)
	b[0] = byte(n)
	b[1] = byte(n >> 8)
	b[2] = byte(n >> 16)
	b[3] = seq
	copy(b[4:], payload)
	return b
}

// uncompressedFrame builds a compressed-protocol frame whose payload is stored
// verbatim (length-before-compression == 0), as a server does for small or
// incompressible chunks.
func uncompressedFrame(seq byte, body []byte) []byte {
	cl := len(body)
	f := make([]byte, 7+cl)
	f[0] = byte(cl)
	f[1] = byte(cl >> 8)
	f[2] = byte(cl >> 16)
	f[3] = seq
	// bytes 4..6 (uncompressed length) stay 0
	copy(f[7:], body)
	return f
}

// zlibFrame builds a compressed-protocol frame whose payload is zlib-compressed.
func zlibFrame(t *testing.T, seq byte, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := compress.GetPooledZlibWriter(&buf)
	if err != nil {
		t.Fatalf("GetPooledZlibWriter: %v", err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zlib close: %v", err)
	}
	body := buf.Bytes()
	cl := len(body)
	ul := len(payload)
	f := make([]byte, 7+cl)
	f[0] = byte(cl)
	f[1] = byte(cl >> 8)
	f[2] = byte(cl >> 16)
	f[3] = seq
	f[4] = byte(ul)
	f[5] = byte(ul >> 8)
	f[6] = byte(ul >> 16)
	copy(f[7:], body)
	return f
}

// TestReadPacketPacketSpanningUncompressedFrames reproduces the desync where a
// single MySQL packet spans out of an uncompressed (length-before-compression 0)
// frame into the following frame. Before the fix the uncompressed frame was read
// from the raw, unbounded connection, so copyN read straight through the next
// frame's 7-byte header and corrupted the payload.
func TestReadPacketPacketSpanningUncompressedFrames(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 16) // 256 bytes
	pkt := mysqlPacket(0, payload)

	// Split the packet's bytes mid-payload across two uncompressed frames.
	split := 100
	stream := append(uncompressedFrame(0, pkt[:split]), uncompressedFrame(1, pkt[split:])...)

	c := newReadTestConn(stream, mysql.MYSQL_COMPRESS_ZLIB)
	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch:\n got  %q\n want %q", got, payload)
	}
}

// TestReadPacketPacketSpanningZlibFrames guards the already-working compressed
// path: a packet split across two zlib frames must still reassemble correctly.
func TestReadPacketPacketSpanningZlibFrames(t *testing.T) {
	payload := bytes.Repeat([]byte("the quick brown fox "), 32) // 640 bytes
	pkt := mysqlPacket(0, payload)

	split := 200
	stream := append(zlibFrame(t, 0, pkt[:split]), zlibFrame(t, 1, pkt[split:])...)

	c := newReadTestConn(stream, mysql.MYSQL_COMPRESS_ZLIB)
	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch:\n got  %q\n want %q", got, payload)
	}
}

// TestReadPacketMultiplePacketsInOneZlibFrame guards reader reuse across reads: a
// server may pack several MySQL packets into a single compressed frame, and each is
// retrieved by a separate ReadPacket call. The first read consumes the frame header
// and decompresses; subsequent reads must continue from the same compressedReader
// rather than consuming another frame header. Recreating the reader on every read
// (e.g. dropping the nil guard) reads garbage as the next frame's header and desyncs.
func TestReadPacketMultiplePacketsInOneZlibFrame(t *testing.T) {
	payloadA := bytes.Repeat([]byte("alpha "), 8)
	payloadB := bytes.Repeat([]byte("bravo "), 8)

	// Two complete MySQL packets (sequence 0 and 1) inside one zlib frame.
	combined := append(mysqlPacket(0, payloadA), mysqlPacket(1, payloadB)...)
	stream := zlibFrame(t, 0, combined)

	c := newReadTestConn(stream, mysql.MYSQL_COMPRESS_ZLIB)

	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket #1: %v", err)
	}
	if !bytes.Equal(got, payloadA) {
		t.Fatalf("packet #1 mismatch:\n got  %q\n want %q", got, payloadA)
	}

	got, err = c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket #2: %v", err)
	}
	if !bytes.Equal(got, payloadB) {
		t.Fatalf("packet #2 mismatch:\n got  %q\n want %q", got, payloadB)
	}
}

// TestReadPacketSingleUncompressedFrame guards the common small-response case:
// a whole packet inside one uncompressed frame.
func TestReadPacketSingleUncompressedFrame(t *testing.T) {
	payload := []byte("small uncompressed response")
	stream := uncompressedFrame(0, mysqlPacket(0, payload))

	c := newReadTestConn(stream, mysql.MYSQL_COMPRESS_ZLIB)
	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch:\n got  %q\n want %q", got, payload)
	}
}
