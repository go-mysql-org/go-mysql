package packet

import (
	"bufio"
	"bytes"
	"io"
	"net"
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

// benchmarkReadPacket measures the read path over loopback TCP: unbuffered is
// what a conn from NewTLSConn pays without EnableReadBuffering (two read(2)
// calls per packet), buffered is the same conn after EnableReadBuffering.
func benchmarkReadPacket(b *testing.B, buffered bool) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()

	const payloadSize = 64
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		w := bufio.NewWriterSize(conn, DefaultBufferSize)
		frame := make([]byte, 4+payloadSize)
		frame[0] = payloadSize
		for seq := byte(0); ; seq++ {
			frame[3] = seq
			if _, err := w.Write(frame); err != nil {
				return
			}
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	c := NewTLSConn(conn)
	if buffered {
		c.EnableReadBuffering(DefaultReadBufferSize)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.ReadPacket(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadPacketUnbuffered(b *testing.B) { benchmarkReadPacket(b, false) }

func BenchmarkReadPacketBuffered(b *testing.B) { benchmarkReadPacket(b, true) }

// benchmarkWriteResultset measures writing a point-SELECT-shaped response (22
// packets: column count, 17 column definitions, row, EOF/OK framing) followed
// by one Flush. Unbuffered issues one write(2) per packet, buffered one per
// response.
func benchmarkWriteResultset(b *testing.B, buffered bool) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	c := NewTLSConn(conn)
	if buffered {
		c.EnableWriteBuffering(DefaultBufferSize)
	}

	const packetsPerResponse = 22
	frame := make([]byte, 4+64)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for range packetsPerResponse {
			if err := c.WritePacket(frame); err != nil {
				b.Fatal(err)
			}
		}
		if err := c.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteResultsetUnbuffered(b *testing.B) { benchmarkWriteResultset(b, false) }

func BenchmarkWriteResultsetBuffered(b *testing.B) { benchmarkWriteResultset(b, true) }

// TestReadPacketSequenceMismatchIsBadConn covers a desynced stream: once a packet
// arrives with an unexpected sequence, nothing after it can be interpreted, so the
// connection must be reported as bad rather than merely as a failed read. A server
// closing an idle connection queues its ERR packet outside any command cycle, so it
// carries sequence 0 and the next command reads it while expecting 1.
func TestReadPacketSequenceMismatchIsBadConn(t *testing.T) {
	c := newReadTestConn(mysqlPacket(0, []byte("out of band")), mysql.MYSQL_COMPRESS_NONE)
	c.Sequence = 1

	_, err := c.ReadPacket()
	if err == nil {
		t.Fatal("ReadPacket succeeded on a mismatched sequence, want an error")
	}
	if !mysql.ErrorEqual(err, mysql.ErrBadConn) {
		t.Errorf("err = %v, want an error equal to mysql.ErrBadConn", err)
	}
}

// writeCaptureConn records everything written to it.
type writeCaptureConn struct {
	net.Conn
	written bytes.Buffer
}

func (w *writeCaptureConn) Write(b []byte) (int, error) { return w.written.Write(b) }

type writtenFrame struct {
	seq     byte
	payload []byte // uncompressed
}

// parseCompressedFrames splits a compressed-protocol byte stream into frames
// and decompresses their payloads.
func parseCompressedFrames(t *testing.T, stream []byte) []writtenFrame {
	t.Helper()
	var frames []writtenFrame
	for len(stream) > 0 {
		if len(stream) < 7 {
			t.Fatalf("truncated compressed header: %d bytes left", len(stream))
		}
		cl := int(stream[0]) | int(stream[1])<<8 | int(stream[2])<<16
		ul := int(stream[4]) | int(stream[5])<<8 | int(stream[6])<<16
		if len(stream) < 7+cl {
			t.Fatalf("frame %d: compressed length %d, only %d bytes left", len(frames), cl, len(stream)-7)
		}
		body := stream[7 : 7+cl]
		if ul > 0 {
			r, err := compress.GetPooledZlibReader(bytes.NewReader(body))
			if err != nil {
				t.Fatalf("frame %d: zlib reader: %v", len(frames), err)
			}
			body, err = io.ReadAll(r)
			if err != nil {
				t.Fatalf("frame %d: zlib read: %v", len(frames), err)
			}
			if len(body) != ul {
				t.Fatalf("frame %d: uncompressed length %d, header says %d", len(frames), len(body), ul)
			}
		}
		frames = append(frames, writtenFrame{seq: stream[3], payload: body})
		stream = stream[7+cl:]
	}
	return frames
}

// TestWritePacketCompressedLargePacket checks that a packet of 16MB or more is
// sent in compressed packets of at most mysql.MaxPayloadLen bytes with
// consecutive sequence numbers, and that the packet sequence then continues
// from the compressed sequence like the server expects. Before, the 16MB
// parts were written to the connection without a compressed header.
func TestWritePacketCompressedLargePacket(t *testing.T) {
	const tail = 1000
	payload := make([]byte, mysql.MaxPayloadLen+tail)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	data := append(make([]byte, 4), payload...)

	w := &writeCaptureConn{}
	c := &Conn{Conn: w, Compression: mysql.MYSQL_COMPRESS_ZLIB}
	if err := c.WritePacket(data); err != nil {
		t.Fatalf("WritePacket: %v", err)
	}

	frames := parseCompressedFrames(t, w.written.Bytes())
	var stream []byte
	for i, f := range frames {
		if f.seq != byte(i) {
			t.Fatalf("frame %d has compressed sequence %d", i, f.seq)
		}
		if len(f.payload) > mysql.MaxPayloadLen {
			t.Fatalf("frame %d carries %d bytes, more than %d", i, len(f.payload), mysql.MaxPayloadLen)
		}
		stream = append(stream, f.payload...)
	}
	want := append(mysqlPacket(0, payload[:mysql.MaxPayloadLen]), mysqlPacket(1, payload[mysql.MaxPayloadLen:])...)
	if !bytes.Equal(stream, want) {
		t.Fatalf("uncompressed stream does not match the two packets (%d bytes, want %d)", len(stream), len(want))
	}
	if int(c.CompressedSequence) != len(frames) || c.Sequence != c.CompressedSequence {
		t.Fatalf("after %d frames: Sequence %d, CompressedSequence %d", len(frames), c.Sequence, c.CompressedSequence)
	}
}

// TestCompressedSequenceContinuesAfterRead checks that the compressed sequence
// is shared by both directions within a command, as in LOAD DATA LOCAL INFILE
// where the client sends the file after the server's request. Before, every
// write restarted at compressed sequence 0.
func TestCompressedSequenceContinuesAfterRead(t *testing.T) {
	request := append([]byte{mysql.LocalInFile_HEADER}, "data.csv"...)
	w := &writeCaptureConn{}
	c := newReadTestConn(uncompressedFrame(1, mysqlPacket(1, request)), mysql.MYSQL_COMPRESS_ZLIB)
	c.Conn = w
	// state after the client sent the query in compressed packet 0
	c.Sequence, c.CompressedSequence = 1, 1

	got, err := c.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if !bytes.Equal(got, request) {
		t.Fatalf("ReadPacket = %q, want %q", got, request)
	}

	for i, content := range [][]byte{[]byte("1,2\n"), nil} {
		if err := c.WritePacket(append(make([]byte, 4), content...)); err != nil {
			t.Fatalf("WritePacket #%d: %v", i, err)
		}
	}
	frames := parseCompressedFrames(t, w.written.Bytes())
	if len(frames) != 2 {
		t.Fatalf("got %d frames, want 2", len(frames))
	}
	for i, f := range frames {
		seq := byte(2 + i)
		if f.seq != seq {
			t.Errorf("frame %d has compressed sequence %d, want %d", i, f.seq, seq)
		}
		if len(f.payload) < 4 || f.payload[3] != seq {
			t.Errorf("frame %d: packet sequence is not %d", i, seq)
		}
	}
	if c.Sequence != 4 || c.CompressedSequence != 4 {
		t.Errorf("Sequence %d, CompressedSequence %d, want 4 and 4", c.Sequence, c.CompressedSequence)
	}

	c.ResetSequence()
	if c.Sequence != 0 || c.CompressedSequence != 0 {
		t.Errorf("ResetSequence left Sequence %d, CompressedSequence %d", c.Sequence, c.CompressedSequence)
	}
}
