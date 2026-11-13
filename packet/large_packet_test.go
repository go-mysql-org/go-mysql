package packet

import (
	"bytes"
	"io"
	"math/rand"
	"net"
	"strconv"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// fragmentedPacket builds a packet the way the protocol sends it: payloads of 16 MB or more are cut into fragments of exactly
// MaxPayloadLen, ending with a shorter one (empty if the payload is an exact multiple).
func fragmentedPacket(payload []byte) []byte {
	var packet []byte
	for seq := byte(0); ; seq++ {
		n := min(len(payload), mysql.MaxPayloadLen)
		packet = append(packet, mysqlPacket(seq, payload[:n])...)
		payload = payload[n:]
		if n < mysql.MaxPayloadLen {
			return packet
		}
	}
}

// zstdFrame builds a compressed-protocol frame whose payload is zstd-compressed.
func zstdFrame(t *testing.T, seq byte, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = w.Write(payload)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	body := buf.Bytes()
	cl, ul := len(body), len(payload)
	f := make([]byte, 7+cl)
	f[0], f[1], f[2], f[3] = byte(cl), byte(cl>>8), byte(cl>>16), seq
	f[4], f[5], f[6] = byte(ul), byte(ul>>8), byte(ul>>16)
	copy(f[7:], body)
	return f
}

// framesOf cuts data into compressed-protocol frames: the first frame is firstFrame bytes if that is set, and every other frame
// is frameSize bytes if that is set (otherwise whatever is left).
func framesOf(t *testing.T, frame func(*testing.T, byte, []byte) []byte, data []byte, firstFrame, frameSize int) []byte {
	t.Helper()
	var stream []byte
	for seq := byte(0); len(data) > 0; seq++ {
		n := len(data)
		switch {
		case seq == 0 && firstFrame > 0:
			n = min(n, firstFrame)
		case frameSize > 0:
			n = min(n, frameSize)
		}
		stream = append(stream, frame(t, seq, data[:n])...)
		data = data[n:]
	}
	return stream
}

// stallingReader serves its data, and when asked for more tells the test (on reading) and blocks until it is released. The
// test can then look at what ReadPacketTo has done with what it was given, with the read stopped where it is.
type stallingReader struct {
	data    *bytes.Reader
	reading chan struct{}
	release chan struct{}
}

func (r *stallingReader) Read(p []byte) (int, error) {
	if r.data.Len() == 0 {
		r.reading <- struct{}{}
		<-r.release
		return 0, io.EOF
	}
	return r.data.Read(p)
}

// A header announcing a packet is no reason to allocate the packet: the peer may never send it.
func TestReadPacketAllocatesForWhatArrivesNotWhatIsAnnounced(t *testing.T) {
	announced := []byte{0xff, 0xff, 0xff, 0x00} // 16 MB

	for _, tc := range []struct {
		name         string
		delivered    int
		wantAllocate func(t *testing.T, capacity int)
	}{
		{"nothing has arrived", 0, func(t *testing.T, capacity int) { require.Less(t, capacity, 2*firstStepSize) }},
		{"most of the first step has arrived", firstStepSize - 1, func(t *testing.T, capacity int) { require.Less(t, capacity, 2*firstStepSize) }},
		{"the first step has arrived", firstStepSize, func(t *testing.T, capacity int) { require.GreaterOrEqual(t, capacity, mysql.MaxPayloadLen) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &stallingReader{
				data:    bytes.NewReader(append(announced, make([]byte, tc.delivered)...)),
				reading: make(chan struct{}),
				release: make(chan struct{}),
			}
			c := newReadTestConn(nil, mysql.MYSQL_COMPRESS_NONE)
			c.reader = r

			var buf bytes.Buffer
			done := make(chan error, 1)
			go func() { done <- c.ReadPacketTo(&buf) }()

			<-r.reading // everything that was delivered has been read, and the read is stuck waiting for more
			tc.wantAllocate(t, buf.Cap())

			close(r.release)
			require.ErrorIs(t, <-done, mysql.ErrBadConn)
		})
	}
}

func TestReadPacketLargePayloadsIntact(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	for _, size := range []int{
		firstStepSize - 1, firstStepSize, firstStepSize + 1, 2*firstStepSize - 1, 2 * firstStepSize, 2*firstStepSize + 1,
		1 << 20, mysql.MaxPayloadLen, mysql.MaxPayloadLen + 5,
	} {
		payload := make([]byte, size)
		random.Read(payload)

		c := newReadTestConn(fragmentedPacket(payload), mysql.MYSQL_COMPRESS_NONE)
		got, err := c.ReadPacket()
		require.NoError(t, err, "payload of %d bytes", size)
		require.True(t, bytes.Equal(payload, got), "payload of %d bytes was changed", size)
	}
}

// Reading a payload in steps must not leave the buffer much larger than the payload: growing a buffer by less than its size
// doubles it. (Go rounds large allocations up a little, hence the margin; doubling would be well past it.)
func TestReadPacketDoesNotOversizeTheBuffer(t *testing.T) {
	for _, size := range []int{firstStepSize + 1, 300000, 2*firstStepSize - 1, 2 * firstStepSize, 2*firstStepSize + 1, 1<<20 + 1, 5 << 20} {
		var buf bytes.Buffer
		c := newReadTestConn(mysqlPacket(0, make([]byte, size)), mysql.MYSQL_COMPRESS_NONE)
		require.NoError(t, c.ReadPacketTo(&buf))
		require.Equal(t, size, buf.Len())
		require.LessOrEqual(t, buf.Cap(), size+size/2, "a payload of %d bytes got a buffer of %d", size, buf.Cap())
	}
}

// The connection can end a compressed frame anywhere, including just before the end of the first step, exactly on it, or just
// after. The read must come out the same whichever it is, and leave the connection in step.
func TestReadPacketLargePayloadAcrossCompressedFrames(t *testing.T) {
	endOfFirstStep := 4 + firstStepSize // the packet's header, then the first step
	next := mysqlPacket(1, []byte("the packet after"))

	frameKinds := []struct {
		name        string
		compression uint8
		frame       func(*testing.T, byte, []byte) []byte
	}{
		{"zlib", mysql.MYSQL_COMPRESS_ZLIB, zlibFrame},
		{"zstd", mysql.MYSQL_COMPRESS_ZSTD, zstdFrame},
		{"stored", mysql.MYSQL_COMPRESS_ZLIB, func(_ *testing.T, seq byte, body []byte) []byte { return uncompressedFrame(seq, body) }},
	}
	cuts := []struct {
		name                  string
		firstFrame, frameSize int
	}{
		{"one frame", 0, 0},
		{"first frame ends before the first step does", endOfFirstStep - 1, 0},
		{"first frame ends with the first step", endOfFirstStep, 0},
		{"first frame ends after the first step does", endOfFirstStep + 1, 0},
		{"frames of 64 KiB", 0, 64 << 10},
		{"frames of 100000 bytes", 0, 100000},
	}

	random := rand.New(rand.NewSource(1))
	for _, size := range []int{300000, 1 << 20} { // one step, and two
		payload := make([]byte, size)
		random.Read(payload)
		data := append(mysqlPacket(0, payload), next...)

		for _, kind := range frameKinds {
			for _, cut := range cuts {
				t.Run(kind.name+"/"+cut.name+"/"+strconv.Itoa(size), func(t *testing.T) {
					c := newReadTestConn(framesOf(t, kind.frame, data, cut.firstFrame, cut.frameSize), kind.compression)

					got, err := c.ReadPacket()
					require.NoError(t, err)
					require.True(t, bytes.Equal(payload, got), "the payload came back changed")

					got, err = c.ReadPacket()
					require.NoError(t, err, "the connection is out of step after the large packet")
					require.Equal(t, []byte("the packet after"), got)
				})
			}
		}
	}
}

// The peer can stop sending during either step of the read.
func TestReadPacketLargePayloadTruncated(t *testing.T) {
	for _, delivered := range []int{100, firstStepSize + 100} {
		stream := append([]byte{0x00, 0x00, 0x10, 0x00}, make([]byte, delivered)...) // announces 1 MiB

		_, err := newReadTestConn(stream, mysql.MYSQL_COMPRESS_NONE).ReadPacket()
		require.ErrorIs(t, err, mysql.ErrBadConn, "delivered %d bytes", delivered)
	}
}

// BenchmarkReadPacketBySize times ReadPacket alone: the Conn is built once and is fed the same recorded packet for every read.
func BenchmarkReadPacketBySize(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{"4KiB", 4 << 10},
		{"64KiB", 64 << 10},
		{"256KiB", firstStepSize},
		{"256KiB+1B", firstStepSize + 1},
		{"512KiB", 2 * firstStepSize},
		{"512KiB+1B", 2*firstStepSize + 1}, // the smallest packet that is read in two steps
		{"1MiB", 1 << 20},
		{"4MiB", 4 << 20},
		{"16MiB-1B", mysql.MaxPayloadLen},
		{"16MiB+5B", 16<<20 + 5}, // two fragments
	} {
		packet := fragmentedPacket(make([]byte, tc.size))

		b.Run(tc.name, func(b *testing.B) {
			replay := &replayConn{}
			c := NewConn(replay)
			b.ReportAllocs()
			b.SetBytes(int64(tc.size))
			b.ResetTimer()
			for range b.N {
				replay.stream.Reset(packet)
				c.Sequence = 0
				if _, err := c.ReadPacket(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// replayConn serves the same recorded bytes again and again.
type replayConn struct {
	net.Conn
	stream bytes.Reader
}

func (c *replayConn) Read(p []byte) (int, error) { return c.stream.Read(p) }
