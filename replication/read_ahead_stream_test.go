package replication

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/stretchr/testify/require"
)

// Both transports feed the same packet.Conn, stream loop and test assertions.
func readAheadTransport(t *testing.T, encrypted bool) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, e := listener.Accept()
		if e == nil {
			accepted <- c
		}
	}()
	raw, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	peer := <-accepted
	t.Cleanup(func() { _ = raw.Close(); _ = peer.Close() })
	if !encrypted {
		return raw, peer
	}
	certServer := httptest.NewTLSServer(nil)
	t.Cleanup(certServer.Close)
	roots := x509.NewCertPool()
	roots.AddCert(certServer.Certificate())
	c := tls.Client(raw, &tls.Config{RootCAs: roots, ServerName: certServer.Certificate().DNSNames[0]})
	s := tls.Server(peer, certServer.TLS)
	done := make(chan error, 1)
	go func() { done <- s.Handshake() }()
	require.NoError(t, c.Handshake())
	require.NoError(t, <-done)
	require.NotEmpty(t, c.ConnectionState().VerifiedChains)
	return c, s
}

func readAheadHeartbeat(seq byte, pos uint32) []byte {
	p := make([]byte, 4+1+EventHeaderSize)
	p[0] = byte(len(p) - 4)
	p[3] = seq
	p[5+4] = byte(HEARTBEAT_EVENT)
	binary.LittleEndian.PutUint32(p[5+9:], EventHeaderSize)
	binary.LittleEndian.PutUint32(p[5+13:], pos)
	return p
}

func startReadAheadStream(t *testing.T, c net.Conn, budget int, timeout time.Duration) (*BinlogSyncer, *BinlogStreamer) {
	t.Helper()
	b := NewBinlogSyncer(BinlogSyncerConfig{ServerID: 1, ReadAheadBufferSize: budget, ReadTimeout: timeout, DisableRetrySync: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	b.c = &client.Conn{Conn: packet.NewConnWithTimeout(c, timeout, 0, 65536)}
	b.nextPos = mysql.Position{Name: "mysql-bin.000001", Pos: 4}
	s := NewBinlogStreamerWithChanSize(1)
	b.wg.Add(1)
	go b.onStream(s)
	t.Cleanup(b.Close)
	return b, s
}

func TestReadAheadTransportOrderAndTimeout(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, budget := range []int{0, 1024} {
			t.Run(fmt.Sprintf("tls=%t/budget=%d", encrypted, budget), func(t *testing.T) {
				c, peer := readAheadTransport(t, encrypted)
				_, s := startReadAheadStream(t, c, budget, 200*time.Millisecond)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				writer := make(chan error, 1)
				go func() {
					for i := range 6 {
						if _, err := peer.Write(readAheadHeartbeat(byte(i), uint32(100+i))); err != nil {
							writer <- err
							return
						}
						time.Sleep(20 * time.Millisecond)
					}
					writer <- nil
				}()
				var held []*BinlogEvent
				for i := range 6 {
					e, err := s.GetEvent(ctx)
					require.NoError(t, err)
					require.Equal(t, HEARTBEAT_EVENT, e.Header.EventType)
					require.EqualValues(t, 100+i, e.Header.LogPos)
					held = append(held, e)
					if i == 0 {
						time.Sleep(250 * time.Millisecond)
					} // downstream stall longer than ReadTimeout
				}
				require.NoError(t, <-writer)
				for i, e := range held {
					require.EqualValues(t, 100+i, binary.LittleEndian.Uint32(e.RawData[13:]))
				}
				_, err := s.GetEvent(ctx)
				require.ErrorContains(t, err, "timeout")
				require.NotErrorIs(t, err, context.DeadlineExceeded)
			})
		}
	}
}

type rejectReadDeadlineConn struct{ net.Conn }

func (c rejectReadDeadlineConn) SetReadDeadline(time.Time) error {
	return fmt.Errorf("deadlines unsupported")
}

func TestReadAheadTransportClose(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, reject := range []bool{false, true} {
			for _, kind := range []string{"idle", "partial-header", "partial-body", "backpressure"} {
				t.Run(fmt.Sprintf("tls=%t/reject-deadline=%t/%s", encrypted, reject, kind), func(t *testing.T) {
					c, peer := readAheadTransport(t, encrypted)
					if reject {
						c = rejectReadDeadlineConn{c}
					}
					b, s := startReadAheadStream(t, c, 32, 0)
					switch kind {
					case "partial-header":
						_, err := peer.Write([]byte{4, 0})
						require.NoError(t, err)
					case "partial-body":
						_, err := peer.Write([]byte{4, 0, 0, 0, 'a'})
						require.NoError(t, err)
					case "backpressure":
						var wire []byte
						for i := range 100 {
							wire = append(wire, readAheadHeartbeat(byte(i), uint32(100+i))...)
						}
						_, err := peer.Write(wire)
						require.NoError(t, err)
						require.Eventually(t, func() bool { return len(s.ch) == 1 }, time.Second, time.Millisecond)
					}
					done := make(chan struct{})
					go func() { b.Close(); close(done) }()
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Fatal("Close did not join the reader")
					}
					b.Close()
				})
			}
		}
	}
}

// A blocked consumer must not refresh the producer's already-running deadline.
func TestReadAheadCancellationRejectsBufferedPackets(t *testing.T) {
	s := testSource()
	for range 10 {
		s.packets <- queuedPacket{data: []byte("buffered")}
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := newPacketReadAhead(ctx, s, 1024)
	defer r.close()
	_, err := r.readPacket()
	require.NoError(t, err)
	cancel()
	_, err = r.readPacket()
	require.ErrorIs(t, err, context.Canceled)
}

// Semi-sync must retain one owner for packet sequence numbers and ACK writes.
// Even when the prefetch switch is positive it must not read the next packet
// while the synchronous event handler is processing the current event.
type readAheadObservedConn struct {
	net.Conn
	reads atomic.Int32
}

func (c *readAheadObservedConn) Read(p []byte) (int, error) { c.reads.Add(1); return c.Conn.Read(p) }

type readAheadBlockingHandler struct{ entered, release chan struct{} }

func (h readAheadBlockingHandler) HandleEvent(*BinlogEvent) error {
	close(h.entered)
	<-h.release
	return nil
}

func TestReadAheadSemiSyncFallback(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%t", encrypted), func(t *testing.T) {
			c, peer := readAheadTransport(t, encrypted)
			observed := &readAheadObservedConn{Conn: c}
			entered, release := make(chan struct{}), make(chan struct{})
			b := NewBinlogSyncer(BinlogSyncerConfig{ServerID: 1, SemiSyncEnabled: true, ReadAheadBufferSize: 1024, SynchronousEventHandler: readAheadBlockingHandler{entered, release}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			b.c = &client.Conn{Conn: packet.NewBufferedConn(observed, 65536)}
			s := NewBinlogStreamer()
			b.wg.Add(1)
			go b.onStream(s)
			t.Cleanup(b.Close)
			wire := readAheadHeartbeat(0, 100)
			wire = append(wire[:5], append([]byte{SemiSyncIndicator, 1}, wire[5:]...)...)
			wire[0] += 2
			_, err := peer.Write(wire)
			require.NoError(t, err)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("handler not called")
			}
			reads := observed.reads.Load()
			time.Sleep(20 * time.Millisecond)
			require.Equal(t, reads, observed.reads.Load())
			close(release)
			require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
			ackReader := packet.NewConn(peer)
			ack, err := ackReader.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, SemiSyncIndicator, ack[0])
			require.EqualValues(t, 100, binary.LittleEndian.Uint64(ack[1:9]))
		})
	}
}
