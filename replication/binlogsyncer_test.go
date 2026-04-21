package replication

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/packet"
)

func TestLocalHostname(t *testing.T) {
	b := BinlogSyncer{
		cfg: BinlogSyncerConfig{
			Localhost: "foobar",
		},
	}

	require.Equal(t, "foobar", b.localHostname())
}

func TestLocalHostname_long(t *testing.T) {
	b := BinlogSyncer{
		cfg: BinlogSyncerConfig{
			Localhost: strings.Repeat("x", 255),
		},
	}

	require.Equal(t, 255, len(b.localHostname()))
}

func TestLocalHostname_toolong(t *testing.T) {
	b := BinlogSyncer{
		cfg: BinlogSyncerConfig{
			Localhost: strings.Repeat("x", 300),
		},
	}

	require.Equal(t, 255, len(b.localHostname()))
}

func TestLocalHostname_os(t *testing.T) {
	b := BinlogSyncer{
		cfg: BinlogSyncerConfig{
			Localhost: "",
		},
	}

	h, _ := os.Hostname()
	require.Equal(t, h, b.localHostname())
}

// deadlinelessConn mimics the net.Conn surface of an ssh tunneled channel:
// Read blocks until Close, and SetReadDeadline refuses with an error rather
// than interrupting the parked Read.
type deadlinelessConn struct {
	closeOnce sync.Once
	closed    chan struct{}
}

func newDeadlinelessConn() *deadlinelessConn {
	return &deadlinelessConn{closed: make(chan struct{})}
}

func (c *deadlinelessConn) Read(b []byte) (int, error) {
	<-c.closed
	return 0, io.EOF
}

func (c *deadlinelessConn) Write(b []byte) (int, error) { return len(b), nil }

func (c *deadlinelessConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (*deadlinelessConn) LocalAddr() net.Addr         { return &net.TCPAddr{} }
func (*deadlinelessConn) RemoteAddr() net.Addr        { return &net.TCPAddr{} }
func (*deadlinelessConn) SetDeadline(time.Time) error { return errors.New("deadline not supported") }
func (*deadlinelessConn) SetReadDeadline(time.Time) error {
	return errors.New("deadline not supported")
}

func (*deadlinelessConn) SetWriteDeadline(time.Time) error {
	return errors.New("deadline not supported")
}

// TestCloseUnblocksWhenSetReadDeadlineFails exercises the deadlock path where
// SetReadDeadline cannot unblock the binlog reader (e.g. ssh tunnel) and KILL
// also fails to reach the server (thread already reaped). Under the previous
// behaviour close() parked indefinitely on wg.Wait.
func TestCloseUnblocksWhenSetReadDeadlineFails(t *testing.T) {
	b := NewBinlogSyncer(BinlogSyncerConfig{ServerID: 1})

	fake := newDeadlinelessConn()
	b.c = &client.Conn{Conn: packet.NewConn(fake)}
	b.running = true

	// Mimic onStream's parked ReadPacket: block until the underlying conn is
	// closed, then honour ctx cancellation before signalling wg.Done.
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		_, _ = b.c.ReadPacket()
		<-b.ctx.Done()
	}()

	done := make(chan struct{})
	go func() {
		b.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung when SetReadDeadline refused a deadline")
	}
}
