package replication

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
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
	b.wg.Go(func() {
		_, _ = b.c.ReadPacket()
		<-b.ctx.Done()
	})

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

// TestHandleEventAndACKPayloadInnerGSet verifies XID/Query events decoded from
// a compressed transaction payload get the current GTID set attached, same as
// their uncompressed counterparts, so consumers (eg canal) can checkpoint GTID
// progress
func TestHandleEventAndACKPayloadInnerGSet(t *testing.T) {
	b := NewBinlogSyncer(BinlogSyncerConfig{ServerID: 1})
	defer b.Close()

	gset, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	require.NoError(t, err)
	b.currGset = gset

	innerQuery := &BinlogEvent{Header: &EventHeader{EventType: QUERY_EVENT}, Event: &QueryEvent{}}
	innerXID := &BinlogEvent{Header: &EventHeader{EventType: XID_EVENT}, Event: &XIDEvent{}}
	ev := &BinlogEvent{
		Header: &EventHeader{EventType: TRANSACTION_PAYLOAD_EVENT, LogPos: 5000, EventSize: 1200},
		Event:  &TransactionPayloadEvent{Events: []*BinlogEvent{innerQuery, innerXID}},
	}

	require.NoError(t, b.handleEventAndACK(NewBinlogStreamer(), ev, false))
	require.Equal(t, gset.String(), innerQuery.Event.(*QueryEvent).GSet.String())
	require.Equal(t, gset.String(), innerXID.Event.(*XIDEvent).GSet.String())
}

// TestHandleEventAndACKRotateLogDedup verifies the artificial rotate event the
// server sends right after the real one at each rotation does not produce a
// second "rotate to next binlog" log line.
func TestHandleEventAndACKRotateLogDedup(t *testing.T) {
	var buf bytes.Buffer
	b := NewBinlogSyncer(BinlogSyncerConfig{
		ServerID: 1,
		Logger:   slog.New(slog.NewTextHandler(&buf, nil)),
	})
	defer b.Close()

	rotate := func(logPos uint32, flags uint16, name string) *BinlogEvent {
		return &BinlogEvent{
			Header: &EventHeader{EventType: ROTATE_EVENT, LogPos: logPos, Flags: flags},
			Event:  &RotateEvent{NextLogName: []byte(name), Position: 4},
		}
	}

	s := NewBinlogStreamer()
	// Artificial rotate sent at dump start.
	require.NoError(t, b.handleEventAndACK(s, rotate(0, LOG_EVENT_ARTIFICIAL_F, "mysql-bin.000001"), false))
	// Real rotate at the end of the old file, then the artificial duplicate.
	require.NoError(t, b.handleEventAndACK(s, rotate(1234, 0, "mysql-bin.000002"), false))
	require.NoError(t, b.handleEventAndACK(s, rotate(0, LOG_EVENT_ARTIFICIAL_F, "mysql-bin.000002"), false))

	require.Equal(t, mysql.Position{Name: "mysql-bin.000002", Pos: 4}, b.GetNextPosition())
	require.Equal(t, 2, strings.Count(buf.String(), "rotate to next binlog"))
}

func TestBinlogSyncerConfigLogsAsJSON(t *testing.T) {
	var buf bytes.Buffer

	cfg := BinlogSyncerConfig{
		ServerID:                       100,
		Host:                           "127.0.0.1",
		Port:                           3306,
		User:                           "root",
		Password:                       "hunter2",
		Logger:                         slog.New(slog.NewJSONHandler(&buf, nil)),
		TLSConfig:                      &tls.Config{},
		Option:                         func(*client.Conn) error { return nil },
		Dialer:                         (&net.Dialer{}).DialContext,
		RowsEventDecodeFunc:            func(*RowsEvent, []byte) error { return nil },
		TableMapOptionalMetaDecodeFunc: func([]byte) error { return nil },
		SynchronousEventHandler:        NewBackupEventHandler(nil),
	}
	NewBinlogSyncer(cfg).Close()

	require.NotContains(t, buf.String(), "!ERROR")
	require.NotContains(t, buf.String(), "hunter2")
	require.Contains(t, buf.String(), `"ServerID":100`)
	require.Contains(t, buf.String(), `"Port":3306`)
}
