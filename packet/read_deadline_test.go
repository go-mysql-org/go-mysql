package packet

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// deadlineConn records the timeout scope without relying on wall-clock timing.
type deadlineConn struct {
	net.Conn
	input       *bytes.Reader
	maxRead     int
	reads       int
	deadlines   []time.Time
	deadlineErr error
	beforeRead  func()
}

func (c *deadlineConn) Read(p []byte) (int, error) {
	c.reads++
	if c.beforeRead != nil {
		c.beforeRead()
	}
	if c.maxRead > 0 && len(p) > c.maxRead {
		p = p[:c.maxRead]
	}
	return c.input.Read(p)
}

func (c *deadlineConn) SetReadDeadline(d time.Time) error {
	c.deadlines = append(c.deadlines, d)
	return c.deadlineErr
}

func deadlinePacket(seq byte, payload []byte) []byte {
	n := len(payload)
	return append([]byte{byte(n), byte(n >> 8), byte(n >> 16), seq}, payload...)
}

func TestReadDeadlineBufferedPackets(t *testing.T) {
	var wire []byte
	for i := range 64 {
		wire = append(wire, deadlinePacket(byte(i), []byte("test"))...)
	}
	raw := &deadlineConn{input: bytes.NewReader(wire)}
	c := NewConnWithTimeout(raw, time.Minute, 0, 4096)
	for range 64 {
		got, err := c.ReadPacket()
		require.NoError(t, err)
		require.Equal(t, []byte("test"), got)
	}
	require.Equal(t, 1, raw.reads)
	require.Len(t, raw.deadlines, 1)
	// The next packet needs a network read, so it must renew the deadline.
	_, err := c.ReadPacket()
	require.Error(t, err)
	require.Len(t, raw.deadlines, 2)
}

func TestReadDeadlinePartialReads(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		name := "unbuffered"
		if buffered {
			name = "buffered"
		}
		t.Run(name, func(t *testing.T) {
			raw := &deadlineConn{input: bytes.NewReader(deadlinePacket(0, []byte("test"))), maxRead: 1}
			var c *Conn
			if buffered {
				c = NewConnWithTimeout(raw, time.Minute, 0, 16)
			} else {
				c = NewTLSConnWithTimeout(raw, time.Minute, 0)
			}
			raw.beforeRead = func() {
				// Four partial reads of the header share one deadline, as do the body.
				require.Len(t, raw.deadlines, 1+(raw.reads-1)/4)
			}
			got, err := c.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, []byte("test"), got)
			require.Equal(t, 8, raw.reads)
			require.Len(t, raw.deadlines, 2)
		})
	}
}

func TestReadDeadlineDisabledAndFailure(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		raw := &deadlineConn{input: bytes.NewReader(deadlinePacket(0, []byte("test")))}
		_, err := NewConnWithTimeout(raw, 0, 0, 4096).ReadPacket()
		require.NoError(t, err)
		require.Empty(t, raw.deadlines)
	})
	t.Run("failure", func(t *testing.T) {
		raw := &deadlineConn{input: bytes.NewReader(nil), deadlineErr: errors.New("deadline refused")}
		_, err := NewConnWithTimeout(raw, time.Minute, 0, 4096).ReadPacket()
		require.ErrorContains(t, err, "deadline refused")
		require.Zero(t, raw.reads)
	})
}

func TestReadDeadlineCompressedPathUnchanged(t *testing.T) {
	// A verbatim compressed-protocol frame still follows the existing conservative
	// timeout path: the compressed reader's buffered availability is unknown.
	body := deadlinePacket(0, []byte("test"))
	frame := append([]byte{byte(len(body)), 0, 0, 0, 0, 0, 0}, body...)
	raw := &deadlineConn{input: bytes.NewReader(frame)}
	c := NewConnWithTimeout(raw, time.Minute, 0, 4096)
	c.Compression = mysql.MYSQL_COMPRESS_ZLIB
	got, err := c.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, []byte("test"), got)
	require.Len(t, raw.deadlines, 3)
}

func TestReadDeadlineIdleAndPartialPacket(t *testing.T) {
	for name, prefix := range map[string][]byte{"idle": nil, "partial header": {4, 0}, "partial body": {4, 0, 0, 0, 'a'}} {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			c := NewConnWithTimeout(client, 50*time.Millisecond, 0, 4096)
			writer := make(chan struct{})
			go func() {
				defer close(writer)
				if len(prefix) > 0 {
					_, _ = server.Write(prefix)
				}
			}()
			started := time.Now()
			_, err := c.ReadPacket()
			require.ErrorContains(t, err, "timeout")
			require.Less(t, time.Since(started), 2*time.Second)
			<-writer
		})
	}
}

func TestReadDeadlineBufferedDataAfterExpiry(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := NewConnWithTimeout(client, 50*time.Millisecond, 0, 4096)
	go func() { _, _ = server.Write(append(deadlinePacket(0, []byte("a")), deadlinePacket(1, []byte("b"))...)) }()
	_, err := c.ReadPacket()
	require.NoError(t, err)
	// A deadline concerns the underlying I/O, not data already buffered in memory.
	require.NoError(t, client.SetReadDeadline(time.Now().Add(-time.Second)))
	got, err := c.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, []byte("b"), got)
	_, err = c.ReadPacket()
	require.ErrorContains(t, err, "timeout")
}

func TestReadDeadlineCloseUnblocks(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	c := NewConnWithTimeout(client, time.Minute, 0, 4096)
	done := make(chan error, 1)
	go func() { _, err := c.ReadPacket(); done <- err }()
	require.NoError(t, c.Close())
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock ReadPacket")
	}
}

// Client authentication replaces the pre-handshake unbuffered packet.Conn with
// a normal buffered packet.Conn over tls.Conn. Exercise that exact read stack.
type deadlineCountingConn struct {
	net.Conn
	deadlines int
}

func (c *deadlineCountingConn) SetReadDeadline(d time.Time) error {
	c.deadlines++
	return c.Conn.SetReadDeadline(d)
}

func TestReadDeadlineTLS(t *testing.T) {
	certServer := httptest.NewTLSServer(nil)
	defer certServer.Close()
	roots := x509.NewCertPool()
	roots.AddCert(certServer.Certificate())
	rawClient, rawServer := net.Pipe()
	defer rawClient.Close()
	defer rawServer.Close()
	server := tls.Server(rawServer, certServer.TLS)
	client := tls.Client(rawClient, &tls.Config{RootCAs: roots, ServerName: certServer.Certificate().DNSNames[0]})
	serverDone := make(chan error, 1)
	go func() {
		if err := server.Handshake(); err != nil {
			serverDone <- err
			return
		}
		var wire []byte
		for i := range 64 {
			wire = append(wire, deadlinePacket(byte(i), []byte("test"))...)
		}
		_, err := server.Write(wire)
		serverDone <- err
	}()
	require.NoError(t, client.Handshake())
	counted := &deadlineCountingConn{Conn: client}
	c := NewConnWithTimeout(counted, 50*time.Millisecond, 0, 4096)
	for range 64 {
		got, err := c.ReadPacket()
		require.NoError(t, err)
		require.Equal(t, []byte("test"), got)
	}
	require.NoError(t, <-serverDone)
	require.Equal(t, 1, counted.deadlines)
	_, err := c.ReadPacket()
	require.ErrorContains(t, err, "timeout")
	require.Equal(t, 2, counted.deadlines)
}
