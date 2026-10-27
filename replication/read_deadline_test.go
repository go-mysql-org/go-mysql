package replication

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/packet"
)

type partialReadConn struct {
	net.Conn
	reads   int
	waiting chan struct{}
}

func (c *partialReadConn) Read(p []byte) (int, error) {
	c.reads++
	if c.reads == 2 {
		close(c.waiting)
	}
	return c.Conn.Read(p)
}

func TestReadDeadlineCloseDuringPartialPacket(t *testing.T) {
	for name, prefix := range map[string][]byte{"partial header": {4, 0}, "partial body": {4, 0, 0, 0, 'a'}} {
		t.Run(name, func(t *testing.T) {
			raw, peer := net.Pipe()
			defer raw.Close()
			defer peer.Close()
			b := NewBinlogSyncer(BinlogSyncerConfig{ServerID: 1, ReadTimeout: time.Minute})
			observed := &partialReadConn{Conn: raw, waiting: make(chan struct{})}
			b.c = &client.Conn{Conn: packet.NewConnWithTimeout(observed, time.Minute, 0, 4096)}
			b.running = true
			b.wg.Add(1)
			go b.onStream(NewBinlogStreamer())
			// Wait until the next underlying read, after setting the body deadline,
			// before closing. This specifically exercises waking an already parked read.
			_, err := peer.Write(prefix)
			require.NoError(t, err)
			<-observed.waiting
			closed := make(chan struct{})
			go func() { b.Close(); close(closed) }()
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("Close did not wake the partial packet read")
			}
		})
	}
}
