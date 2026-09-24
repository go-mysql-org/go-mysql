package client

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
)

func captureAttributeRequest(t *testing.T, c *Conn, peer *packet.Conn, send func() error) []byte {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- send() }()
	peer.ResetSequence()
	payload, err := peer.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.WritePacket([]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return payload
}

func attributeConnection(t *testing.T) (*Conn, *packet.Conn) {
	a, b := net.Pipe()
	_ = a.SetDeadline(time.Now().Add(5 * time.Second))
	_ = b.SetDeadline(time.Now().Add(5 * time.Second))
	t.Cleanup(func() { a.Close(); b.Close() })
	return &Conn{Conn: packet.NewConn(a), capability: mysql.CLIENT_QUERY_ATTRIBUTES, includeLine: -1}, packet.NewConn(b)
}

func TestQueryAttributeAttributesConsumed(t *testing.T) {
	c, p := attributeConnection(t)
	if err := c.SetQueryAttributes(mysql.QueryAttribute{Name: "trace", Value: "first"}); err != nil {
		t.Fatal(err)
	}
	first := captureAttributeRequest(t, c, p, func() error { _, err := c.Execute("SELECT 1"); return err })
	second := captureAttributeRequest(t, c, p, func() error { _, err := c.Execute("SELECT 2"); return err })
	t.Logf("first=%x second=%x", first, second)
	if second[1] != 0 {
		t.Fatalf("next query still announces %d attributes; want zero", second[1])
	}
}

func TestQueryAttributeZeroParameterAttributes(t *testing.T) {
	c, p := attributeConnection(t)
	if err := c.SetQueryAttributes(mysql.QueryAttribute{Name: "trace", Value: "marker"}); err != nil {
		t.Fatal(err)
	}
	s := &Stmt{conn: c}
	data := captureAttributeRequest(t, c, p, func() error { _, err := s.Execute(); return err })
	t.Logf("execute=%x", data)
	if !bytes.Contains(data, []byte("marker")) {
		t.Fatal("zero-parameter execution lost its query attribute")
	}
}

func TestQueryAttributeNullParameterAttributes(t *testing.T) {
	c, p := attributeConnection(t)
	if err := c.SetQueryAttributes(mysql.QueryAttribute{Name: "trace", Value: "marker"}); err != nil {
		t.Fatal(err)
	}
	s := &Stmt{conn: c}
	s.Params = 1
	data := captureAttributeRequest(t, c, p, func() error { _, err := s.Execute(nil); return err })
	t.Logf("execute=%x", data)
	if !bytes.Contains(data, []byte("marker")) {
		t.Fatal("NULL-parameter execution lost its query attribute")
	}
}

func TestQueryAttributeCallerAttributesReusable(t *testing.T) {
	c, p := attributeConnection(t)
	attrs := []mysql.QueryAttribute{{Name: "trace", Value: "marker"}}
	if err := c.SetQueryAttributes(attrs...); err != nil {
		t.Fatal(err)
	}
	captureAttributeRequest(t, c, p, func() error { _, err := c.Execute("SELECT 1"); return err })
	if attrs[0].Name != "trace" || attrs[0].Value != "marker" {
		t.Fatal("sending query mutated caller attribute slice")
	}
}

func TestQueryAttributeLineAttributeDoesNotAccumulate(t *testing.T) {
	c, p := attributeConnection(t)
	c.IncludeLine(0)
	for i := range 3 {
		data := captureAttributeRequest(t, c, p, func() error { _, err := c.Execute("SELECT 1"); return err })
		if data[1] != 1 {
			t.Fatalf("query %d has %d attributes; want one line attribute", i, data[1])
		}
	}
}

func TestQueryAttributeUnsupportedCapability(t *testing.T) {
	c, p := attributeConnection(t)
	c.capability = 0
	if err := c.SetQueryAttributes(mysql.QueryAttribute{Name: "trace", Value: "marker"}); err != nil {
		t.Fatal(err)
	}
	s := &Stmt{conn: c}
	s.Params = 1
	data := captureAttributeRequest(t, c, p, func() error { _, err := s.Execute(7); return err })
	if data[5] != 0 {
		t.Fatalf("legacy execute has unsupported flags %x", data[5])
	}
	if bytes.Contains(data, []byte("marker")) {
		t.Fatal("attributes encoded without negotiated support")
	}
}

func TestQueryAttributeLineDoesNotOverwriteCallerCapacity(t *testing.T) {
	c, p := attributeConnection(t)
	c.IncludeLine(0)
	attrs := []mysql.QueryAttribute{{Name: "trace", Value: "marker"}, {Name: "reserved", Value: "unchanged"}}
	if err := c.SetQueryAttributes(attrs[:1]...); err != nil {
		t.Fatal(err)
	}
	data := captureAttributeRequest(t, c, p, func() error { _, err := c.Execute("SELECT 1"); return err })
	if attrs[1].Name != "reserved" || attrs[1].Value != "unchanged" {
		t.Fatal("automatic line attribute overwrote caller-owned backing array")
	}
	if data[1] != 2 || !bytes.Contains(data, []byte("_line")) {
		t.Fatal("automatic line attribute was not sent alongside the caller attribute")
	}
}
