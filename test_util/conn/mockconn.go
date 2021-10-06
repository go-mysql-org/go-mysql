package conn

import (
	"errors"
	"net"
	"time"
)

type mockAddr struct{}

func (m mockAddr) String() string  { return "mocking" }
func (m mockAddr) Network() string { return "mocking" }

// MockConn is a simple struct implementing net.Conn that allows us to read what
// was written to it and feed data it will read from
type MockConn struct {
	readResponses [][]byte
	WriteBuffered []byte
	Closed        bool

	MultiWrite bool
}

func (m *MockConn) SetResponse(r [][]byte) {
	m.readResponses = r
}

func (m *MockConn) Read(p []byte) (n int, err error) {
	if m.Closed {
		return -1, errors.New("connection closed")
	}

	if len(m.readResponses) == 0 {
		return -1, errors.New("no response left")
	}

	copy(p, m.readResponses[0])
	m.readResponses = m.readResponses[1:]

	return len(p), nil
}

func (m *MockConn) Write(p []byte) (n int, err error) {
	if m.Closed {
		return -1, errors.New("connection closed")
	}

	if m.MultiWrite {
		m.WriteBuffered = append(m.WriteBuffered, p...)
	} else {
		m.WriteBuffered = make([]byte, len(p))
		copy(m.WriteBuffered, p)
	}

	return len(p), nil
}

func (m MockConn) LocalAddr() net.Addr  { return mockAddr{} }
func (m MockConn) RemoteAddr() net.Addr { return mockAddr{} }

func (m *MockConn) Close() error {
	m.Closed = true

	return nil
}

func (m MockConn) SetDeadline(t time.Time) error {
	return errors.New("not implemented")
}

func (m MockConn) SetReadDeadline(t time.Time) error {
	return errors.New("not implemented")
}

func (m MockConn) SetWriteDeadline(t time.Time) error {
	return errors.New("not implemented")
}
