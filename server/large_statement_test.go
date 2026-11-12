package server

import (
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type recordingHandler struct {
	EmptyHandler
	queries chan string
}

func (h recordingHandler) HandleQuery(query string) (*mysql.Result, error) {
	h.queries <- query
	return &mysql.Result{}, nil
}

// A client sending a statement bigger than the server preallocates for, including one that arrives as more than one packet,
// must reach the handler whole, and leave the connection usable.
func TestServerReceivesLargeStatementsIntact(t *testing.T) {
	handler := recordingHandler{queries: make(chan string, 1)}
	authHandler := NewInMemoryAuthenticationHandler()
	require.NoError(t, authHandler.AddUser("user", "password"))
	s := NewDefaultServer()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		c, err := s.NewCustomizedConn(raw, authHandler, handler)
		if err != nil {
			return
		}
		for c.HandleCommand() == nil {
		}
	}()

	conn, err := client.Connect(listener.Addr().String(), "user", "password", "")
	require.NoError(t, err)
	defer conn.Close()

	for _, size := range []int{1000, 300 << 10, 1 << 20, 17 << 20} { // the last needs two packets
		query := "SELECT '" + strings.Repeat("x", size) + "'"
		_, err := conn.Execute(query)
		require.NoError(t, err, "a statement of %d bytes", len(query))
		require.True(t, query == <-handler.queries, "a statement of %d bytes reached the handler changed", len(query))
	}
}
