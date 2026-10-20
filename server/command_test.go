package server

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	mockconn "github.com/go-mysql-org/go-mysql/test_util/conn"
	"github.com/stretchr/testify/require"
)

// Ensure EmptyHandler implements Handler interface or cause compile time error
var (
	_ Handler            = EmptyHandler{}
	_ ReplicationHandler = EmptyReplicationHandler{}
	_ MultiResultHandler = (*multiResultHandler)(nil)
)

// multiResultHandler answers a query of ';'-separated statements with one
// result per statement, stopping at the first that fails:
//
//	ok      an OK result with 1 affected row
//	rows    a resultset with one row
//	warn    a resultset with one row, ended with a warning count of 2 (the
//	        client does not read it back; TestWriteMoreResultSetsStatusOnTerminator
//	        checks the bytes)
//	fail    error 1146
//	stream  a streamed resultset that fails after its first row
type multiResultHandler struct {
	EmptyHandler
	conn *Conn

	mu       sync.Mutex
	executed []string
	moreErr  error
}

func (h *multiResultHandler) HandleQuery(query string) (*mysql.Result, error) {
	return h.HandleMultiQuery(query, nil)
}

func (h *multiResultHandler) HandleMultiQuery(query string, more func(*mysql.Result) error) (*mysql.Result, error) {
	statements := strings.Split(query, ";")
	if more == nil {
		statements = []string{query}
	}
	for i, statement := range statements {
		h.mu.Lock()
		h.executed = append(h.executed, statement)
		h.mu.Unlock()
		h.conn.SetWarnings(0)
		r, err := h.execute(statement)
		if err != nil {
			return nil, err
		}
		if i == len(statements)-1 {
			return r, nil
		}
		if err := more(r); err != nil {
			h.mu.Lock()
			h.moreErr = err
			h.mu.Unlock()
			return nil, err
		}
	}
	return nil, errors.New("empty query")
}

func (h *multiResultHandler) execute(statement string) (*mysql.Result, error) {
	switch statement {
	case "ok":
		return &mysql.Result{AffectedRows: 1}, nil
	case "rows", "warn":
		if statement == "warn" {
			h.conn.SetWarnings(2)
		}
		rs, err := mysql.BuildSimpleTextResultset([]string{"a"}, [][]any{{statement}})
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(rs), nil
	case "stream":
		sr := mysql.NewStreamResult(makeFields("a"), 1, false)
		go func() {
			sr.WriteRow(context.Background(), []any{"first"})
			sr.SetError(mysql.NewError(mysql.ER_QUERY_INTERRUPTED, "stream failed"))
			sr.Close()
		}()
		return sr.AsResult(), nil
	default:
		return nil, mysql.NewError(mysql.ER_NO_SUCH_TABLE, "no such table: "+statement)
	}
}

func (h *multiResultHandler) state() ([]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.executed...), h.moreErr
}

func (h *multiResultHandler) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.executed = nil
	h.moreErr = nil
}

// serveMultiResult serves one connection with a multiResultHandler and returns
// a client connected to it. The server advertises CLIENT_MULTI_STATEMENTS.
func serveMultiResult(t *testing.T) (*client.Conn, *multiResultHandler) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })

	svr := NewDefaultServer()
	require.NoError(t, svr.SetCapability(mysql.CLIENT_MULTI_STATEMENTS))
	auth := NewInMemoryAuthenticationHandler()
	require.NoError(t, auth.AddUser("root", ""))

	h := &multiResultHandler{}
	ready := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() { <-done })
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			close(ready)
			return
		}
		co, err := svr.NewCustomizedConn(conn, auth, h)
		h.conn = co
		close(ready)
		if err != nil {
			return
		}
		//nolint:revive // loop drains commands; work is in the condition
		for co.HandleCommand() == nil {
		}
	}()
	c, err := client.Connect(l.Addr().String(), "root", "", "", func(c *client.Conn) error {
		return c.SetCapability(mysql.CLIENT_MULTI_STATEMENTS)
	})
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	<-ready
	return c, h
}

type multiResponse struct {
	results []*mysql.Result
	err     error
}

func executeMultiple(t *testing.T, c *client.Conn, query string) multiResponse {
	t.Helper()
	var response multiResponse
	_, err := c.ExecuteMultiple(query, func(r *mysql.Result, err error) {
		if err != nil {
			response.err = err
			return
		}
		response.results = append(response.results, r)
	})
	require.NoError(t, err)
	return response
}

func TestMultiResultHandlerAnswersEachStatement(t *testing.T) {
	c, h := serveMultiResult(t)
	require.True(t, c.HasCapability(mysql.CLIENT_MULTI_STATEMENTS))

	response := executeMultiple(t, c, "ok;rows;warn;ok")
	require.NoError(t, response.err)
	require.Len(t, response.results, 4)
	for i, r := range response.results {
		more := r.Status&mysql.SERVER_MORE_RESULTS_EXISTS != 0
		require.Equal(t, i < 3, more, "result %d: SERVER_MORE_RESULTS_EXISTS", i)
	}
	require.EqualValues(t, 1, response.results[0].AffectedRows)
	require.Equal(t, [][]any{{[]byte("rows")}}, resultValues(response.results[1]))
	require.Equal(t, [][]any{{[]byte("warn")}}, resultValues(response.results[2]))
	require.EqualValues(t, 1, response.results[3].AffectedRows)

	executed, moreErr := h.state()
	require.Equal(t, []string{"ok", "rows", "warn", "ok"}, executed)
	require.NoError(t, moreErr)

	// The status bit does not outlive the response.
	r, err := c.Execute("ok")
	require.NoError(t, err)
	require.Zero(t, r.Status&mysql.SERVER_MORE_RESULTS_EXISTS)
}

func TestMultiResultHandlerStopsAtFirstError(t *testing.T) {
	c, h := serveMultiResult(t)

	response := executeMultiple(t, c, "ok;rows;fail;ok")
	require.Len(t, response.results, 2)
	var myErr *mysql.MyError
	require.ErrorAs(t, response.err, &myErr)
	require.EqualValues(t, mysql.ER_NO_SUCH_TABLE, myErr.Code)
	executed, _ := h.state()
	require.Equal(t, []string{"ok", "rows", "fail"}, executed)

	// The connection is in step for the next command.
	h.reset()
	r, err := c.Execute("ok")
	require.NoError(t, err)
	require.EqualValues(t, 1, r.AffectedRows)
}

func TestMultiResultHandlerSingleResultIsHandleQuery(t *testing.T) {
	c, _ := serveMultiResult(t)

	r, err := c.Execute("rows")
	require.NoError(t, err)
	require.Zero(t, r.Status&mysql.SERVER_MORE_RESULTS_EXISTS)
	require.Equal(t, [][]any{{[]byte("rows")}}, resultValues(r))

	_, err = c.Execute("fail")
	var myErr *mysql.MyError
	require.ErrorAs(t, err, &myErr)
	require.EqualValues(t, mysql.ER_NO_SUCH_TABLE, myErr.Code)
}

func TestMultiResultHandlerFailedStreamEndsResponse(t *testing.T) {
	c, h := serveMultiResult(t)

	response := executeMultiple(t, c, "ok;stream;ok")
	require.Len(t, response.results, 1)
	var myErr *mysql.MyError
	require.ErrorAs(t, response.err, &myErr)
	require.EqualValues(t, mysql.ER_QUERY_INTERRUPTED, myErr.Code)

	executed, moreErr := h.state()
	require.Equal(t, []string{"ok", "stream"}, executed)
	require.ErrorIs(t, moreErr, errStreamAnswered)

	h.reset()
	r, err := c.Execute("ok")
	require.NoError(t, err)
	require.EqualValues(t, 1, r.AffectedRows)
}

func TestMultiResultHandlerWriteFailureEndsCommand(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}
	h := &multiResultHandler{conn: conn}
	clientConn.Closed = true

	v := conn.handleMultiQuery(h, "ok;ok")
	failed, ok := v.(writeFailed)
	require.True(t, ok, "response %T, want writeFailed", v)
	require.ErrorContains(t, conn.WriteValue(v), "connection closed")
	executed, moreErr := h.state()
	require.Equal(t, []string{"ok"}, executed)
	require.ErrorIs(t, moreErr, failed.err)
}

func TestWriteMoreResultSetsStatusOnTerminator(t *testing.T) {
	for _, deprecateEOF := range []bool{false, true} {
		capability := mysql.CLIENT_PROTOCOL_41
		if deprecateEOF {
			capability |= mysql.CLIENT_DEPRECATE_EOF
		}
		clientConn := &mockconn.MockConn{MultiWrite: true}
		conn := &Conn{
			Conn:       packet.NewConn(clientConn),
			serverConf: &Server{capability: capability},
			capability: capability,
		}
		conn.SetStatus(mysql.SERVER_STATUS_AUTOCOMMIT)
		conn.SetWarnings(2)

		rs, err := mysql.BuildSimpleTextResultset([]string{"a"}, [][]any{{"b"}})
		require.NoError(t, err)
		require.NoError(t, conn.writeMoreResult(mysql.NewResult(rs)))
		// The terminator is the last packet written: a 5-byte EOF (header,
		// warnings, status) or a 7-byte EOF-headered OK (header, affected
		// rows, insert id, status, warnings).
		status := mysql.SERVER_STATUS_AUTOCOMMIT | mysql.SERVER_MORE_RESULTS_EXISTS
		want := []byte{5, 0, 0, 4, mysql.EOF_HEADER, 2, 0, byte(status), 0}
		if deprecateEOF {
			want = []byte{7, 0, 0, 3, mysql.EOF_HEADER, 0, 0, byte(status), 0, 2, 0}
		}
		require.Equal(t, want, clientConn.WriteBuffered[len(clientConn.WriteBuffered)-len(want):], "deprecateEOF=%v", deprecateEOF)
		require.Equal(t, mysql.SERVER_STATUS_AUTOCOMMIT, conn.status, "deprecateEOF=%v", deprecateEOF)

		// An OK result that follows continues the response's sequence.
		clientConn.WriteBuffered = nil
		require.NoError(t, conn.writeMoreResult(&mysql.Result{AffectedRows: 3, Warnings: 1}))
		seq := byte(5)
		if deprecateEOF {
			seq = 4
		}
		require.Equal(t, []byte{7, 0, 0, seq, mysql.OK_HEADER, 3, 0, byte(status), 0, 1, 0}, clientConn.WriteBuffered, "deprecateEOF=%v", deprecateEOF)
	}
}

func resultValues(r *mysql.Result) [][]any {
	values := make([][]any, len(r.Values))
	for i, row := range r.Values {
		values[i] = make([]any, len(row))
		for j, v := range row {
			values[i][j] = v.Value()
		}
	}
	return values
}
