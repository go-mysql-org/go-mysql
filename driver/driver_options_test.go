package driver

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/log"
	"github.com/stretchr/testify/require"
)

var _ server.Handler = &mockHandler{}

type testServer struct {
	*server.Server

	listener net.Listener
	handler  *mockHandler
}

type mockHandler struct {
}

func TestDriverOptions_ConnectTimeout(t *testing.T) {
	log.SetLevel(log.LevelDebug)
	srv := CreateMockServer(t)
	defer srv.Stop()

	conn, err := sql.Open("mysql", "root@127.0.0.1:3307/test?timeout=1s")
	require.NoError(t, err)

	rows, err := conn.QueryContext(context.TODO(), "select * from table;")
	require.NotNil(t, rows)
	require.NoError(t, err)

	conn.Close()
}

func TestDriverOptions_ReadTimeout(t *testing.T) {
	log.SetLevel(log.LevelDebug)
	srv := CreateMockServer(t)
	defer srv.Stop()

	conn, err := sql.Open("mysql", "root@127.0.0.1:3307/test?readTimeout=1s")
	require.NoError(t, err)

	rows, err := conn.QueryContext(context.TODO(), "select * from slow;")
	require.Nil(t, rows)
	require.Error(t, err)

	rows, err = conn.QueryContext(context.TODO(), "select * from fast;")
	require.NotNil(t, rows)
	require.NoError(t, err)

	conn.Close()
}

func TestDriverOptions_writeTimeout(t *testing.T) {
	log.SetLevel(log.LevelDebug)
	srv := CreateMockServer(t)
	defer srv.Stop()

	conn, err := sql.Open("mysql", "root@127.0.0.1:3307/test?writeTimeout=10")
	require.NoError(t, err)

	result, err := conn.ExecContext(context.TODO(), "insert into slow(a,b) values(1,2);")
	require.Nil(t, result)
	require.Error(t, err)

	conn.Close()
}

func CreateMockServer(t *testing.T) *testServer {
	inMemProvider := server.NewInMemoryProvider()
	inMemProvider.AddUser(*testUser, *testPassword)
	defaultServer := server.NewDefaultServer()

	l, err := net.Listen("tcp", "127.0.0.1:3307")
	require.NoError(t, err)

	handler := &mockHandler{}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			go func() {
				co, err := server.NewCustomizedConn(conn, defaultServer, inMemProvider, handler)
				require.NoError(t, err)
				for {
					err = co.HandleCommand()
					if err != nil {
						return
					}
				}
			}()
		}
	}()

	return &testServer{
		Server:   defaultServer,
		listener: l,
		handler:  handler,
	}
}

func (s *testServer) Stop() {
	s.listener.Close()
}

func (h *mockHandler) UseDB(dbName string) error {
	return nil
}

func (h *mockHandler) handleQuery(query string, binary bool) (*mysql.Result, error) {
	ss := strings.Split(query, " ")
	switch strings.ToLower(ss[0]) {
	case "select":
		var r *mysql.Resultset
		var err error
		//for handle go mysql driver select @@max_allowed_packet
		if strings.Contains(strings.ToLower(query), "max_allowed_packet") {
			r, err = mysql.BuildSimpleResultset([]string{"@@max_allowed_packet"}, [][]interface{}{
				{mysql.MaxPayloadLen},
			}, binary)
		} else {
			if strings.Contains(query, "slow") {
				time.Sleep(time.Second * 5)
			}

			r, err = mysql.BuildSimpleResultset([]string{"a", "b"}, [][]interface{}{
				{1, "hello world"},
			}, binary)
		}

		if err != nil {
			return nil, errors.Trace(err)
		} else {
			return &mysql.Result{
				Status:       0,
				Warnings:     0,
				InsertId:     0,
				AffectedRows: 0,
				Resultset:    r,
			}, nil
		}
	case "insert":
		return &mysql.Result{
			Status:       0,
			Warnings:     0,
			InsertId:     1,
			AffectedRows: 0,
			Resultset:    nil,
		}, nil
	default:
		return nil, fmt.Errorf("invalid query %s", query)
	}
}

func (h *mockHandler) HandleQuery(query string) (*mysql.Result, error) {
	return h.handleQuery(query, false)
}

func (h *mockHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}

func (h *mockHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	params = 1
	columns = 0
	return params, columns, nil, nil
}

func (h *mockHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {

	if strings.HasPrefix(strings.ToLower(query), "select") {
		return h.HandleQuery(query)
	}

	return &mysql.Result{
		Status:       0,
		Warnings:     0,
		InsertId:     1,
		AffectedRows: 0,
		Resultset:    nil,
	}, nil
}

func (h *mockHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h *mockHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return nil
}
