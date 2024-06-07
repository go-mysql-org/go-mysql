package driver

import (
	"context"
	"database/sql"
	sqlDriver "database/sql/driver"
	"fmt"
	"math"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
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

func TestDriverOptions_SetCollation(t *testing.T) {
	c := &client.Conn{}
	err := CollationOption(c, "latin2_bin")
	require.NoError(t, err)
	require.Equal(t, "latin2_bin", c.GetCollation())
}

func TestDriverOptions_SetCompression(t *testing.T) {
	var err error
	c := &client.Conn{}
	err = CompressOption(c, "zlib")
	require.NoError(t, err)
	require.True(t, c.HasCapability(mysql.CLIENT_COMPRESS))

	err = CompressOption(c, "zstd")
	require.NoError(t, err)
	require.True(t, c.HasCapability(mysql.CLIENT_ZSTD_COMPRESSION_ALGORITHM))

	err = CompressOption(c, "uncompressed")
	require.NoError(t, err)
	require.False(t, c.HasCapability(mysql.CLIENT_COMPRESS))
	require.False(t, c.HasCapability(mysql.CLIENT_ZSTD_COMPRESSION_ALGORITHM))

	require.Error(t, CompressOption(c, "foo"))
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

	conn, err := sql.Open("mysql", "root@127.0.0.1:3307/test?writeTimeout=1ns")
	require.NoError(t, err)

	result, err := conn.ExecContext(context.TODO(), "insert into slow(a,b) values(1,2);")
	require.Error(t, err)
	require.Nil(t, result)

	conn.Close()
}

func TestDriverOptions_namedValueChecker(t *testing.T) {
	AddNamedValueChecker(func(nv *sqlDriver.NamedValue) error {
		rv := reflect.ValueOf(nv.Value)
		if rv.Kind() != reflect.Uint64 {
			// fallback to the default value converter when the value is not a uint64
			return sqlDriver.ErrSkip
		}

		return nil
	})

	log.SetLevel(log.LevelDebug)
	srv := CreateMockServer(t)
	defer srv.Stop()
	conn, err := sql.Open("mysql", "root@127.0.0.1:3307/test?writeTimeout=1s")
	require.NoError(t, err)
	defer conn.Close()

	stmt, err := conn.Prepare("select a, b from fast where uint64 = ?")
	require.NoError(t, err)
	defer stmt.Close()

	var val uint64 = math.MaxUint64
	result, err := stmt.Query(val)
	require.NoError(t, err)
	require.NotNil(t, result)

	var a uint64
	var b string
	require.True(t, result.Next())
	require.NoError(t, result.Scan(&a, &b))
	require.True(t, math.MaxUint64 == a)
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
				if err != nil {
					return
				}
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

			var aValue uint64 = 1
			if strings.Contains(query, "uint64") {
				aValue = math.MaxUint64
			}

			r, err = mysql.BuildSimpleResultset([]string{"a", "b"}, [][]interface{}{
				{aValue, "hello world"},
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
	columns = 2
	return params, columns, nil, nil
}

func (h *mockHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	if strings.HasPrefix(strings.ToLower(query), "select") {
		return h.handleQuery(query, true)
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
