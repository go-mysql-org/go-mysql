package server

import (
	"database/sql"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/test_util"
	"github.com/go-mysql-org/go-mysql/test_util/test_keys"
)

var delay = 50

// test caching for 'caching_sha2_password'
// NOTE the idea here is to plugin a throttled credential provider so that the first connection (cache miss) will take longer time
// than the second connection (cache hit). Remember to set the password for MySQL user otherwise it won't cache empty password.
func TestCachingSha2Cache(t *testing.T) {
	log.SetLevel(log.LevelDebug)

	remoteProvider := &RemoteThrottleProvider{NewInMemoryProvider(), delay + 50}
	remoteProvider.AddUser(*testUser, *testPassword)
	cacheServer := NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, test_keys.PubPem, tlsConf)

	// no TLS
	suite.Run(t, &cacheTestSuite{
		server:       cacheServer,
		credProvider: remoteProvider,
		tlsPara:      "false",
	})
}

func TestCachingSha2CacheTLS(t *testing.T) {
	log.SetLevel(log.LevelDebug)

	remoteProvider := &RemoteThrottleProvider{NewInMemoryProvider(), delay + 50}
	remoteProvider.AddUser(*testUser, *testPassword)
	cacheServer := NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, test_keys.PubPem, tlsConf)

	// TLS
	suite.Run(t, &cacheTestSuite{
		server:       cacheServer,
		credProvider: remoteProvider,
		tlsPara:      "skip-verify",
	})
}

type RemoteThrottleProvider struct {
	*InMemoryProvider
	delay int // in milliseconds
}

func (m *RemoteThrottleProvider) GetCredential(username string) (password string, found bool, err error) {
	time.Sleep(time.Millisecond * time.Duration(m.delay))
	return m.InMemoryProvider.GetCredential(username)
}

type cacheTestSuite struct {
	suite.Suite
	server       *Server
	serverAddr   string
	credProvider CredentialProvider
	tlsPara      string

	db *sql.DB

	l net.Listener
}

func (s *cacheTestSuite) SetupSuite() {
	s.serverAddr = fmt.Sprintf("%s:%s", *test_util.MysqlFakeHost, *test_util.MysqlFakePort)

	var err error

	s.l, err = net.Listen("tcp", s.serverAddr)
	require.NoError(s.T(), err)

	go s.onAccept()

	time.Sleep(30 * time.Millisecond)
}

func (s *cacheTestSuite) TearDownSuite() {
	if s.l != nil {
		s.l.Close()
	}
}

func (s *cacheTestSuite) onAccept() {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}

		go s.onConn(conn)
	}
}

func (s *cacheTestSuite) onConn(conn net.Conn) {
	//co, err := NewConn(conn, *testUser, *testPassword, &testHandler{s})
	co, err := NewCustomizedConn(conn, s.server, s.credProvider, &testCacheHandler{s})
	require.NoError(s.T(), err)
	for {
		err = co.HandleCommand()
		if err != nil {
			return
		}
	}
}

func (s *cacheTestSuite) runSelect() {
	var a int64
	var b string

	err := s.db.QueryRow("SELECT a, b FROM tbl WHERE id=1").Scan(&a, &b)
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(1), a)
	require.Equal(s.T(), "hello world", b)
}

func (s *cacheTestSuite) TestCache() {
	// first connection
	t1 := time.Now()
	var err error
	s.db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=%s", *testUser, *testPassword, s.serverAddr, *testDB, s.tlsPara))
	require.NoError(s.T(), err)
	s.db.SetMaxIdleConns(4)
	s.runSelect()
	t2 := time.Now()

	d1 := int(t2.Sub(t1).Nanoseconds() / 1e6)
	//log.Debugf("first connection took %d milliseconds", d1)

	require.GreaterOrEqual(s.T(), d1, delay)

	if s.db != nil {
		s.db.Close()
	}

	// second connection
	t3 := time.Now()
	s.db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=%s", *testUser, *testPassword, s.serverAddr, *testDB, s.tlsPara))
	require.NoError(s.T(), err)
	s.db.SetMaxIdleConns(4)
	s.runSelect()
	t4 := time.Now()

	d2 := int(t4.Sub(t3).Nanoseconds() / 1e6)
	//log.Debugf("second connection took %d milliseconds", d2)

	require.Less(s.T(), d2, delay)
	if s.db != nil {
		s.db.Close()
	}

	s.server.cacheShaPassword = &sync.Map{}
}

type testCacheHandler struct {
	s *cacheTestSuite
}

func (h *testCacheHandler) UseDB(dbName string) error {
	return nil
}

func (h *testCacheHandler) handleQuery(query string, binary bool) (*mysql.Result, error) {
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
	case "delete", "update", "replace":
		return &mysql.Result{
			Status:       0,
			Warnings:     0,
			InsertId:     0,
			AffectedRows: 1,
			Resultset:    nil,
		}, nil
	default:
		return nil, fmt.Errorf("invalid query %s", query)
	}
}

func (h *testCacheHandler) HandleQuery(query string) (*mysql.Result, error) {
	return h.handleQuery(query, false)
}

func (h *testCacheHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}
func (h *testCacheHandler) HandleStmtPrepare(sql string) (params int, columns int, ctx interface{}, err error) {
	return 0, 0, nil, nil
}

func (h *testCacheHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h *testCacheHandler) HandleStmtExecute(ctx interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return h.handleQuery(query, true)
}

func (h *testCacheHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("command %d is not supported now", cmd))
}
