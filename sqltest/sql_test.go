package sqltest

import (
	"database/sql"
	"fmt"
	_ "github.com/siddontang/go-mysql/mysql"
	"math/rand"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
)

//refer: https://github.com/bradfitz/go-sql-test

type Tester interface {
	RunTest(*testing.T, func(params))
}

var (
	goMysql Tester = &goMysqlDB{}
)

const TablePrefix = "gosqltest_"

type goMysqlDB struct {
	once    sync.Once // guards init of running
	running bool      // whether port 3306 is listening
}

func (m *goMysqlDB) Running() bool {
	m.once.Do(func() {
		c, err := net.Dial("tcp", "localhost:3306")
		if err == nil {
			m.running = true
			c.Close()
		}
	})
	return m.running
}

type params struct {
	dbType Tester
	*testing.T
	*sql.DB
}

func (t params) mustExec(sql string, args ...interface{}) sql.Result {
	res, err := t.DB.Exec(sql, args...)
	if err != nil {
		t.Fatalf("Error running %q: %v", sql, err)
	}
	return res
}

var qrx = regexp.MustCompile(`\?`)

// q converts "?" characters to $1, $2, $n on postgres, :1, :2, :n on Oracle
func (t params) q(sql string) string {
	return sql
}

func (m *goMysqlDB) RunTest(t *testing.T, fn func(params)) {
	if !m.Running() {
		t.Logf("skipping test; no MySQL running on localhost:3306")
		return
	}
	user := os.Getenv("GOSQLTEST_MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pass, ok := getenvOk("GOSQLTEST_MYSQL_PASS")
	if !ok {
		pass = "root"
	}
	dbName := "gosqltest"
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@127.0.0.1:3306/%s", user, pass, dbName))
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}

	params := params{goMysql, t, db}

	// Drop all tables in the test database.
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Fatalf("failed to enumerate tables: %v", err)
	}
	for rows.Next() {
		var table string
		if rows.Scan(&table) == nil &&
			strings.HasPrefix(strings.ToLower(table), strings.ToLower(TablePrefix)) {
			params.mustExec("DROP TABLE " + table)
		}
	}

	fn(params)
}

func sqlBlobParam(t params, size int) string {
	return fmt.Sprintf("VARBINARY(%d)", size)
}

func TestBlobs_GoMySQL(t *testing.T) { goMysql.RunTest(t, testBlobs) }

func testBlobs(t params) {
	var blob = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	t.mustExec("create table " + TablePrefix + "foo (id integer primary key, bar " + sqlBlobParam(t, 16) + ")")
	t.mustExec(t.q("insert into "+TablePrefix+"foo (id, bar) values(?,?)"), 0, blob)

	want := fmt.Sprintf("%x", blob)

	b := make([]byte, 16)
	err := t.QueryRow(t.q("select bar from "+TablePrefix+"foo where id = ?"), 0).Scan(&b)
	got := fmt.Sprintf("%x", b)
	if err != nil {
		t.Errorf("[]byte scan: %v", err)
	} else if got != want {
		t.Errorf("for []byte, got %q; want %q", got, want)
	}

	err = t.QueryRow(t.q("select bar from "+TablePrefix+"foo where id = ?"), 0).Scan(&got)
	want = string(blob)
	if err != nil {
		t.Errorf("string scan: %v", err)
	} else if got != want {
		t.Errorf("for string, got %q; want %q", got, want)
	}
}

func TestManyQueryRow_GoMySQL(t *testing.T) { goMysql.RunTest(t, testManyQueryRow) }

func testManyQueryRow(t params) {
	if testing.Short() {
		t.Logf("skipping in short mode")
		return
	}
	t.mustExec("create table " + TablePrefix + "foo (id integer primary key, name varchar(50))")
	t.mustExec(t.q("insert into "+TablePrefix+"foo (id, name) values(?,?)"), 1, "bob")
	var name string
	for i := 0; i < 10000; i++ {
		err := t.QueryRow(t.q("select name from "+TablePrefix+"foo where id = ?"), 1).Scan(&name)
		if err != nil || name != "bob" {
			t.Fatalf("on query %d: err=%v, name=%q", i, err, name)
		}
	}
}

func TestTxQuery_GoMySQL(t *testing.T) { goMysql.RunTest(t, testTxQuery) }

func testTxQuery(t params) {
	tx, err := t.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = t.DB.Exec("create table " + TablePrefix + "foo (id integer primary key, name varchar(50))")
	if err != nil {
		t.Logf("cannot drop table "+TablePrefix+"foo: %s", err)
	}

	_, err = tx.Exec(t.q("insert into "+TablePrefix+"foo (id, name) values(?,?)"), 1, "bob")
	if err != nil {
		t.Fatal(err)
	}

	r, err := tx.Query(t.q("select name from "+TablePrefix+"foo where id = ?"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(err)
		}
		t.Fatal("expected one rows")
	}

	var name string
	err = r.Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPreparedStmt_GoMySQL(t *testing.T) { goMysql.RunTest(t, testPreparedStmt) }

func testPreparedStmt(t params) {
	t.mustExec("CREATE TABLE " + TablePrefix + "t (count INT)")
	sel, err := t.Prepare("SELECT count FROM " + TablePrefix + "t ORDER BY count DESC")
	if err != nil {
		t.Fatalf("prepare 1: %v", err)
	}
	ins, err := t.Prepare(t.q("INSERT INTO " + TablePrefix + "t (count) VALUES (?)"))
	if err != nil {
		t.Fatalf("prepare 2: %v", err)
	}

	for n := 1; n <= 3; n++ {
		if _, err := ins.Exec(n); err != nil {
			t.Fatalf("insert(%d) = %v", n, err)
		}
	}

	const nRuns = 10
	ch := make(chan bool)
	for i := 0; i < nRuns; i++ {
		go func() {
			defer func() {
				ch <- true
			}()
			for j := 0; j < 10; j++ {
				count := 0
				if err := sel.QueryRow().Scan(&count); err != nil && err != sql.ErrNoRows {
					t.Errorf("Query: %v", err)
					return
				}
				if _, err := ins.Exec(rand.Intn(100)); err != nil {
					t.Errorf("Insert: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < nRuns; i++ {
		<-ch
	}
}

func getenvOk(k string) (v string, ok bool) {
	v = os.Getenv(k)
	if v != "" {
		return v, true
	}
	keq := k + "="
	for _, kv := range os.Environ() {
		if kv == keq {
			return "", true
		}
	}
	return "", false
}
