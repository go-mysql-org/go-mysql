package mysql

import (
	"sync"
	"testing"
)

var testDBOnce sync.Once
var testDB *DB

func newTestDB() *DB {
	f := func() {
		testDB, _ = NewDB("qing:admin@127.0.0.1:3306/mixer", 16)
	}

	testDBOnce.Do(f)
	return testDB
}

func TestDB_Init(t *testing.T) {
	newTestDB()
}

func TestDB_Ping(t *testing.T) {
	db := newTestDB()
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestDB_DeleteTable(t *testing.T) {
	db := newTestDB()

	if _, err := db.Exec("drop table if exists mixer_test"); err != nil {
		t.Fatal(err)
	}
}

func TestDB_CreateTable(t *testing.T) {
	s := `CREATE TABLE IF NOT EXISTS mixer_test (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          f DOUBLE,
          e enum("test1", "test2"),
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	db := newTestDB()

	if _, err := db.Exec(s); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Insert(t *testing.T) {
	s := `insert into mixer_test (id, str, f, e) values(1, "a", 3.14, "test1")`

	db := newTestDB()

	if pkg, err := db.Exec(s); err != nil {
		t.Fatal(err)
	} else {
		if pkg.AffectedRows != 1 {
			t.Fatal(pkg.AffectedRows)
		}
	}
}

func TestDB_Select(t *testing.T) {
	s := `select str, f, e from mixer_test where id = 1`

	db := newTestDB()

	if result, err := db.Query(s); err != nil {
		t.Fatal(err)
	} else {
		if len(result.Fields) != 3 {
			t.Fatal(len(result.Fields))
		}

		if len(result.Data) != 1 {
			t.Fatal(len(result.Data))
		}

		if str, _ := result.GetString(0, 0); str != "a" {
			t.Fatal("invalid str", str)
		}

		if f, _ := result.GetFloat(0, 1); f != float64(3.14) {
			t.Fatal("invalid f", f)
		}

		if e, _ := result.GetString(0, 2); e != "test1" {
			t.Fatal("invalid e", e)
		}

		if str, _ := result.GetStringByName(0, "str"); str != "a" {
			t.Fatal("invalid str", str)
		}

		if f, _ := result.GetFloatByName(0, "f"); f != float64(3.14) {
			t.Fatal("invalid f", f)
		}

		if e, _ := result.GetStringByName(0, "e"); e != "test1" {
			t.Fatal("invalid e", e)
		}

	}
}

func TestDBStmt_Delete(t *testing.T) {
	str := `delete from mixer_test`

	db := newTestDB()

	s, err := db.Prepare(str)

	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.Exec(); err != nil {
		t.Fatal(err)
	}

	s.Close()
}

func TestDBStmt_Insert(t *testing.T) {
	str := `insert into mixer_test (id, str, f, e) values (?, ?, ?, ?)`

	db := newTestDB()

	s, err := db.Prepare(str)

	if err != nil {
		t.Fatal(err)
	}

	if pkg, err := s.Exec(1, "a", 3.14, "test1"); err != nil {
		t.Fatal(err)
	} else {
		if pkg.AffectedRows != 1 {
			t.Fatal(pkg.AffectedRows)
		}
	}

	s.Close()
}

func TestDBStmt_Select(t *testing.T) {
	str := `select str, f, e from mixer_test where id = ?`

	db := newTestDB()

	s, err := db.Prepare(str)
	if err != nil {
		t.Fatal(err)
	}

	if result, err := s.Query(1); err != nil {
		t.Fatal(err)
	} else {
		if len(result.Data) != 1 {
			t.Fatal(len(result.Data))
		}

		if len(result.Fields) != 3 {
			t.Fatal(len(result.Fields))
		}

		if str, _ := result.GetString(0, 0); str != "a" {
			t.Fatal("invalid str", str)
		}

		if f, _ := result.GetFloat(0, 1); f != float64(3.14) {
			t.Fatal("invalid f", f)
		}

		if e, _ := result.GetString(0, 2); e != "test1" {
			t.Fatal("invalid e", e)
		}

		if str, _ := result.GetStringByName(0, "str"); str != "a" {
			t.Fatal("invalid str", str)
		}

		if f, _ := result.GetFloatByName(0, "f"); f != float64(3.14) {
			t.Fatal("invalid f", f)
		}

		if e, _ := result.GetStringByName(0, "e"); e != "test1" {
			t.Fatal("invalid e", e)
		}

	}

	s.Close()
}

func TestDB_Trans(t *testing.T) {
	db := newTestDB()

	var tx1 *Tx
	var tx2 *Tx
	var err error

	if tx1, err = db.Begin(); err != nil {
		t.Fatal(err)
	}

	if tx2, err = db.Begin(); err != nil {
		t.Fatal(err)
	}

	if !tx1.conn.IsInTransaction() {
		t.Fatal("tx1 not in transaction")
	}

	if !tx2.conn.IsInTransaction() {
		t.Fatal("tx2 not in transaction")
	}

	if _, err := tx1.Exec(`insert into mixer_test (id, str) values (111, "abc")`); err != nil {
		t.Fatal(err)
	}

	var s *Stmt
	if s, err = tx1.Prepare(`select str from mixer_test where id = ?`); err != nil {
		t.Fatal(err)
	}

	if r, err := s.Query(111); err != nil {
		t.Fatal(err)
	} else {
		if s, _ := r.GetString(0, 0); s != "abc" {
			t.Fatal(s)
		}
	}

	if r, err := tx2.Query(`select str from mixer_test where id = ?`, 111); err != nil {
		t.Fatal(err)
	} else {
		if r.RowNumber() != 0 {
			t.Fatal(r.RowNumber())
		}
	}

	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	if tx1.conn.IsInTransaction() {
		t.Fatal("tx1 in transaction")
	}

	if tx2.conn.IsInTransaction() {
		t.Fatal("tx2 in transaction")
	}

	if r, err := db.Query(`select str from mixer_test where id = ?`, 111); err != nil {
		t.Fatal(err)
	} else {
		if r.RowNumber() != 1 {
			t.Fatal(r.RowNumber())
		}
	}

	if r, err := s.Query(111); err != nil {
		t.Fatal(err)
	} else {
		if s, _ := r.GetString(0, 0); s != "abc" {
			t.Fatal(s)
		}
	}

	s.Close()
}
