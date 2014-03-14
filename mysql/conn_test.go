package mysql

import (
	"fmt"
	"testing"
)

func newTestConn() *conn {
	c := new(conn)

	if err := c.Connect("127.0.0.1:3306", "qing", "admin", "mixer"); err != nil {
		panic(err)
	}

	return c
}

func TestConn_Connect(t *testing.T) {
	c := newTestConn()
	defer c.Close()
}

func TestConn_Ping(t *testing.T) {
	c := newTestConn()
	defer c.Close()

	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestConn_DeleteTable(t *testing.T) {
	c := newTestConn()
	defer c.Close()

	if _, err := c.Exec("drop table if exists mixer_test_conn"); err != nil {
		t.Fatal(err)
	}
}

func TestConn_CreateTable(t *testing.T) {
	s := `CREATE TABLE IF NOT EXISTS mixer_test_conn (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          f DOUBLE,
          e enum("test1", "test2"),
          u tinyint unsigned,
          i tinyint,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	c := newTestConn()
	defer c.Close()

	if _, err := c.Exec(s); err != nil {
		t.Fatal(err)
	}
}

func TestConn_Insert(t *testing.T) {
	s := `insert into mixer_test_conn (id, str, f, e) values(1, "a", 3.14, "test1")`

	c := newTestConn()
	defer c.Close()

	if pkg, err := c.Exec(s); err != nil {
		t.Fatal(err)
	} else {
		if pkg.AffectedRows != 1 {
			t.Fatal(pkg.AffectedRows)
		}
	}
}

func TestConn_Select(t *testing.T) {
	s := `select str, f, e from mixer_test_conn where id = 1`

	c := newTestConn()
	defer c.Close()

	if result, err := c.Query(s); err != nil {
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

func TestConn_FieldList(t *testing.T) {
	c := newTestConn()
	defer c.Close()

	if result, err := c.FieldList("mixer_test_conn", "st%"); err != nil {
		t.Fatal(err)
	} else {
		if len(result) != 1 {
			t.Fatal(len(result))
		}

		if string(result[0].Name) != `str` {
			t.Fatal(string(result[0].Name))
		}
	}
}

func TestConn_Escape(t *testing.T) {
	c := newTestConn()
	defer c.Close()

	e := `""''\abc`
	s := fmt.Sprintf(`insert into mixer_test_conn (id, str) values(5, "%s")`,
		Escape(e))

	if _, err := c.Exec(s); err != nil {
		t.Fatal(err)
	}

	s = `select str from mixer_test_conn where id = ?`

	if r, err := c.Query(s, 5); err != nil {
		t.Fatal(err)
	} else {
		str, _ := r.GetString(0, 0)
		if str != e {
			t.Fatal(str)
		}
	}
}

func TestConn_SetCharset(t *testing.T) {
	c := newTestConn()
	defer c.Close()

	if err := c.SetCharset("gb2312"); err != nil {
		t.Fatal(err)
	}
}
