package mysql

import (
	"container/list"
	"fmt"
	"lib/log"
	"strings"
	"sync"
)

type DB struct {
	addr     string
	user     string
	password string
	db       string

	maxIdleConns int

	sync.Mutex

	conns *list.List

	closed bool
}

type Conn struct {
	sync.Mutex

	db *DB
	co *conn

	closed bool
}

func (c *Conn) check() error {
	if c.db.closed {
		c.finalize()
		return ErrBadConn
	}

	return nil
}

func (c *Conn) Query(query string, args ...interface{}) (r *Resultset, err error) {
	if err = c.check(); err != nil {
		return
	}

	c.Lock()
	r, err = c.co.Query(query, args...)
	c.Unlock()
	return
}

func (c *Conn) Exec(query string, args ...interface{}) (r *Result, err error) {
	if err = c.check(); err != nil {
		return
	}

	c.Lock()
	r, err = c.co.Exec(query, args...)
	c.Unlock()
	return
}

func (c *Conn) Begin() (err error) {
	if err = c.check(); err != nil {
		return
	}

	c.Lock()
	err = c.co.Begin()
	c.Unlock()
	return
}

func (c *Conn) Commit() (err error) {
	if err = c.check(); err != nil {
		return
	}

	c.Lock()
	err = c.co.Commit()
	c.Unlock()
	return
}

func (c *Conn) Rollback() (err error) {
	if err = c.check(); err != nil {
		return
	}

	c.Lock()
	err = c.co.Rollback()
	c.Unlock()
	return
}

func (c *Conn) Ping() (err error) {
	if err = c.check(); err != nil {
		return
	}

	c.Lock()
	err = c.co.Ping()
	c.Unlock()
	return
}

func (c *Conn) SetCharset(charset string) (err error) {
	if err = c.check(); err != nil {
		return
	}

	c.Lock()
	err = c.co.SetCharset(charset)
	c.Unlock()
	return
}

func (c *Conn) GetCharset() (charset string) {
	return c.co.GetCharset()
}

func (c *Conn) Prepare(query string) (*Stmt, error) {
	if err := c.check(); err != nil {
		return nil, err
	}

	st, err := c.prepare(query)
	if err != nil {
		return nil, err
	}

	s := newStmt(c.db, query, c, st, true)

	return s, nil
}

func (c *Conn) prepare(query string) (st *stmt, err error) {
	c.Lock()
	st, err = c.co.Prepare(query)
	c.Unlock()
	return
}

func (c *Conn) Close() (err error) {
	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	c.closed = true
	c.Unlock()

	c.db.pushConn(c, nil)
	return
}

func (c *Conn) IsInTransaction() bool {
	return c.co.IsInTransaction()
}

func (c *Conn) IsAutoCommit() bool {
	return c.co.IsAutoCommit()
}

func (c *Conn) finalize() (err error) {
	c.Lock()
	c.closed = true
	err = c.co.Close()
	c.Unlock()
	return
}

func NewDB(dsn string, maxIdleConns int) (*DB, error) {
	d := new(DB)

	if err := d.parseDSN(dsn); err != nil {
		return nil, err
	}

	d.maxIdleConns = maxIdleConns

	d.conns = list.New()

	d.closed = false

	return d, nil
}

func (db *DB) Addr() string {
	return db.addr
}

//dsn: <username>:<password>@<host>:<port>/<database>
func (db *DB) parseDSN(dsn string) error {
	ns := strings.Split(dsn, "@")
	if len(ns) != 2 {
		return fmt.Errorf("invalid dsn %s", dsn)
	}

	if us := strings.Split(ns[0], ":"); len(us) > 2 {
		return fmt.Errorf("invalid dsn %s: error around %s", dsn, ns[0])
	} else if len(us) == 1 {
		db.user = us[0]
		db.password = ""
	} else {
		db.user = us[0]
		db.password = us[1]
	}

	if ds := strings.Split(ns[1], "/"); len(ds) != 2 {
		return fmt.Errorf("invalid dsn %s, error around %s", dsn, ns[1])
	} else {
		db.addr = ds[0]
		db.db = ds[1]
	}
	return nil
}

func (db *DB) Close() error {
	db.Lock()

	if db.closed {
		db.Unlock()
		return nil
	}

	db.closed = true

	for {
		if db.conns.Len() > 0 {
			v := db.conns.Back()
			co := v.Value.(*Conn)
			db.conns.Remove(v)

			co.finalize()

		} else {
			break
		}
	}

	db.Unlock()

	return nil
}

func (db *DB) GetConn() (*Conn, error) {
	return db.popConn()
}

func (db *DB) newConn() (*Conn, error) {
	co := new(conn)

	if err := co.Connect(db.addr, db.user, db.password, db.db); err != nil {
		log.Error("connect %s error %s", db.addr, err.Error())
		return nil, err
	}

	dc := new(Conn)
	dc.db = db
	dc.co = co
	dc.closed = false

	return dc, nil
}

func (db *DB) tryReuse(co *Conn) error {
	if co.IsInTransaction() {
		//we can not reuse a connection in transaction status
		log.Warn("reuse connection can not in transaction status, rollback")
		if err := co.Rollback(); err != nil {
			return err
		}
	} else if !co.IsAutoCommit() {
		//we can not  reuse a connection not in autocomit
		log.Warn("reuse connection must have autocommit status, enable autocommit")
		if _, err := co.Exec("set autocommit = 1"); err != nil {
			return err
		}
	}

	//connection may be set names early
	//we must use default utf8
	if co.GetCharset() != DEFAULT_CHARSET {
		if err := co.SetCharset(DEFAULT_CHARSET); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) popConn() (co *Conn, err error) {
	db.Lock()
	if db.conns.Len() > 0 {
		v := db.conns.Front()
		co = v.Value.(*Conn)
		db.conns.Remove(v)
	}
	db.Unlock()

	if co != nil {
		if err := co.Ping(); err == nil {
			if err := db.tryReuse(co); err == nil {
				//connection may alive
				return co, nil
			}
		}

		co.finalize()
	}

	return db.newConn()
}

func (db *DB) pushConn(co *Conn, err error) {
	var closeConn *Conn = nil

	if err == ErrBadConn {
		closeConn = co
	} else {
		db.Lock()

		if db.conns.Len() >= db.maxIdleConns {
			v := db.conns.Front()
			closeConn = v.Value.(*Conn)
			db.conns.Remove(v)
		}

		db.conns.PushBack(co)

		db.Unlock()

	}

	if closeConn != nil {
		closeConn.finalize()
	}
}

func (db *DB) Ping() (err error) {
	var c *Conn
	for i := 0; i < 3; i++ {
		c, err = db.popConn()
		if err != nil {
			return
		}

		err = c.Ping()

		db.pushConn(c, err)

		if err != ErrBadConn {
			break
		}
	}
	return
}

func (db *DB) Exec(query string, args ...interface{}) (r *Result, err error) {
	for i := 0; i < 10; i++ {
		if r, err = db.exec(query, args...); err != ErrBadConn {
			break
		}
	}
	return
}

func (db *DB) exec(query string, args ...interface{}) (r *Result, err error) {
	var c *Conn
	c, err = db.popConn()
	if err != nil {
		return
	}

	r, err = c.Exec(query, args...)

	db.pushConn(c, err)
	return
}

func (db *DB) Query(query string, args ...interface{}) (r *Resultset, err error) {
	for i := 0; i < 10; i++ {
		if r, err = db.query(query, args...); err != ErrBadConn {
			break
		}
	}
	return
}

func (db *DB) query(query string, args ...interface{}) (r *Resultset, err error) {
	var c *Conn
	c, err = db.popConn()
	if err != nil {
		return
	}

	r, err = c.Query(query, args...)

	db.pushConn(c, err)
	return
}

func (db *DB) Prepare(query string) (s *Stmt, err error) {
	s = newStmt(db, query, nil, nil, false)

	for i := 0; i < 10; i++ {
		err = s.reprepare()
		if err != ErrBadConn {
			break
		}
	}
	return
}

func (db *DB) Begin() (t *Tx, err error) {
	t = new(Tx)

	t.db = db
	t.done = false

	var conn *Conn

	for i := 0; i < 10; i++ {
		if conn, err = db.begin(); err == nil {
			t.conn = conn
			return
		} else {
			db.pushConn(conn, err)
		}

		if err != ErrBadConn {
			break
		}
	}

	return
}

func (db *DB) begin() (conn *Conn, err error) {
	if conn, err = db.popConn(); err != nil {
		return
	}

	err = conn.Begin()
	return
}

//for mysql stmt test, stmt is global to session
//so when a transaction prepare a stmt, it's exists after transaction over.

type Stmt struct {
	db  *DB
	str string

	c  *Conn
	st *stmt

	//if bind conn is false, stmt may try to reprepare and use another conn
	//when conn is closed
	bindConn bool

	Params  []Field
	Columns []Field
}

func newStmt(db *DB, query string, c *Conn, st *stmt, bindConn bool) *Stmt {
	s := new(Stmt)

	s.db = db
	s.str = query
	s.c = c
	s.st = st

	s.bindConn = bindConn

	if st != nil {
		s.Params = st.params
		s.Columns = st.columns
	}

	return s
}

func (s *Stmt) reprepare() error {
	c, err := s.db.popConn()
	if err != nil {
		return err
	}

	var st *stmt
	st, err = c.prepare(s.str)
	s.db.pushConn(c, err)
	if err != nil {
		return err
	}
	s.c = c
	s.st = st

	s.Params = st.params
	s.Columns = st.columns

	return nil
}

func (s *Stmt) Exec(args ...interface{}) (r *Result, err error) {
	s.c.Lock()
	r, err = s.st.Exec(args...)
	s.c.Unlock()

	if s.bindConn || err != ErrBadConn {
		return
	}

	for i := 0; i < 10; i++ {
		if r, err = s.exec(args...); err != ErrBadConn {
			break
		}
	}
	return

}

func (s *Stmt) exec(args ...interface{}) (r *Result, err error) {
	if err = s.reprepare(); err != nil {
		return
	}

	s.c.Lock()
	r, err = s.st.Exec(args...)
	s.c.Unlock()
	return
}

func (s *Stmt) Query(args ...interface{}) (r *Resultset, err error) {
	s.c.Lock()
	r, err = s.st.Query(args...)
	s.c.Unlock()

	if s.bindConn || err != ErrBadConn {
		return
	}

	for i := 0; i < 10; i++ {
		if r, err = s.query(args...); err != ErrBadConn {
			break
		}
	}
	return

}

func (s *Stmt) query(args ...interface{}) (r *Resultset, err error) {
	if err = s.reprepare(); err != nil {
		return
	}

	s.c.Lock()
	r, err = s.st.Query(args...)
	s.c.Unlock()
	return
}

func (s *Stmt) Close() (err error) {
	s.c.Lock()
	err = s.st.Close()
	s.c.Unlock()
	return
}

type Tx struct {
	sync.Mutex
	db   *DB
	done bool
	conn *Conn
}

func (t *Tx) Exec(query string, args ...interface{}) (*Result, error) {
	if t.done {
		return nil, ErrTxDone
	}

	r, err := t.conn.Exec(query, args...)
	return r, err
}

func (t *Tx) Query(query string, args ...interface{}) (*Resultset, error) {
	if t.done {
		return nil, ErrTxDone
	}

	r, err := t.conn.Query(query, args...)
	return r, err
}

func (t *Tx) Prepare(query string) (*Stmt, error) {
	if t.done {
		return nil, ErrTxDone
	}

	return t.conn.Prepare(query)
}

func (t *Tx) Commit() error {
	if t.done {
		return ErrTxDone
	}

	err := t.conn.Commit()

	t.db.pushConn(t.conn, err)

	t.done = true

	return err
}

func (t *Tx) Rollback() error {
	if t.done {
		return ErrTxDone
	}

	err := t.conn.Commit()

	t.db.pushConn(t.conn, err)

	t.done = true

	return err
}
