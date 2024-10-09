package db_table_filter

import (
	"context"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // mysql 驱动

	"github.com/dlclark/regexp2"
	"github.com/jmoiron/sqlx"
)

func (c *DbTableFilter) DbFilterRegex() string {
	return fmt.Sprintf(`^%s%s`, c.dbFilterIncludeRegex, c.dbFilterExcludeRegex)
}

func (c *DbTableFilter) DBExcludeFilterRegex() string {
	return c.dbFilterExcludeRegex
}

func (c *DbTableFilter) GetDbs(ip string, port int, user string, password string) ([]string, error) {
	db, conn, err := makeConn(ip, port, user, password)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
		_ = db.Close()
	}()

	return c.GetDbsByConn(conn)
}

func (c *DbTableFilter) GetDbsByConn(conn *sqlx.Conn) ([]string, error) {
	pattern, err := regexp2.Compile(c.DbFilterRegex(), regexp2.None)
	if err != nil {
		return nil, err
	}
	var dbs []string
	err = conn.SelectContext(context.Background(), &dbs, `SHOW DATABASES`)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, db := range dbs {
		ok, err := pattern.MatchString(db)
		if err != nil {
			return nil, err
		}
		if ok {
			res = append(res, db)
		}
	}
	return res, nil
}

func (c *DbTableFilter) dbIncludeRegex() {
	c.dbFilterIncludeRegex = buildIncludeRegexp(ReplaceGlobs(c.IncludeDbPatterns))
}

func (c *DbTableFilter) dbExcludeRegex() {
	c.dbFilterExcludeRegex = buildExcludeRegexp(ReplaceGlobs(c.ExcludeDbPatterns))
}
