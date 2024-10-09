// Package db_table_filter
/*
表过滤是在库过滤的结果上做
include db = dba%,dbb%
exclude db = dba1,dbb1
include table = tb%
exclude table = tb1%

库的匹配正则是
(?=(?:(dba.*|dbb.*)))(?!(?:(dba1|dbb1)))

过滤表时, 需要排除掉整个 dba1, dbb1 库, 所以表的匹配正则是
(?=(?:(dba.*\.tb.*|dbb.*\.tb.*)))(?!(?:(dba1\..*|dbb1\..*|dba.*\.tb1.*|dbb.*\.tb1.*)))
这个正则分 3 部分
1. (?=(?:(dba.*\.tb.*|dbb.*\.tb.*))) : 选择 dba%, dbb% 库的所有 tb% 表
2. 排除项中的 dba1\..*|dbb1\..* : 排除 dba1, dbb1 的所有表
3. 排除项中的 dba.*\.tb1.*|dbb.*\.tb1.* : 排除 dba%, dbb% 的 tb1% 表
*/
package db_table_filter

import (
	"context"
	"fmt"

	"github.com/dlclark/regexp2"
	"github.com/jmoiron/sqlx"
)

func (c *DbTableFilter) tableIncludeRegex() {
	var parts []string
	// 匹配 include db 的 include table
	for _, db := range ReplaceGlobs(c.IncludeDbPatterns) {
		for _, table := range ReplaceGlobs(c.IncludeTablePatterns) {
			parts = append(parts, fmt.Sprintf(`%s\.%s`, db, table))
		}
	}
	c.tableFilterIncludeRegex = buildIncludeRegexp(parts)
}

func (c *DbTableFilter) tableExcludeRegex() {
	var parts []string

	// 排除 ignore db 的所有表
	for _, db := range ReplaceGlobs(c.ExcludeDbPatterns) {
		parts = append(parts, fmt.Sprintf(`%s\..*`, db))
	}

	// 排除 include db 的 ignore table
	for _, db := range ReplaceGlobs(c.IncludeDbPatterns) {
		for _, table := range ReplaceGlobs(c.ExcludeTablePatterns) {
			parts = append(parts, fmt.Sprintf(`%s\.%s`, db, table))
		}
	}
	c.tableFilterExcludeRegex = buildExcludeRegexp(parts)
}

func (c *DbTableFilter) TableFilterRegex() string {
	return fmt.Sprintf(`^%s%s`, c.tableFilterIncludeRegex, c.tableFilterExcludeRegex)
}

func (c *DbTableFilter) GetTables(ip string, port int, user string, password string) (map[string][]string, error) {
	db, conn, err := makeConn(ip, port, user, password)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
		_ = db.Close()
	}()
	return c.GetTablesByConn(conn)
}

func (c *DbTableFilter) GetExcludeTables(ip string, port int, user string, password string) (map[string][]string, error) {
	db, conn, err := makeConn(ip, port, user, password)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
		_ = db.Close()
	}()
	return c.GetExcludeTablesByConn(conn)
}

func (c *DbTableFilter) GetTablesByConn(conn *sqlx.Conn) (map[string][]string, error) {
	return c.getTablesByConn(conn, true)
}

func (c *DbTableFilter) GetExcludeTablesByConn(conn *sqlx.Conn) (map[string][]string, error) {
	return c.getTablesByConn(conn, false)
}

func (c *DbTableFilter) getTablesByConn(conn *sqlx.Conn, include bool) (map[string][]string, error) {
	// 拿到匹配的库
	dbs, err := c.GetDbsByConn(conn)
	if err != nil {
		return nil, err
	}

	// 预初始化结果
	res := make(map[string][]string)
	for _, db := range dbs {
		res[db] = []string{}
	}

	pattern, err := regexp2.Compile(c.TableFilterRegex(), regexp2.None)
	if err != nil {
		return nil, err
	}

	for _, db := range dbs {
		var tables []string
		err := conn.SelectContext(
			context.Background(),
			&tables,
			`SELECT table_name FROM INFORMATION_SCHEMA.TABLES 
                  		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'`,
			db,
		)
		if err != nil {
			return nil, err
		}

		for _, table := range tables {
			ok, err := pattern.MatchString(fmt.Sprintf("%s.%s", db, table))
			if err != nil {
				return nil, err
			}
			if ok == include {
				res[db] = append(res[db], table)
			}
		}
	}
	return res, nil
}
