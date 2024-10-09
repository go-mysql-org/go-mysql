package db_table_filter

import (
	"context"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql 驱动

	"github.com/jmoiron/sqlx"
)

func ContainGlob(p string) bool {
	return strings.Contains(p, "*") ||
		strings.Contains(p, "?") ||
		strings.Contains(p, "%")
}

// HasGlobPattern 是否有通配符
func HasGlobPattern(patterns []string) bool {
	for _, p := range patterns {
		if strings.Contains(p, "%") || strings.Contains(p, "?") || strings.Contains(p, "*") {
			return true
		}
	}
	return false
}

func makeConn(ip string, port int, user string, password string) (*sqlx.DB, *sqlx.Conn, error) {
	dbh, err := sqlx.Connect(
		"mysql",
		fmt.Sprintf(`%s:%s@tcp(%s:%d)/`, user, password, ip, port),
	)
	if err != nil {
		return nil, nil, err
	}

	conn, err := dbh.Connx(context.Background())
	if err != nil {
		return nil, nil, err
	}

	return dbh, conn, nil
}
