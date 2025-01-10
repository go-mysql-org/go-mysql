package mysql

import "testing"

func TestColumnNumber(t *testing.T) {
	r := Result{}
	// Make sure ColumnNumber doesn't panic if ResultSet is nil
	// https://github.com/go-mysql-org/go-mysql/issues/964
	r.ColumnNumber()
}
