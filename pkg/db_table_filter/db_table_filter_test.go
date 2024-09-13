package db_table_filter_test

import (
	"testing"

	"dbm-services/mysql/db-tools/dbactuator/pkg/util/db_table_filter"
)

func TestDbtableFilter(t *testing.T) {
	t.Log("start...")
	r, err := db_table_filter.NewDbTableFilter(
		[]string{"*"},
		[]string{"*"},
		[]string{"sys", "information_schema"},
		[]string{"*"},
	)
	if err != nil {
		t.Fatal(err)
		return
	}
	r.BuildFilter()
	t.Log(r.TableFilterRegex())
}
