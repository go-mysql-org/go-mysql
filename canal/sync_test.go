package canal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/replication"
)

func TestGetShowBinaryLogQuery(t *testing.T) {
	tests := []struct {
		flavor        string
		serverVersion string
		expected      string
	}{
		{flavor: "mariadb", serverVersion: "10.5.2", expected: "SHOW BINLOG STATUS"},
		{flavor: "mariadb", serverVersion: "10.6.0", expected: "SHOW BINLOG STATUS"},
		{flavor: "mariadb", serverVersion: "10.4.0", expected: "SHOW MASTER STATUS"},
		{flavor: "mysql", serverVersion: "8.4.0", expected: "SHOW BINARY LOG STATUS"},
		{flavor: "mysql", serverVersion: "8.4.1", expected: "SHOW BINARY LOG STATUS"},
		{flavor: "mysql", serverVersion: "8.0.33", expected: "SHOW MASTER STATUS"},
		{flavor: "mysql", serverVersion: "5.7.41", expected: "SHOW MASTER STATUS"},
		{flavor: "other", serverVersion: "1.0.0", expected: "SHOW MASTER STATUS"},
	}

	for _, tt := range tests {
		t.Run(tt.flavor+"_"+tt.serverVersion, func(t *testing.T) {
			got := getShowBinaryLogQuery(tt.flavor, tt.serverVersion)
			require.Equal(t, tt.expected, got)
		})
	}
}

type rowsQueryRecorder struct {
	DummyEventHandler
	queries []string
}

func (h *rowsQueryRecorder) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	h.queries = append(h.queries, string(e.Query))
	return nil
}

func TestHandleEventRowsQuery(t *testing.T) {
	query := "/* app:42 */ INSERT INTO test.t (id) VALUES (1)"
	tests := []struct {
		name  string
		event replication.Event
	}{
		{name: "mysql_rows_query", event: &replication.RowsQueryEvent{Query: []byte(query)}},
		{name: "mariadb_annotate_rows", event: &replication.MariadbAnnotateRowsEvent{Query: []byte(query)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &rowsQueryRecorder{}
			c := &Canal{master: &masterInfo{}, eventHandler: h}
			err := c.handleEvent(&replication.BinlogEvent{Header: &replication.EventHeader{}, Event: tt.event})
			require.NoError(t, err)
			require.Equal(t, []string{query}, h.queries)
		})
	}
}
