package canal

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
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

type unparsedQueryRecorder struct {
	DummyEventHandler
	queries  []string
	nextPos  mysql.Position
	parseErr error
	ddls     []string
	err      error
}

func (h *unparsedQueryRecorder) OnUnparsedQuery(_ *replication.EventHeader, nextPos mysql.Position, e *replication.QueryEvent, parseErr error) error {
	h.queries = append(h.queries, string(e.Query))
	h.nextPos = nextPos
	h.parseErr = parseErr
	return h.err
}

func (h *unparsedQueryRecorder) OnDDL(_ *replication.EventHeader, _ mysql.Position, e *replication.QueryEvent) error {
	h.ddls = append(h.ddls, string(e.Query))
	return nil
}

func newQueryEventCanal(h EventHandler) *Canal {
	logger := slog.New(slog.DiscardHandler)
	return &Canal{
		cfg:          &Config{Logger: logger},
		master:       &masterInfo{pos: mysql.Position{Name: "mysql-bin.000001", Pos: 4}, logger: logger},
		parser:       parser.New(),
		eventHandler: h,
	}
}

func queryEvent(query string, logPos uint32) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{LogPos: logPos},
		Event:  &replication.QueryEvent{Schema: []byte("test"), Query: []byte(query)},
	}
}

// The parser does not support spatial column types.
const unparsableDDL = "ALTER TABLE t ADD COLUMN g POINT"

func TestHandleEventUnparsedQuery(t *testing.T) {
	h := &unparsedQueryRecorder{}
	c := newQueryEventCanal(h)

	err := c.handleEvent(queryEvent(unparsableDDL, 120))
	require.NoError(t, err)
	require.Equal(t, []string{unparsableDDL}, h.queries)
	require.Equal(t, mysql.Position{Name: "mysql-bin.000001", Pos: 120}, h.nextPos)
	require.Error(t, h.parseErr)
	require.Empty(t, h.ddls)
}

func TestHandleEventUnparsedQueryError(t *testing.T) {
	handlerErr := errors.New("handler failed")
	h := &unparsedQueryRecorder{err: handlerErr}
	c := newQueryEventCanal(h)

	err := c.handleEvent(queryEvent(unparsableDDL, 120))
	require.ErrorIs(t, err, handlerErr)
}

func TestHandleEventUnparsedQueryWithoutHandler(t *testing.T) {
	c := newQueryEventCanal(&DummyEventHandler{})

	err := c.handleEvent(queryEvent(unparsableDDL, 120))
	require.NoError(t, err)
}

func TestHandleEventParsedQuerySkipsUnparsedHandler(t *testing.T) {
	h := &unparsedQueryRecorder{}
	c := newQueryEventCanal(h)
	query := "ALTER TABLE t ADD COLUMN c INT"

	err := c.handleEvent(queryEvent(query, 120))
	require.NoError(t, err)
	require.Equal(t, []string{query}, h.ddls)
	require.Empty(t, h.queries)
}
