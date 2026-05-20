package canal

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/stretchr/testify/require"
)

type posSyncCall struct {
	header *replication.EventHeader
	pos    mysql.Position
	set    string
	force  bool
}

type posSyncRecorder struct {
	DummyEventHandler

	calls []posSyncCall
}

func (h *posSyncRecorder) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	call := posSyncCall{
		header: header,
		pos:    pos,
		force:  force,
	}
	if set != nil {
		call.set = set.String()
	}
	h.calls = append(h.calls, call)
	return nil
}

func newPosSyncTestCanal(t *testing.T, transactionalPosSync bool) (*Canal, *posSyncRecorder) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := NewDefaultConfig()
	cfg.Logger = logger
	cfg.TransactionalPosSync = transactionalPosSync

	ctx, cancel := context.WithCancel(context.Background())
	handler := &posSyncRecorder{}
	c := &Canal{
		cfg:          cfg,
		parser:       parser.New(),
		master:       &masterInfo{logger: logger},
		syncer:       replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: 1, Flavor: mysql.MySQLFlavor, Logger: logger}),
		eventHandler: handler,
		ctx:          ctx,
		cancel:       cancel,
	}

	return c, handler
}

func mustParseMysqlGTIDSet(t *testing.T, value string) mysql.GTIDSet {
	t.Helper()

	set, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, value)
	require.NoError(t, err)
	return set
}

func TestIsBeginQuery(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{query: "BEGIN", want: true},
		{query: " begin ; ", want: true},
		{query: "BEGIN WORK"},
		{query: "START TRANSACTION"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			require.Equal(t, tt.want, isBeginQuery([]byte(tt.query)))
		})
	}
}

func TestHandleEventBeginPositionSync(t *testing.T) {
	const sid = "24bc7850-2c58-11ee-be56-0242ac120002"

	tests := []struct {
		name                 string
		transactionalPosSync bool
		wantCalls            int
		wantPos              mysql.Position
		wantGTIDSet          string
	}{
		{
			name:        "legacy syncs begin position",
			wantCalls:   1,
			wantPos:     mysql.Position{Name: "mysql-bin.000001", Pos: 456},
			wantGTIDSet: sid + ":1-2",
		},
		{
			name:                 "transactional sync skips begin position",
			transactionalPosSync: true,
			wantPos:              mysql.Position{Name: "mysql-bin.000001", Pos: 123},
			wantGTIDSet:          sid + ":1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, handler := newPosSyncTestCanal(t, tt.transactionalPosSync)
			c.master.Update(mysql.Position{Name: "mysql-bin.000001", Pos: 123})
			c.master.UpdateGTIDSet(mustParseMysqlGTIDSet(t, sid+":1"))

			err := c.handleEvent(&replication.BinlogEvent{
				Header: &replication.EventHeader{
					Timestamp: 10,
					LogPos:    456,
					EventType: replication.QUERY_EVENT,
				},
				Event: &replication.QueryEvent{
					Query: []byte("BEGIN"),
					GSet:  mustParseMysqlGTIDSet(t, sid+":1-2"),
				},
			})
			require.NoError(t, err)

			require.Len(t, handler.calls, tt.wantCalls)
			require.Equal(t, tt.wantPos, c.master.Position())
			require.Equal(t, tt.wantGTIDSet, c.master.GTIDSet().String())
			if tt.wantCalls > 0 {
				require.Equal(t, tt.wantPos, handler.calls[0].pos)
				require.Equal(t, tt.wantGTIDSet, handler.calls[0].set)
				require.False(t, handler.calls[0].force)
			}
		})
	}
}

func TestHandleEventXIDForcePositionSync(t *testing.T) {
	const sid = "24bc7850-2c58-11ee-be56-0242ac120002"

	tests := []struct {
		name                 string
		transactionalPosSync bool
		wantForce            bool
	}{
		{name: "legacy keeps xid force false"},
		{name: "transactional sync forces xid", transactionalPosSync: true, wantForce: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, handler := newPosSyncTestCanal(t, tt.transactionalPosSync)
			c.master.Update(mysql.Position{Name: "mysql-bin.000001", Pos: 123})
			c.master.UpdateGTIDSet(mustParseMysqlGTIDSet(t, sid+":1"))

			err := c.handleEvent(&replication.BinlogEvent{
				Header: &replication.EventHeader{
					Timestamp: 10,
					LogPos:    789,
					EventType: replication.XID_EVENT,
				},
				Event: &replication.XIDEvent{
					XID:  1,
					GSet: mustParseMysqlGTIDSet(t, sid+":1-2"),
				},
			})
			require.NoError(t, err)

			require.Len(t, handler.calls, 1)
			require.Equal(t, mysql.Position{Name: "mysql-bin.000001", Pos: 789}, handler.calls[0].pos)
			require.Equal(t, sid+":1-2", handler.calls[0].set)
			require.Equal(t, tt.wantForce, handler.calls[0].force)
			require.Equal(t, sid+":1-2", c.master.GTIDSet().String())
		})
	}
}

func TestClosePositionSync(t *testing.T) {
	const sid = "24bc7850-2c58-11ee-be56-0242ac120002"

	tests := []struct {
		name                 string
		transactionalPosSync bool
		wantCalls            int
	}{
		{name: "legacy forces position sync on close", wantCalls: 1},
		{name: "transactional sync skips close position sync", transactionalPosSync: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, handler := newPosSyncTestCanal(t, tt.transactionalPosSync)
			c.master.Update(mysql.Position{Name: "mysql-bin.000001", Pos: 123})
			c.master.UpdateGTIDSet(mustParseMysqlGTIDSet(t, sid+":1"))

			c.Close()

			require.Len(t, handler.calls, tt.wantCalls)
			if tt.wantCalls > 0 {
				require.Nil(t, handler.calls[0].header)
				require.Equal(t, mysql.Position{Name: "mysql-bin.000001", Pos: 123}, handler.calls[0].pos)
				require.Equal(t, sid+":1", handler.calls[0].set)
				require.True(t, handler.calls[0].force)
			}
		})
	}
}

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
