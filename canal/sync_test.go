package canal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestIsImpossibleBinlogPositionError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "mysql impossible position",
			err:  errors.New("ERROR 1236 (HY000): Client requested master to start replication from impossible position; the first event 'binlog.000001' at 500, the last event read from 'binlog.000001' at 4, the last byte read from 'binlog.000001' at 4."),
			want: true,
		},
		{
			name: "generic impossible position text",
			err:  errors.New("start sync replication failed because impossible position requested"),
			want: true,
		},
		{
			name: "other mysql error",
			err:  errors.New("ERROR 1045 (28000): Access denied for user"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isImpossibleBinlogPositionError(tt.err)
			require.Equal(t, tt.want, got)
		})
	}
}
