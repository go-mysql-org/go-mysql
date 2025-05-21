package canal

import (
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
