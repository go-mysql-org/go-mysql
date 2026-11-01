package canal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigIgnoreTables(t *testing.T) {
	data := `
addr = "127.0.0.1:3306"
user = "root"

[dump.ignore_tables]
mydb = ["t1", "t2"]
"weird.db.name" = ["other"]
`
	cfg, err := NewConfig(data)
	require.NoError(t, err)

	require.Equal(t, map[string][]string{
		"mydb":          {"t1", "t2"},
		"weird.db.name": {"other"},
	}, cfg.Dump.IgnoreTables)
}
