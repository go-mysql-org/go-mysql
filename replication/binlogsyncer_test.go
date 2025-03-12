package replication

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalHostname(t *testing.T) {
	b := BinlogSyncer{
		cfg: BinlogSyncerConfig{
			Localhost: "foobar",
		},
	}

	require.Equal(t, "foobar", b.localHostname())
}

func TestLocalHostname_long(t *testing.T) {
	b := BinlogSyncer{
		cfg: BinlogSyncerConfig{
			Localhost: strings.Repeat("x", 255),
		},
	}

	require.Equal(t, 255, len(b.localHostname()))
}

func TestLocalHostname_toolong(t *testing.T) {
	b := BinlogSyncer{
		cfg: BinlogSyncerConfig{
			Localhost: strings.Repeat("x", 300),
		},
	}

	require.Equal(t, 255, len(b.localHostname()))
}

func TestLocalHostname_os(t *testing.T) {
	b := BinlogSyncer{
		cfg: BinlogSyncerConfig{
			Localhost: "",
		},
	}

	h, _ := os.Hostname()
	require.Equal(t, h, b.localHostname())
}
