package dump

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetMysqldumpVersion(t *testing.T) {
	versions := []struct {
		line    string // mysqldump --help | head -1
		version string // 9.1.0
	}{
		// Oracle MySQL
		{`mysqldump  Ver 10.13 Distrib 5.5.62, for linux-glibc2.12 (x86_64)`, `5.5.62`},
		{`mysqldump  Ver 10.13 Distrib 5.6.44, for linux-glibc2.12 (x86_64)`, `5.6.44`},
		{`mysqldump  Ver 10.13 Distrib 5.7.31, for linux-glibc2.12 (x86_64)`, `5.7.31`},
		{`mysqldump  Ver 10.13 Distrib 5.7.36, for linux-glibc2.12 (x86_64)`, `5.7.36`},
		{`mysqldump  Ver 8.0.11 for linux-glibc2.12 on x86_64 (MySQL Community Server - GPL)`, `8.0.11`},
		{`mysqldump  Ver 8.0.22 for Linux on x86_64 (MySQL Community Server - GPL)`, `8.0.22`},
		{`mysqldump  Ver 8.0.25 for Linux on x86_64 (MySQL Community Server - GPL)`, `8.0.25`},
		{`mysqldump  Ver 8.0.26 for Linux on x86_64 (MySQL Community Server - GPL)`, `8.0.26`},
		{`mysqldump  Ver 8.0.27 for Linux on x86_64 (MySQL Community Server - GPL)`, `8.0.27`},
		{`mysqldump  Ver 8.0.28 for Linux on x86_64 (MySQL Community Server - GPL)`, `8.0.28`},
		{`mysqldump  Ver 8.0.31 for Linux on x86_64 (Source distribution)`, `8.0.31`},
		{`mysqldump  Ver 8.0.32 for Linux on x86_64 (MySQL Community Server - GPL)`, `8.0.32`},
		{`mysqldump  Ver 8.4.2 for FreeBSD14.0 on amd64 (Source distribution)`, `8.4.2`},
		{`mysqldump  Ver 9.1.0 for Linux on x86_64 (MySQL Community Server - GPL)`, `9.1.0`},

		// MariaDB
		{`mysqldump  Ver 10.19 Distrib 10.3.37-MariaDB, for linux-systemd (x86_64)`, `10.3.37-MariaDB`},
		{`mysqldump  Ver 10.19 Distrib 10.6.11-MariaDB, for linux-systemd (x86_64)`, `10.6.11-MariaDB`},
		{`opt/mysql/11.0.0/bin/mysqldump from 11.0.0-preview-MariaDB, client 10.19 for linux-systemd (x86_64)`, `11.0.0-preview-MariaDB`},
		{`opt/mysql/11.2.2/bin/mysqldump from 11.2.2-MariaDB, client 10.19 for linux-systemd (x86_64)`, `11.2.2-MariaDB`},
	}

	d := new(Dumper)
	for _, v := range versions {
		ver := d.getMysqldumpVersion([]byte(v.line))
		require.Equal(t, v.version, ver, v.line)
	}
}

func TestDetectSourceDataSupported(t *testing.T) {
	versions := []struct {
		version   string
		supported bool
	}{
		{`5.7.40`, false},
		{`8.0.11`, true},
		{`8.4.1`, true},
		{`9.1.0`, true},
		{``, false},
		{`10.3.37-MariaDB`, false},
		{`11.2.2-MariaDB`, false},
	}

	d := new(Dumper)
	for _, v := range versions {
		require.Equal(t, v.supported, d.detectSourceDataSupported(v.version), v.version)
	}
}
