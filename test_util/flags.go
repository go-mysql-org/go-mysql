package test_util

import (
	"flag"
	"os"
)

var (
	MysqlHost    = flag.String("host", "127.0.0.1", "MySQL server host")
	MysqlPort    = flag.String("port", "3306", "MySQL server port")
	ServerFlavor = os.Getenv("MYSQL_FLAVOR")

	MysqlFakeHost = flag.String("fake-host", "127.0.0.1", "MySQL fake server host")
	MysqlFakePort = flag.String("fake-port", "4000", "MySQL fake server port")
)
