package test_util

import "flag"

var (
	MysqlHost = flag.String("host", "127.0.0.1", "MySQL server host")
	MysqlPort = flag.String("port", "3306", "MySQL server port")

	MysqlFakeHost = flag.String("fake-host", "127.0.0.1", "MySQL fake server host")
	MysqlFakePort = flag.String("fake-port", "4000", "MySQL fake server port")
)
