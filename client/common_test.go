package client

import (
	"flag"
)

var (
	testUser     = flag.String("user", "root", "MySQL user")
	testPassword = flag.String("pass", "", "MySQL password")
	testDB       = flag.String("db", "test", "MySQL test database")
)
