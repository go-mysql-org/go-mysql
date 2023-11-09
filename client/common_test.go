package client

import (
	"flag"

	. "github.com/pingcap/check"
)

var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")
