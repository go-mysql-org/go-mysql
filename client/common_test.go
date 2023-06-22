package client

import (
	"flag"
	"strings"
	"testing"

	. "github.com/pingcap/check"

	"github.com/go-mysql-org/go-mysql/test_util"
)

var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")

func Test(t *testing.T) {
	// We cover the whole range of MySQL server versions using docker-compose to bind them to different ports for testing.
	// MySQL is constantly updating auth plugin to make it secure:
	// starting from MySQL 8.0.4, a new auth plugin is introduced, causing plain password auth to fail with error:
	// ERROR 1251 (08004): Client does not support authentication protocol requested by server; consider upgrading MySQL client
	// Hint: use docker-compose to start corresponding MySQL docker containers and add their ports here

	segs := strings.Split(*test_util.MysqlPort, ",")
	for _, seg := range segs {
		Suite(&clientTestSuite{port: seg})
		Suite(&connTestSuite{port: seg})
		Suite(&poolTestSuite{port: seg})
	}
	TestingT(t)
}
