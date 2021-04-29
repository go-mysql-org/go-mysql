package dump

import (
	"flag"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"
)

// use docker mysql for test
var host = flag.String("host", "127.0.0.1", "MySQL host")
var port = flag.Int("port", 3306, "MySQL host")

var execution = flag.String("exec", "mysqldump", "mysqldump execution path")

func Test(t *testing.T) {
	TestingT(t)
}

type testParseHandler struct {
	gset mysql.GTIDSet
}

func (h *testParseHandler) BinLog(name string, pos uint64) error {
	return nil
}

func (h *testParseHandler) GtidSet(gtidsets string) (err error) {
	if h.gset != nil {
		err = h.gset.Update(gtidsets)
	} else {
		h.gset, err = mysql.ParseGTIDSet("mysql", gtidsets)
	}
	return err
}

func (h *testParseHandler) Data(schema string, table string, values []string) error {
	return nil
}
