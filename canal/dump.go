package canal

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/errors"
	"github.com/shopspring/decimal"
	"github.com/siddontang/go-log/log"
)

type dumpParseHandler struct {
	c    *Canal
	name string
	pos  uint64
	gset mysql.GTIDSet
}

func (h *dumpParseHandler) BinLog(name string, pos uint64) error {
	h.name = name
	h.pos = pos
	return nil
}

func (h *dumpParseHandler) GtidSet(gtidsets string) (err error) {
	if h.gset != nil {
		err = h.gset.Update(gtidsets)
	} else {
		h.gset, err = mysql.ParseGTIDSet("mysql", gtidsets)
	}
	return err
}

func (h *dumpParseHandler) Data(db string, table string, values []string) error {
	if err := h.c.ctx.Err(); err != nil {
		return err
	}

	tableInfo, err := h.c.GetTable(db, table)
	if err != nil {
		e := errors.Cause(err)
		if e == ErrExcludedTable ||
			e == schema.ErrTableNotExist ||
			e == schema.ErrMissingTableMeta {
			return nil
		}
		log.Errorf("get %s.%s information err: %v", db, table, err)
		return errors.Trace(err)
	}

	vs := make([]interface{}, len(values))

	for i, v := range values {
		if v == "NULL" {
			vs[i] = nil
		} else if v == "_binary ''" {
			vs[i] = []byte{}
		} else if v[0] != '\'' {
			if tableInfo.Columns[i].Type == schema.TYPE_NUMBER || tableInfo.Columns[i].Type == schema.TYPE_MEDIUM_INT {
				var n interface{}
				var err error

				if tableInfo.Columns[i].IsUnsigned {
					n, err = strconv.ParseUint(v, 10, 64)
				} else {
					n, err = strconv.ParseInt(v, 10, 64)
				}

				if err != nil {
					return fmt.Errorf("parse row %v at %d error %v, int expected", values, i, err)
				}

				vs[i] = n
			} else if tableInfo.Columns[i].Type == schema.TYPE_FLOAT {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return fmt.Errorf("parse row %v at %d error %v, float expected", values, i, err)
				}
				vs[i] = f
			} else if tableInfo.Columns[i].Type == schema.TYPE_DECIMAL {
				if h.c.cfg.UseDecimal {
					d, err := decimal.NewFromString(v)
					if err != nil {
						return fmt.Errorf("parse row %v at %d error %v, decimal expected", values, i, err)
					}
					vs[i] = d
				} else {
					f, err := strconv.ParseFloat(v, 64)
					if err != nil {
						return fmt.Errorf("parse row %v at %d error %v, float expected", values, i, err)
					}
					vs[i] = f
				}
			} else if strings.HasPrefix(v, "0x") {
				buf, err := hex.DecodeString(v[2:])
				if err != nil {
					return fmt.Errorf("parse row %v at %d error %v, hex literal expected", values, i, err)
				}
				vs[i] = string(buf)
			} else {
				return fmt.Errorf("parse row %v error, invalid type at %d", values, i)
			}
		} else {
			vs[i] = v[1 : len(v)-1]
		}
	}

	events := newRowsEvent(tableInfo, InsertAction, [][]interface{}{vs}, nil)
	return h.c.eventHandler.OnRow(events)
}

func (c *Canal) AddDumpDatabases(dbs ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddDatabases(dbs...)
}

func (c *Canal) AddDumpTables(db string, tables ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddTables(db, tables...)
}

func (c *Canal) AddDumpIgnoreTables(db string, tables ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddIgnoreTables(db, tables...)
}

func (c *Canal) dump() error {
	if c.dumper == nil {
		return errors.New("mysqldump does not exist")
	}

	c.master.UpdateTimestamp(uint32(time.Now().Unix()))

	h := &dumpParseHandler{c: c}
	// If users call StartFromGTID with empty position to start dumping with gtid,
	// we record the current gtid position before dump starts.
	//
	// See tryDump() to see when dump is skipped.
	if c.master.GTIDSet() != nil {
		gset, err := c.GetMasterGTIDSet()
		if err != nil {
			return errors.Trace(err)
		}
		h.gset = gset
	}

	if c.cfg.Dump.SkipMasterData {
		pos, err := c.GetMasterPos()
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("skip master data, get current binlog position %v", pos)
		h.name = pos.Name
		h.pos = uint64(pos.Pos)
	}

	start := time.Now()
	log.Info("try dump MySQL and parse")
	if err := c.dumper.DumpAndParse(h); err != nil {
		return errors.Trace(err)
	}

	pos := mysql.Position{Name: h.name, Pos: uint32(h.pos)}
	c.master.Update(pos)
	c.master.UpdateGTIDSet(h.gset)
	if err := c.eventHandler.OnPosSynced(pos, c.master.GTIDSet(), true); err != nil {
		return errors.Trace(err)
	}
	var startPos fmt.Stringer = pos
	if h.gset != nil {
		c.master.UpdateGTIDSet(h.gset)
		startPos = h.gset
	}
	log.Infof("dump MySQL and parse OK, use %0.2f seconds, start binlog replication at %s",
		time.Now().Sub(start).Seconds(), startPos)
	return nil
}

func (c *Canal) tryDump() error {
	pos := c.master.Position()
	gset := c.master.GTIDSet()
	if (len(pos.Name) > 0 && pos.Pos > 0) ||
		(gset != nil && gset.String() != "") {
		// we will sync with binlog name and position
		log.Infof("skip dump, use last binlog replication pos %s or GTID set %v", pos, gset)
		return nil
	}

	if c.dumper == nil {
		log.Info("skip dump, no mysqldump")
		return nil
	}

	return c.dump()
}
