package server

import (
	"bytes"
	"fmt"
	"log"

	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/utils"
)

// Handler is what a server needs to implement the client-server protocol
type Handler interface {
	//handle COM_INIT_DB command, you can check whether the dbName is valid, or other.
	UseDB(dbName string) error
	//handle COM_QUERY command, like SELECT, INSERT, UPDATE, etc...
	//If Result has a Resultset (SELECT, SHOW, etc...), we will send this as the response, otherwise, we will send Result
	HandleQuery(query string) (*Result, error)
	//handle COM_FILED_LIST command
	HandleFieldList(table string, fieldWildcard string) ([]*Field, error)
	//handle COM_STMT_PREPARE, params is the param number for this statement, columns is the column number
	//context will be used later for statement execute
	HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error)
	//handle COM_STMT_EXECUTE, context is the previous one set in prepare
	//query is the statement prepare query, and args is the params for this statement
	HandleStmtExecute(context interface{}, query string, args []interface{}) (*Result, error)
	//handle COM_STMT_CLOSE, context is the previous one set in prepare
	//this handler has no response
	HandleStmtClose(context interface{}) error
	//handle any other command that is not currently handled by the library,
	//default implementation for this method will return an ER_UNKNOWN_ERROR
	HandleOtherCommand(cmd byte, data []byte) error
}

// ReplicationHandler is for handlers that want to implement the replication protocol
type ReplicationHandler interface {
	// handle Replication command
	HandleRegisterSlave(data []byte) error
	HandleBinlogDump(pos Position) (*replication.BinlogStreamer, error)
	HandleBinlogDumpGTID(gtidSet *MysqlGTIDSet) (*replication.BinlogStreamer, error)
}

// HandleCommand is handling commands received by the server
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html
func (c *Conn) HandleCommand() error {
	if c.Conn == nil {
		return fmt.Errorf("connection closed")
	}

	data, err := c.ReadPacket()
	if err != nil {
		c.Close()
		c.Conn = nil
		return err
	}

	v := c.dispatch(data)

	err = c.WriteValue(v)

	if c.Conn != nil {
		c.ResetSequence()
	}

	if err != nil {
		c.Close()
		c.Conn = nil
	}
	return err
}

func (c *Conn) dispatch(data []byte) interface{} {
	cmd := data[0]
	data = data[1:]

	switch cmd {
	case COM_QUIT:
		c.Close()
		c.Conn = nil
		return noResponse{}
	case COM_QUERY:
		if r, err := c.h.HandleQuery(utils.ByteSliceToString(data)); err != nil {
			return err
		} else {
			return r
		}
	case COM_PING:
		return nil
	case COM_INIT_DB:
		if err := c.h.UseDB(utils.ByteSliceToString(data)); err != nil {
			return err
		} else {
			return nil
		}
	case COM_FIELD_LIST:
		index := bytes.IndexByte(data, 0x00)
		table := utils.ByteSliceToString(data[0:index])
		wildcard := utils.ByteSliceToString(data[index+1:])

		if fs, err := c.h.HandleFieldList(table, wildcard); err != nil {
			return err
		} else {
			return fs
		}
	case COM_STMT_PREPARE:
		c.stmtID++
		st := new(Stmt)
		st.ID = c.stmtID
		st.Query = utils.ByteSliceToString(data)
		var err error
		if st.Params, st.Columns, st.Context, err = c.h.HandleStmtPrepare(st.Query); err != nil {
			return err
		} else {
			st.ResetParams()
			c.stmts[c.stmtID] = st
			return st
		}
	case COM_STMT_EXECUTE:
		if r, err := c.handleStmtExecute(data); err != nil {
			return err
		} else {
			return r
		}
	case COM_STMT_CLOSE:
		if err := c.handleStmtClose(data); err != nil {
			return err
		}
		return noResponse{}
	case COM_STMT_SEND_LONG_DATA:
		if err := c.handleStmtSendLongData(data); err != nil {
			return err
		}
		return noResponse{}
	case COM_STMT_RESET:
		if r, err := c.handleStmtReset(data); err != nil {
			return err
		} else {
			return r
		}
	case COM_SET_OPTION:
		if err := c.h.HandleOtherCommand(cmd, data); err != nil {
			return err
		}

		return eofResponse{}
	case COM_REGISTER_SLAVE:
		if h, ok := c.h.(ReplicationHandler); ok {
			return h.HandleRegisterSlave(data)
		} else {
			return c.h.HandleOtherCommand(cmd, data)
		}
	case COM_BINLOG_DUMP:
		if h, ok := c.h.(ReplicationHandler); ok {
			pos, err := parseBinlogDump(data)
			if err != nil {
				return err
			}
			if s, err := h.HandleBinlogDump(pos); err != nil {
				return err
			} else {
				return s
			}
		} else {
			return c.h.HandleOtherCommand(cmd, data)
		}
	case COM_BINLOG_DUMP_GTID:
		if h, ok := c.h.(ReplicationHandler); ok {
			gtidSet, err := parseBinlogDumpGTID(data)
			if err != nil {
				return err
			}
			if s, err := h.HandleBinlogDumpGTID(gtidSet); err != nil {
				return err
			} else {
				return s
			}
		} else {
			return c.h.HandleOtherCommand(cmd, data)
		}
	default:
		return c.h.HandleOtherCommand(cmd, data)
	}
}

// EmptyHandler is a mostly empty implementation for demonstration purposes
type EmptyHandler struct {
}

// EmptyReplicationHandler is a empty handler that implements the replication protocol
type EmptyReplicationHandler struct {
	EmptyHandler
}

// UseDB is called for COM_INIT_DB
func (h EmptyHandler) UseDB(dbName string) error {
	log.Printf("Received: UseDB %s", dbName)
	return nil
}

// HandleQuery is called for COM_QUERY
func (h EmptyHandler) HandleQuery(query string) (*Result, error) {
	log.Printf("Received: Query: %s", query)

	// These two queries are implemented for minimal support for MySQL Shell
	if query == `SET NAMES 'utf8mb4';` {
		return nil, nil
	}
	if query == `select concat(@@version, ' ', @@version_comment)` {
		r, err := mysql.BuildSimpleResultset([]string{"concat(@@version, ' ', @@version_comment)"}, [][]interface{}{
			{"8.0.11"},
		}, false)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(r), nil
	}

	return nil, fmt.Errorf("not supported now")
}

// HandleFieldList is called for COM_FIELD_LIST packets
// Note that COM_FIELD_LIST has been deprecated since MySQL 5.7.11
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_field_list.html
func (h EmptyHandler) HandleFieldList(table string, fieldWildcard string) ([]*Field, error) {
	log.Printf("Received: FieldList: table=%s, fieldWildcard:%s", table, fieldWildcard)
	return nil, fmt.Errorf("not supported now")
}

// HandleStmtPrepare is called for COM_STMT_PREPARE
func (h EmptyHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	log.Printf("Received: StmtPrepare: %s", query)
	return 0, 0, nil, fmt.Errorf("not supported now")
}

// 'context' isn't used but replacing it with `_` would remove important information for who
// wants to extend this later.
//revive:disable:unused-parameter

// HandleStmtExecute is called for COM_STMT_EXECUTE
func (h EmptyHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*Result, error) {
	log.Printf("Received: StmtExecute: %s (args: %v)", query, args)
	return nil, fmt.Errorf("not supported now")
}

// HandleStmtClose is called for COM_STMT_CLOSE
func (h EmptyHandler) HandleStmtClose(context interface{}) error {
	log.Println("Received: StmtClose")
	return nil
}

//revive:enable:unused-parameter

// HandleRegisterSlave is called for COM_REGISTER_SLAVE
func (h EmptyReplicationHandler) HandleRegisterSlave(data []byte) error {
	log.Printf("Received: RegisterSlave: %x", data)
	return fmt.Errorf("not supported now")
}

// HandleBinlogDump is called for COM_BINLOG_DUMP (non-GTID)
func (h EmptyReplicationHandler) HandleBinlogDump(pos Position) (*replication.BinlogStreamer, error) {
	log.Printf("Received: BinlogDump: pos=%s", pos.String())
	return nil, fmt.Errorf("not supported now")
}

// HandleBinlogDumpGTID is called for COM_BINLOG_DUMP_GTID
func (h EmptyReplicationHandler) HandleBinlogDumpGTID(gtidSet *MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	log.Printf("Received: BinlogDumpGTID: gtidSet=%s", gtidSet.String())
	return nil, fmt.Errorf("not supported now")
}

// HandleOtherCommand is called for commands not handled elsewhere
func (h EmptyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	log.Printf("Received: OtherCommand: cmd=%x, data=%x", cmd, data)
	return NewError(
		ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}
