package schema

import (
	"fmt"
	"github.com/bytewatch/ddl-executor"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
)

var HAHealthCheckSchema = "mysql.ha_health_check"

type SchemaTracker struct {
	cfg *TrackerConfig

	curPos mysql.Position

	executor *executor.Executor

	storage SchemaStorage
}

// New a schema tracker that can track DDL statements, making a schema mirror.
func NewSchemaTracker(cfg *TrackerConfig) (*SchemaTracker, error) {
	var err error
	var storage SchemaStorage

	switch cfg.Storage {
	case StorageType_Boltdb:
		storage, err = NewBoltdbStorage(cfg.Dir + "/schema.dat")
	case StorageType_Mysql:
		storage, err = NewMysqlStorage(cfg.Addr, cfg.User, cfg.Password, cfg.Database)
	default:
		err = fmt.Errorf("unknown storage type: %s", cfg.Storage)
	}
	if err != nil {
		log.Errorf("new storage error: %s", err)
		return nil, err
	}

	// Restore schema from storage, into memory
	snapshot, pos, err := storage.LoadLastSnapshot()
	if err != nil {
		log.Errorf("load last snapshot from storage error: %s", err)
		return nil, err
	}

	executor := executor.NewExecutor(&executor.Config{
		CharsetServer:       cfg.CharsetServer,
		LowerCaseTableNames: true,
		NeedAtomic:          true,
	})
	err = executor.Restore(snapshot)
	if err != nil {
		log.Errorf("set snapshot to executor error: %s", err)
		return nil, err
	}

	// TODO: Replay all statements after last snapshot

	tracker := &SchemaTracker{
		cfg:      cfg,
		executor: executor,
		storage:  storage,
		curPos:   pos,
	}

	return tracker, nil
}

// Check whether the SQL statement is DDL, means not DML/DCL
func (o *SchemaTracker) IsDdl(sql string) (bool, error) {
	return o.executor.IsDdl(sql)
}

// Persistent the schema info into storage.
// Before Persist is called, must ensure the binlog DML events previous is synced.
func (o *SchemaTracker) Persist(pos mysql.Position) error {
	snapshot, err := o.executor.Snapshot()
	if err != nil {
		log.Errorf("get executor snapshot error: %s", err)
		return err
	}
	err = o.storage.SaveSnapshot(snapshot, pos)
	if err != nil {
		log.Errorf("save snapshot error: %s", err)
		return err
	}

	log.Infof("save snapshot succeed, pos: %s", pos)
	o.curPos = pos

	return nil
}

// Exec ddl statement, and persistent the schema info into storage
func (o *SchemaTracker) ExecAndPersist(db string, statement string, pos mysql.Position) error {
	var err error

	// Check whether this ddl we have executed already.
	// The comparison here doesn't care about server_id in order to
	// be compatible with go-mysql/mysql.Position struct.
	if pos.Compare(o.curPos) == 0 {
		log.Warnf("this statement has been executed before: %s", pos)
		return nil
	}

	err = o.Exec(db, statement)
	if err != nil {
		return err
	}

	if o.needTriggerSnapshot() {
		snapshot, err := o.executor.Snapshot()
		if err != nil {
			log.Errorf("get executor snapshot error: %s", err)
			return err
		}
		err = o.storage.SaveSnapshot(snapshot, pos)
		if err != nil {
			log.Errorf("save snapshot error: %s", err)
			return err
		}
		log.Infof("save snapshot succeed, pos: %s", pos)
	} else {
		o.storage.SaveStatement(db, statement, pos)
		if err != nil {
			log.Errorf("save statement error: %s", err)
			return err
		}
		log.Infof("save statement succeed, pos: %s", pos)
	}

	o.curPos = pos

	return nil
}

// Exec ddl statement, but don't persistent the schema info
func (o *SchemaTracker) Exec(db string, statement string) error {
	var err error
	if db != "" {
		sql := "USE " + db
		err = o.executor.Exec(sql)
		if err != nil {
			log.Errorf("execute sql error: %s, sql: %s", err, sql)
			return err
		}
	}

	log.Infof("executing sql: %s", statement)
	err = o.executor.Exec(statement)
	if err != nil {
		log.Errorf("execute sql error: %s, sql: %s", err, statement)
		return err
	}

	return nil
}

func (o *SchemaTracker) GetTableDef(db string, table string) (*TableDef, error) {
	t, err := o.executor.GetTableDef(db, table)
	if err != nil {
		// work around : RDS HAHeartBeat
		// ref : https://github.com/alibaba/canal/blob/master/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L385
		// issue : https://github.com/alibaba/canal/issues/222
		// This is a common error in RDS that canal can't get HAHealthCheckSchema's meta, so we mock a table meta.
		// If canal just skip and log error, as RDS HA heartbeat interval is very short, so too many HAHeartBeat errors will be logged.
		key := fmt.Sprintf("%s.%s", db, table)
		if key == HAHealthCheckSchema {
			// mock ha_health_check meta
			tableDef := &TableDef{
				Database: db,
				Name:     table,
				Columns:  make([]*ColumnDef, 0, 2),
			}
			tableDef.Columns = append(
				tableDef.Columns, &ColumnDef{
					Name:      "id",
					Type:      "bigint(20)",
					InnerType: TypeLong,
				}, &ColumnDef{
					Name:      "type",
					Type:      "char(1)",
					InnerType: TypeVarString,
				})
			return tableDef, nil
		}
		return nil, err
	}

	return makeTableDef(t), nil
}

// Get all db names, like 'SHOW DATABASES'
func (o *SchemaTracker) GetDatabases() []string {
	return o.executor.GetDatabases()
}

// Get all table names in specified db, like 'SHOW TABLES'
func (o *SchemaTracker) GetTables(db string) ([]string, error) {
	return o.executor.GetTables(db)
}

// Reset executor's schema info and storage
func (o *SchemaTracker) Reset() error {
	o.executor.Reset()
	err := o.storage.Reset()
	if err != nil {
		log.Errorf("reset storage error: %s", err)
		return err
	}
	return nil
}

func (o *SchemaTracker) needTriggerSnapshot() bool {
	// TODO
	return true
}

func (o *SchemaTracker) replayNextStatement() {
	// TODO
}

func (o *SchemaTracker) replayAllStatements() {
	// TODO
}
