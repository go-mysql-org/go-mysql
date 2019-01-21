// Copyright 2019 ByteWatch All Rights Reserved.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"encoding/json"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"strings"
	"sync"
)

const (
	defaultCharsetClient = "utf8mb4"
)

type Executor struct {
	cfg *Config

	parser     *parser.Parser
	enterError error

	databases map[string]*DatabaseDef
	sync.Mutex

	// Default database after exec `use` statement
	currentDatabase string
}

func NewExecutor(cfg *Config) *Executor {
	if cfg.CharsetServer == "" {
		cfg.CharsetServer = "latin1"
	}
	executor := Executor{
		cfg:       cfg,
		parser:    parser.New(),
		databases: make(map[string]*DatabaseDef),
	}

	return &executor
}

func (o *Executor) getSqlName(str model.CIStr) string {
	if o.cfg.LowerCaseTableNames {
		return str.L
	}
	return str.O
}

func (o *Executor) getSqlName2(str string) string {
	if o.cfg.LowerCaseTableNames {
		return strings.ToLower(str)
	}
	return str
}

func (o *Executor) enterUseStmt(stmt *ast.UseStmt) error {
	databaseName := o.getSqlName2(stmt.DBName)
	if _, ok := o.databases[databaseName]; !ok {
		return ErrBadDB.Gen(databaseName)
	}

	o.currentDatabase = databaseName

	return nil
}

func (o *Executor) enterCreateDatabaseStmt(stmt *ast.CreateDatabaseStmt) error {
	databaseName := o.getSqlName2(stmt.Name)
	if _, ok := o.databases[databaseName]; ok {
		// Check if not exists
		if !stmt.IfNotExists {
			return ErrErrDBCreateExists.Gen(databaseName)
		}
		return nil
	}
	databaseCharset := ""
	for _, databaseOption := range stmt.Options {
		if databaseOption.Tp == ast.DatabaseOptionCharset {
			databaseCharset = databaseOption.Value
		}
	}
	if databaseCharset == "" {
		databaseCharset = o.cfg.CharsetServer
	}

	// New database def
	databaseDef := DatabaseDef{
		Name:    databaseName,
		Charset: databaseCharset,
		Tables:  make(map[string]*TableDef),
	}
	o.databases[databaseName] = &databaseDef

	return nil
}

// Get specified database or current database
func (o *Executor) getSpecifiedOrDefaultDb(databaseName string) (*DatabaseDef, error) {
	if databaseName == "" {
		databaseName = o.currentDatabase
	}
	if databaseName == "" {
		// No database selected
		return nil, ErrNoDB
	}

	databaseDef, ok := o.databases[databaseName]
	if !ok {
		return nil, ErrBadDB.Gen(databaseName)
	}

	return databaseDef, nil
}

// Refer MySQL source: mysql_prepare_alter_table()
func (o *Executor) enterAlterTableStmt(stmt *ast.AlterTableStmt) error {
	var err error
	databaseName := o.getSqlName(stmt.Table.Schema)
	tableName := o.getSqlName(stmt.Table.Name)
	databaseDef, err := o.getSpecifiedOrDefaultDb(databaseName)
	if err != nil {
		return err
	}
	tableDef := databaseDef.cloneTable(tableName)
	if tableDef == nil {
		return ErrNoSuchTable.Gen(databaseDef.Name, tableName)
	}

	// First remove columns need to drop
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableDropColumn:
			columnName := spec.OldColumnName.Name.O
			err = tableDef.dropColumn(columnName)
		}

		if err != nil {
			return err
		}
	}

	// Handle columns need to modidy or change
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableModifyColumn:
			specNewColumn := spec.NewColumns[0]
			originalName := specNewColumn.Name.Name.O
			err = tableDef.changeColumn(originalName, specNewColumn, spec.Position)

		case ast.AlterTableChangeColumn:
			specNewColumn := spec.NewColumns[0]
			originalName := spec.OldColumnName.Name.O
			err = tableDef.changeColumn(originalName, specNewColumn, spec.Position)

		case ast.AlterTableAlterColumn:
			// Nothing to do
		}

		if err != nil {
			return err
		}
	}

	// Handle columns new added
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			// Can not add more than one column in one spec
			column := spec.NewColumns[0]
			err = tableDef.addColumn(column, spec.Position)
		}
		if err != nil {
			return err
		}
	}

	// Drop index
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableDropIndex:
			indexName := spec.Name
			err = tableDef.dropIndex(indexName)

		case ast.AlterTableDropForeignKey:

		case ast.AlterTableDropPrimaryKey:
			err = tableDef.dropIndex("PRIMARY")
		}

		if err != nil {
			return err
		}
	}

	// Clean indices whose columns are all dropped
	tableDef.cleanIndices()

	// Rename index
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableRenameIndex:
			err = tableDef.renameIndex(spec.FromKey.O, spec.ToKey.O)
		}
		if err != nil {
			return err
		}
	}

	// Add new index if exists
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAddConstraint:
			constraint := spec.Constraint
			if constraint.Tp == ast.ConstraintForeignKey {
				// The is a froeign key
				err = tableDef.addForeignKey(constraint)
				break
			}

			key := getIndexType(constraint.Tp)
			if key == IndexType_NONE {
				// Nothing to do
				break
			}

			indexName := constraint.Name
			columns := make([]string, len(constraint.Keys))
			for i, columnName := range constraint.Keys {
				columns[i] = columnName.Column.Name.L
			}
			err = tableDef.addExplicitIndex(indexName, columns, key)
		}

		if err != nil {
			return err
		}
	}

	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableRenameTable:
			newDatabaseName := o.getSqlName(spec.NewTable.Schema)
			newTableName := o.getSqlName(spec.NewTable.Name)
			if newDatabaseName == databaseName && newTableName == tableName {
				// Nothing to do
				break
			} else if newDatabaseName != databaseName {
				// The new table name is in another database
				newDatabaseDef, err := o.getSpecifiedOrDefaultDb(newDatabaseName)
				if err != nil {
					return err
				}

				if newDatabaseDef.findTable(newTableName) != nil {
					return ErrTableExists.Gen(newTableName)
				}

				databaseDef.dropTable(tableName)
				tableDef.Name = newTableName
				newDatabaseDef.setTable(newTableName, tableDef)

			} else {
				// The new table name is still in the original database
				if databaseDef.findTable(newTableName) != nil &&
					newTableName != tableName {
					return ErrTableExists.Gen(newTableName)
				}
				databaseDef.dropTable(tableName)
				tableDef.Name = newTableName
				databaseDef.setTable(newTableName, tableDef)
			}

		case ast.AlterTableOption:
			// Nothing to do

		}

		if err != nil {
			return err
		}
	}

	// Make sure the table named `tableName` has not been renamed.
	// If the table doesn't exists already, not need to update databaseDef
	if databaseDef.findTable(tableName) != nil {
		tableDef.cleanIndices()
		tableDef.sortIndices()
		databaseDef.setTable(tableName, tableDef)
	}

	return nil
}

func (o *Executor) enterCreateTableStmt(stmt *ast.CreateTableStmt) error {
	var err error

	databaseName := o.getSqlName(stmt.Table.Schema)
	tableName := o.getSqlName(stmt.Table.Name)

	databaseDef, err := o.getSpecifiedOrDefaultDb(databaseName)
	if err != nil {
		return err
	}
	if databaseDef.findTable(tableName) != nil {
		if !stmt.IfNotExists {
			return ErrTableExists.Gen(tableName)
		}
		return nil
	}

	if stmt.ReferTable != nil {
		// Create table like another table
		referDatabase := o.getSqlName(stmt.ReferTable.Schema)
		referTable := o.getSqlName(stmt.ReferTable.Name)

		referDatabaseDef, err := o.getSpecifiedOrDefaultDb(referDatabase)
		if err != nil {
			return err
		}
		tableDef := referDatabaseDef.cloneTable(referTable)
		if tableDef == nil {
			return ErrNoSuchTable.Gen(referDatabase, referTable)
		}
		tableDef.Name = tableName
		databaseDef.setTable(tableName, tableDef)
		return nil
	}

	tableCharset := databaseDef.Charset
	for _, tableOption := range stmt.Options {
		if tableOption.Tp == ast.TableOptionCharset {
			tableCharset = tableOption.StrValue
		}
	}

	// New table def
	tableDef := TableDef{
		Name:    tableName,
		Charset: tableCharset,
	}

	for _, column := range stmt.Cols {
		err = tableDef.addColumn(column, nil)
		if err != nil {
			return err
		}
	}

	// Check table constraint
	for _, constraint := range stmt.Constraints {
		key := getIndexType(constraint.Tp)
		if key == IndexType_NONE {
			// Nothing to do
			continue
		}

		indexName := constraint.Name
		columns := make([]string, len(constraint.Keys))
		for i, columnName := range constraint.Keys {
			columns[i] = columnName.Column.Name.L
		}
		err = tableDef.addExplicitIndex(indexName, columns, key)
		if err != nil {
			return err
		}
	}

	tableDef.cleanIndices()
	tableDef.sortIndices()
	databaseDef.setTable(tableName, &tableDef)

	return nil
}

func (o *Executor) enterCreateIndexStmt(stmt *ast.CreateIndexStmt) error {
	var err error
	databaseName := o.getSqlName(stmt.Table.Schema)
	tableName := o.getSqlName(stmt.Table.Name)

	databaseDef, err := o.getSpecifiedOrDefaultDb(databaseName)
	if err != nil {
		return err
	}
	tableDef := databaseDef.cloneTable(tableName)
	if tableDef == nil {
		return ErrNoSuchTable.Gen(databaseDef.Name, tableName)
	}

	var key IndexType
	if stmt.Unique {
		key = IndexType_UNI
	} else {
		key = IndexType_MUL
	}
	indexName := stmt.IndexName
	columns := make([]string, len(stmt.IndexColNames))
	for i, columnName := range stmt.IndexColNames {
		columns[i] = columnName.Column.Name.L
	}

	err = tableDef.addExplicitIndex(indexName, columns, key)
	if err != nil {
		return err
	}

	tableDef.cleanIndices()
	tableDef.sortIndices()
	databaseDef.setTable(tableName, tableDef)
	return nil
}

func (o *Executor) enterDropDatabaseStmt(stmt *ast.DropDatabaseStmt) error {
	databaseName := stmt.Name
	if _, ok := o.databases[databaseName]; !ok {
		if !stmt.IfExists {
			return ErrDBDropExists.Gen(databaseName)
		}
		return nil
	}

	delete(o.databases, databaseName)
	if databaseName == o.currentDatabase {
		// Drop the current db will de-select any db
		o.currentDatabase = ""
	}
	return nil
}

func (o *Executor) enterDropTableStmt(stmt *ast.DropTableStmt) (err error) {
	for _, table := range stmt.Tables {
		var databaseDef *DatabaseDef
		databaseName := o.getSqlName(table.Schema)
		tableName := o.getSqlName(table.Name)

		databaseDef, err = o.getSpecifiedOrDefaultDb(databaseName)
		if err != nil {
			return
		}
		if databaseDef.findTable(tableName) == nil {
			if !stmt.IfExists {
				err = ErrBadTable.Gen(databaseDef.Name, tableName)
				return
			}
			continue
		}
		// We do really drop table only not error happens, to achive 'atomic/consitent'.
		// This behavior is different with MySQL !!
		// MySQL is not 'atomic/consitent' when drop multi tables.
		defer func() {
			if err != nil && o.cfg.NeedAtomic {
				return
			}
			databaseDef.dropTable(tableName)
		}()
	}

	return
}

func (o *Executor) enterDropIndexStmt(stmt *ast.DropIndexStmt) error {
	databaseName := o.getSqlName(stmt.Table.Schema)
	tableName := o.getSqlName(stmt.Table.Name)

	databaseDef, err := o.getSpecifiedOrDefaultDb(databaseName)
	if err != nil {
		return err
	}
	tableDef := databaseDef.cloneTable(tableName)
	if tableDef == nil {
		return ErrNoSuchTable.Gen(databaseDef.Name, tableName)
	}

	indexName := stmt.IndexName

	err = tableDef.dropIndex(indexName)
	if err != nil {
		return err
	}

	databaseDef.setTable(tableName, tableDef)
	return nil

}

func (o *Executor) enterRenameTableStmt(stmt *ast.RenameTableStmt) error {
	oldDatabaseName := o.getSqlName(stmt.OldTable.Schema)
	oldTableName := o.getSqlName(stmt.OldTable.Name)
	newDatabaseName := o.getSqlName(stmt.NewTable.Schema)
	newTableName := o.getSqlName(stmt.NewTable.Name)

	oldDatabaseDef, err := o.getSpecifiedOrDefaultDb(oldDatabaseName)
	if err != nil {
		return err
	}
	tableDef := oldDatabaseDef.cloneTable(oldTableName)
	if tableDef == nil {
		return ErrNoSuchTable.Gen(oldDatabaseDef.Name, oldTableName)
	}

	if newDatabaseName == oldDatabaseName && newTableName == oldTableName {
		// Nothing to do
		return nil
	}

	if newDatabaseName != oldDatabaseName {
		// The new table name is in another database
		newDatabaseDef, err := o.getSpecifiedOrDefaultDb(newDatabaseName)
		if err != nil {
			return err
		}

		if newDatabaseDef.findTable(newTableName) != nil {
			return ErrTableExists.Gen(newTableName)
		}

		oldDatabaseDef.dropTable(oldTableName)
		tableDef.Name = newTableName
		newDatabaseDef.setTable(newTableName, tableDef)

	} else {
		// The new table name is still in the original database
		if oldDatabaseDef.findTable(newTableName) != nil &&
			newTableName != oldTableName {
			return ErrTableExists.Gen(newTableName)
		}
		oldDatabaseDef.dropTable(oldTableName)
		tableDef.Name = newTableName
		oldDatabaseDef.setTable(newTableName, tableDef)
	}

	return nil
}

func (o *Executor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	var err error
	switch stmt := in.(type) {
	case *ast.UseStmt:
		err = o.enterUseStmt(stmt)

	case *ast.CreateDatabaseStmt:
		err = o.enterCreateDatabaseStmt(stmt)

	case *ast.CreateTableStmt:
		err = o.enterCreateTableStmt(stmt)

	case *ast.CreateIndexStmt:
		err = o.enterCreateIndexStmt(stmt)

	case *ast.DropDatabaseStmt:
		err = o.enterDropDatabaseStmt(stmt)

	case *ast.DropTableStmt:
		err = o.enterDropTableStmt(stmt)

	case *ast.DropIndexStmt:
		err = o.enterDropIndexStmt(stmt)

	case *ast.AlterTableStmt:
		err = o.enterAlterTableStmt(stmt)

	case *ast.RenameTableStmt:
		err = o.enterRenameTableStmt(stmt)
	}

	o.enterError = err
	return in, true
}

func (o *Executor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (o *Executor) IsDdl(sql string) (bool, error) {
	o.Lock()
	defer o.Unlock()

	node, err := o.parser.ParseOneStmt(sql, defaultCharsetClient, "")
	if err != nil {
		return false, err
	}

	switch node.(type) {
	case *ast.CreateDatabaseStmt:
		return true, nil

	case *ast.CreateTableStmt:
		return true, nil

	case *ast.CreateIndexStmt:
		return true, nil

	case *ast.DropDatabaseStmt:
		return true, nil

	case *ast.DropTableStmt:
		return true, nil

	case *ast.DropIndexStmt:
		return true, nil

	case *ast.AlterTableStmt:
		return true, nil

	case *ast.RenameTableStmt:
		return true, nil
	}

	return false, nil

}

func (o *Executor) Exec(sql string) error {
	o.Lock()
	defer o.Unlock()

	o.enterError = nil
	nodes, err := o.parser.Parse(sql, defaultCharsetClient, "")
	if err != nil {
		return err
	}

	for _, node := range nodes {
		node.Accept(o)
		if o.enterError != nil {
			return o.enterError
		}
	}

	return nil
}

// Show all database names
func (o *Executor) GetDatabases() []string {
	o.Lock()
	defer o.Unlock()

	databases := make([]string, 0)
	for databaseName := range o.databases {
		databases = append(databases, databaseName)
	}

	return databases
}

// Show all table names in specified database
func (o *Executor) GetTables(databaseName string) ([]string, error) {
	o.Lock()
	defer o.Unlock()

	databaseName = o.getSqlName2(databaseName)
	databaseDef, ok := o.databases[databaseName]
	if !ok {
		return nil, ErrBadDB.Gen(databaseName)
	}

	return databaseDef.getTables(), nil

}

// Get definition of specified table
func (o *Executor) GetTableDef(databaseName, tableName string) (*TableDef, error) {
	o.Lock()
	defer o.Unlock()

	databaseName = o.getSqlName2(databaseName)
	tableName = o.getSqlName2(tableName)

	databaseDef, ok := o.databases[databaseName]
	if !ok {
		return nil, ErrBadDB.Gen(databaseName)
	}
	tableDef := databaseDef.cloneTable(tableName)
	if tableDef == nil {
		return nil, ErrNoSuchTable.Gen(databaseName, tableName)
	}

	return tableDef, nil

}

func (o *Executor) GetCurrentDatabase() string {
	o.Lock()
	defer o.Unlock()
	return o.currentDatabase
}

// Take a snapshot of this Executor, returned bytes is json encoded
func (o *Executor) Snapshot() ([]byte, error) {
	o.Lock()
	defer o.Unlock()
	return json.Marshal(o.databases)

}

// Restore from snaphot
func (o *Executor) Restore(data []byte) error {
	o.Lock()
	defer o.Unlock()
	if data == nil {
		o.databases = make(map[string]*DatabaseDef)
		return nil
	}

	var databases map[string]*DatabaseDef
	err := json.Unmarshal(data, &databases)
	if err != nil {
		return err
	}
	o.databases = databases
	return nil
}

// Reset everything under this Executor
func (o *Executor) Reset() {
	o.Lock()
	defer o.Unlock()
	o.databases = make(map[string]*DatabaseDef)
}

func getIndexType(constraintType ast.ConstraintType) IndexType {
	var key IndexType
	switch constraintType {
	case ast.ConstraintPrimaryKey:
		key = IndexType_PRI
	case ast.ConstraintUniqKey, ast.ConstraintUniqIndex, ast.ConstraintUniq:
		key = IndexType_UNI
	case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintFulltext:
		key = IndexType_MUL
	default:
		key = IndexType_NONE
	}
	return key
}
