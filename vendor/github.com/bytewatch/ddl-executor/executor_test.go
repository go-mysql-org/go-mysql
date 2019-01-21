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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIsDdl(t *testing.T) {
	executor := NewExecutor(NewDefaultConfig())

	ddl := []string{
		"create schema test1",
		"create database test1",
		"create table if not exists t like other",
		"alter table t add column (a smallint unsigned)",
		"drop database test1",
		"drop table t",
		"CREATE INDEX idx ON t (a)",
		"CREATE UNIQUE INDEX idx ON t (a)",
		"drop index idx on t",
		"rename table t to t2",
	}

	notDdl := []string{
		"select * from t",
		"insert into t set col = 0",
		"update t set col = 0 where id = 1",
		"delete from t",
		"grant all on *.* to 'root'@'%'",
	}

	for _, sql := range ddl {
		isDdl, err := executor.IsDdl(sql)
		require.Nil(t, err)
		require.True(t, isDdl)
	}
	for _, sql := range notDdl {
		isDdl, err := executor.IsDdl(sql)
		require.Nil(t, err)
		require.False(t, isDdl)
	}
}

func TestCreateTable(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_PRI,
		Charset:   "",
		Unsigned:  true,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_UNI,
		Charset:   "utf8",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "addr",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_UNI,
		Charset:   "gbk",
		Unsigned:  false,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_UNI,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "age",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_UNI,
		Charset:   "",
		Unsigned:  false,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "score",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_MUL,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "comment",
		Type:      "text",
		InnerType: TypeBlob,
		Key:       IndexType_NONE,
		Charset:   "gbk",
		Unsigned:  false,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "data",
		Type:      "json",
		InnerType: TypeJSON,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  false,
		Nullable:  true,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment primary key,
		name varchar(255) CHARACTER SET utf8 not null default '' unique key,
		addr varchar(255),
		phone int not null,
		age int default 20 unique,
		score int not null default 0,
		comment text,
		data json,
		unique key(id),
		key(name),
		unique (addr),
		unique index(phone),
		key (phone),
		key(age),
		key(score)
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)

	require.Equal(t, "PRIMARY", tableDef.Indices[0].Name)
	require.Equal(t, "name", tableDef.Indices[1].Name)
	require.Equal(t, "id", tableDef.Indices[2].Name)
	require.Equal(t, "phone", tableDef.Indices[3].Name)
	require.Equal(t, "age", tableDef.Indices[4].Name)
	require.Equal(t, "addr", tableDef.Indices[5].Name)

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestCreateTableWithLike(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test2",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_PRI,
		Charset:   "",
		Unsigned:  true,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create schema test;
	create table test.test1(
		id int unsigned auto_increment,
		primary key (id)
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	use test;
	create table test2 like test1;`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test2")
	require.Nil(t, err)

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestAlterTableAddColumn(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_PRI,
		Charset:   "",
		Unsigned:  true,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_UNI,
		Charset:   "utf8",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "addr",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_NONE,
		Charset:   "gbk",
		Unsigned:  false,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_UNI,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment primary key,
		name varchar(255) CHARACTER SET utf8 not null default '' unique key
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 
		add column addr varchar(255), 
		add column phone int not null unique
	`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestAlterTableAddColumnWithPos(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_PRI,
		Charset:   "",
		Unsigned:  true,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "addr",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_NONE,
		Charset:   "gbk",
		Unsigned:  false,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_UNI,
		Charset:   "utf8",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		name varchar(255) CHARACTER SET utf8 not null default '' unique key
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 
		add column id int unsigned auto_increment primary key first,
		add column addr varchar(255) after id
	`)
	require.Nil(t, err)
	//add column addr varchar(255) after id

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestAlterTableDropColumn(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_PRI,
		Charset:   "",
		Unsigned:  true,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_UNI,
		Charset:   "utf8",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_UNI,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment primary key,
		name varchar(255) CHARACTER SET utf8 not null default '' unique key,
		addr varchar(255),
		phone int not null unique,
		unique key (addr)
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)
	require.Equal(t, 4, len(tableDef.Indices))
	require.Equal(t, "addr", tableDef.Indices[3].Name)

	err = executor.Exec(`
	alter table test.test1 drop column addr
	`)
	require.Nil(t, err)
	tableDef, err = executor.GetTableDef("test", "test1")
	require.Equal(t, 3, len(tableDef.Indices))

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestAlterTableAddIndex(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_PRI,
		Charset:   "",
		Unsigned:  true,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_UNI,
		Charset:   "utf8",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_UNI,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment,
		name varchar(255) CHARACTER SET utf8 not null default '',
		phone int not null unique
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 add primary key (id)
	`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 add unique key (name)
	`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 add unique key (name)
	`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 add unique key name_custom(NaMe)
	`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)

	require.Equal(t, "PRIMARY", tableDef.Indices[0].Name)
	require.Equal(t, "phone", tableDef.Indices[1].Name)
	require.Equal(t, "name", tableDef.Indices[2].Name)
	require.Equal(t, "name_2", tableDef.Indices[3].Name)
	require.Equal(t, "name_custom", tableDef.Indices[4].Name)

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestAlterTableAddIndexLowerCase(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "ID",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_PRI,
		Charset:   "",
		Unsigned:  true,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_UNI,
		Charset:   "utf8",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		ID int unsigned ,
		name varchar(255) CHARACTER SET utf8 not null default '',
		phone int not null
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 add primary key (id)
	`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 add unique key (Id)
	`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 add unique key (nAMe)
	`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)

	require.Equal(t, "PRIMARY", tableDef.Indices[0].Name)
	require.Equal(t, "ID", tableDef.Indices[1].Name)
	require.Equal(t, "name", tableDef.Indices[2].Name)

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestAlterTableDropIndex(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_MUL,
		Charset:   "",
		Unsigned:  true,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_NONE,
		Charset:   "utf8",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment,
		name varchar(255) CHARACTER SET utf8 not null default '',
		phone int not null,
		key id_mul (id)
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 add unique key (id)
	`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 drop index id
	`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestAlterTableModifyColumn(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  true,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_UNI,
		Charset:   "",
		Unsigned:  false,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment,
		name varchar(255) CHARACTER SET utf8 not null default '',
		phone int not null,
		key name_mul (name)
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 modify name int unique key
	`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestAlterTableModifyColumnWithPos(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_UNI,
		Charset:   "",
		Unsigned:  false,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  true,
		Nullable:  true,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment,
		name varchar(255) CHARACTER SET utf8 not null default '',
		key name_mul (name)
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 modify name int unique key first
	`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestAlterTableChangeColumn(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  true,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "NAME",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_UNI,
		Charset:   "",
		Unsigned:  false,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment,
		name varchar(255) CHARACTER SET utf8 not null default '',
		phone int not null,
		key name_mul (name)
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 change name NAME int unique key
	`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)

	require.Equal(t, "NAME", tableDef.Indices[0].Columns[0])
	require.Equal(t, "NAME", tableDef.Indices[1].Columns[0])

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestAlterTableRenameTable(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test2",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_MUL,
		Charset:   "",
		Unsigned:  true,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_NONE,
		Charset:   "utf8",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment,
		name varchar(255) CHARACTER SET utf8 not null default '',
		phone int not null,
		key id_mul (id)
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	alter table test.test1 rename test2
	`)
	// It should failed, because executor doesn't know where is table test2
	require.NotNil(t, err)

	err = executor.Exec(`
	use test;
	alter table test.test1 rename test2
	`)
	// It should succeed
	require.Nil(t, err)

	// test.test1 dosen't exist now
	err = executor.Exec(`
	alter table test.test1 drop index id
	`)
	require.NotNil(t, err)

	tableDef, err := executor.GetTableDef("test", "test2")
	require.Nil(t, err)
	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

	// Rename again, to a new database
	err = executor.Exec(`
	create database db_test;
	alter table test.test2 rename db_test.test2
	`)
	require.Nil(t, err)

	// test.test2 dosen't exist now
	tableDef, err = executor.GetTableDef("test", "test2")
	require.NotNil(t, err)

	tableDef, err = executor.GetTableDef("db_test", "test2")
	require.Nil(t, err)
	tableDef.Indices = nil
	expectedDef.Database = "db_test"
	require.Equal(t, expectedDef, tableDef)

}

func TestCreateIndex(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_MUL,
		Charset:   "",
		Unsigned:  true,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_NONE,
		Charset:   "utf8",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment,
		name varchar(255) CHARACTER SET utf8 not null default '',
		phone int not null
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	create unique index idx1 on test.test1(id,name,phone);
	`)
	require.Nil(t, err)

	// This sql will trigger syntex error, because we don't support now
	err = executor.Exec(`
	create fulltext index idx2 on test.test1(name);
	`)
	require.NotNil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)
	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)
}

func TestRenameTable(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test2",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_PRI,
		Charset:   "",
		Unsigned:  true,
		Nullable:  false,
	})
	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment,
		primary key (id)
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	use test;
	rename table test1 to test.test2;`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	// It should be failed because test.test1 doesn't exist
	require.NotNil(t, err)

	tableDef, err = executor.GetTableDef("test", "test2")
	require.Nil(t, err)

	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}

func TestLowerCaseTableNames(t *testing.T) {
	var err error
	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database tesT;
	create table tesT.tesT1(
		id int unsigned auto_increment,
		primary key (id)
	);`)
	require.Nil(t, err)

	err = executor.Exec(`
	use test;
	rename table Test1 to Test.Test2;`)
	require.Nil(t, err)

	_, err = executor.GetTableDef("tEst", "tEst2")
	require.Nil(t, err)

}

func TestOriginalCaseTableNames(t *testing.T) {
	var err error
	executor := NewExecutor(&Config{
		LowerCaseTableNames: false,
	})
	err = executor.Exec(`
	create database tesT;
	create table tesT.tesT1(
		id int unsigned auto_increment,
		primary key (id)
	);`)
	require.Nil(t, err)

	err = executor.Exec(`
	use test;
	rename table Test1 to Test.Test2;`)
	require.NotNil(t, err)

}

func TestDropIndex(t *testing.T) {
	var err error
	expectedDef := &TableDef{
		Name:     "test1",
		Database: "test",
		Charset:  "gbk",
	}
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "id",
		Type:      "int(10) unsigned",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  true,
		Nullable:  true,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "name",
		Type:      "varchar(255)",
		InnerType: TypeVarchar,
		Key:       IndexType_NONE,
		Charset:   "utf8",
		Unsigned:  false,
		Nullable:  false,
	})
	expectedDef.Columns = append(expectedDef.Columns, &ColumnDef{
		Name:      "phone",
		Type:      "int(11)",
		InnerType: TypeLong,
		Key:       IndexType_NONE,
		Charset:   "",
		Unsigned:  false,
		Nullable:  false,
	})

	executor := NewExecutor(NewDefaultConfig())
	err = executor.Exec(`
	create database test;
	create table test.test1(
		id int unsigned auto_increment,
		name varchar(255) CHARACTER SET utf8 not null default '',
		phone int not null unique
	) CHARACTER SET gbk;`)
	require.Nil(t, err)

	err = executor.Exec(`
	drop index phone on test.test1;
	`)
	require.Nil(t, err)

	tableDef, err := executor.GetTableDef("test", "test1")
	require.Nil(t, err)
	tableDef.Indices = nil
	require.Equal(t, expectedDef, tableDef)

}
