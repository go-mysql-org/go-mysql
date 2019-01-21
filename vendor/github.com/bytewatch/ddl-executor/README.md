# About ddl-executor
The ddl-executor is a golang library that can parse and execute MySQL DDL statements. 
The library maintains schema structures in memory, for examples: creates a new schema structure when a CREATE statement executed, modifys a schema structure when a ALTER statement executed.

# What can it be used for ? 
This library may be used for DDL analysis, binlog stream's schema tracking(like binlog_row_metadata=FULL in MySQL 8) and so on. 


# Usage
Here is an example, execute  "CREATE TABLE test1" and "ALTER TABLE test1 ADD COLUMN" statement, and finally print the schema info of `test1`:
```
    executor := NewExecutor("utf8")
    err := executor.Exec(`
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
 
     for _, columnDef := range tableDef.Columns {
         fmt.Printf("%s.%s %s %s %s %s\n",
             tableDef.Name, columnDef.Name, columnDef.Type, columnDef.Key, columnDef.Charset, columnDef.Nullable)
     }
```

# Internals
This library use TiDB 's parser to parse MySQL statement to generate AST(abstract syntax tree). Base on different AST result of different DDL, ddl-executor executes particular logics (like MySQL's DDL logics) to maintain schema structures in memory. For different DDL statements: 
* CREATE DATABASE, DROP DATABASE
* CREATE SCHEMA, DROP SCHEMA
* CREATE INDEX, DROP INDEX
* CREATE TABLE, DROP TABLE
* ALTER TABLE
* RENAME TABLE 
* ALTER DATABASE


# What statements it supports ?
This library support 99% MySQL DDL statements.The ddl-executor  can execute statements same as MySQL 5.7 identically, such as complicated statement like this:
```
# -----------------------------------------------
# It should be impossible to rename index that doesn't exists,
# dropped or added within the same ALTER TABLE.
#
alter table t1 rename key d to e;
alter table t1 drop key c, rename key c to d;
alter table t1 add key d(j), rename key d to e;
 
# -----------------------------------------------
# It should be impossible to rename index to a name
# which is already used by another index, or is used
# by index which is added within the same ALTER TABLE.
#
alter table t1 add key d(j);
alter table t1 rename key c to d;
alter table t1 drop key d;
alter table t1 add key d(j), rename key c to d;
 
# -----------------------------------------------
#
# Rename key is handled before add key, so, it would be error because 'key f not exsits'
alter table t1 add key d(j), add unique key e(i), rename key c to d , rename key f to d;
 
# -----------------------------------------------
# It should be possible to rename index to a name which
# belongs to index which is dropped within the  same ALTER TABLE.
#
alter table t1 add key d(j);
alter table t1 drop key c, rename key d to c;
drop table t1;
```
> Those statements above come from MySQL' s test suit, and is part of our compatibility test cases.

# What statements it doesn't support ?
Some DDL statement that are unfrequent:
* ALTER with 'convert charset': ALTER TABLE t1 CONVERT TO CHARACTER SET latin1;
* ALTER with 'order by': ALTER TABLE  t1 add column new_col int, ORDER BY payoutid, bandid;
* DDL with geo types: ALTER TABLE t1 ADD b GEOMETRY,   ADD c POINT, ADD SPATIAL INDEX(b);
* CREATE TABLE with 'SELECT' clause;
* Some others unfrequent statement  we don't know now;

Those statements above will raise error when executing whit this library.  

# Compatibility tests
You can have a look on 'github.com/bytewatch/ddl-executor/compatibility_test', which is a cmd line tool to test compatibility between this library and MySQL.
Type command like this, will execute hundreds of DDL statements in  file `ddl_cases.sql` using this library and MySQL.
The command will print a diff between output of this library and MySQL's, tells what is not compatible.

```
go build
./test.sh ddl_cases.sql latin1 172.17.0.2 3306 root passwd123456
```
> Of course, replace MySQL connect info by yourself, and replace 'latin1' with your MySQL's charset_server.

# License
[Apache License 2.0](https://github.com/bytewatch/ddl-executor/blob/master/LICENSE)
