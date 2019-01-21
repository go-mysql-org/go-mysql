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
	"fmt"
	"github.com/pingcap/parser/mysql"
)

var (
	ErrParse = NewError(mysql.ErrParse, "")
	// ErrNoDB return for no database selected
	ErrNoDB = NewError(mysql.ErrNoDB, "No database selected")
	// ErrDBDropExists returns for dropping a non-existent database.
	ErrDBDropExists = NewError(mysql.ErrDBDropExists, "Can't drop database '%s'; database doesn't exist")
	// ErrBadDB returns for database not exists.
	ErrBadDB = NewError(mysql.ErrBadDB, "Unknown database '%s'")
	// ErrNoSuchTable returns for table not exists.
	ErrNoSuchTable = NewError(mysql.ErrNoSuchTable, "Table '%s.%s' doesn't exist")
	// ErrErrBadField returns for column not exists.
	ErrErrBadField = NewError(mysql.ErrBadField, "Unknown column '%s' in '%s'")
	// ErrErrWrongFkDef returns for foreign key not match.
	ErrErrWrongFkDef = NewError(mysql.ErrWrongFkDef, "Incorrect foreign key definition for '%s': Key reference and table reference don't match")
	// ErrCannotAddForeign returns for foreign key exists.
	ErrCannotAddForeign = NewError(mysql.ErrCannotAddForeign, "Cannot add foreign key constraint")
	// ErrCantDropFieldOrKey returns for foreign key not exists.
	ErrCantDropFieldOrKey = NewError(mysql.ErrCantDropFieldOrKey, "Can't DROP '%s'; check that column/key exists")
	// ErrErrDBCreateExists returns for database already exists.
	ErrErrDBCreateExists = NewError(mysql.ErrDBCreateExists, "Can't create database '%s'; database exists")
	// ErrTableExists returns for table already exists.
	ErrTableExists = NewError(mysql.ErrTableExists, "Table '%s' already exists")
	// ErrBadTable returns for dropping a non-existent table.
	ErrBadTable = NewError(mysql.ErrBadTable, "Unknown table '%s.%s'")
	// ErrDupFieldName returns for column already exists.
	ErrDupFieldName = NewError(mysql.ErrDupFieldName, "Duplicate column name '%s'")
	// ErrDupIndex returns for index already exists.
	ErrDupIndex = NewError(mysql.ErrDupIndex, "Duplicate Index")
	// ErrDupKeyName returns for index duplicate when rename index.
	ErrDupKeyName = NewError(mysql.ErrDupKeyName, "Duplicate key name '%s'")
	// ErrKeyDoesNotExist returns for index not exists.
	ErrKeyDoesNotExist = NewError(mysql.ErrKeyDoesNotExist, "Key '%s' doesn't exist in table '%s'")
	// ErrMultiplePriKey returns for multiple primary keys.
	ErrMultiplePriKey = NewError(mysql.ErrMultiplePriKey, "Multiple primary key defined")
	// ErrTooManyKeyParts returns for too many key parts.
	ErrTooManyKeyParts = NewError(mysql.ErrTooManyKeyParts, "Too many key parts specified; max %d parts allowed")
)

type Error struct {
	code    int
	message string
	args    []interface{}
}

func NewError(code int, message string) *Error {
	return &Error{
		code:    code,
		message: message,
	}
}

func (o *Error) Code() int {
	return o.code
}

func (o *Error) Error() string {
	return fmt.Sprintf("Error %d: %s", o.code, o.getMsg())
}

func (o *Error) Gen(args ...interface{}) *Error {
	err := *o
	err.args = args
	return &err
}

func (o *Error) getMsg() string {
	if len(o.args) > 0 {
		return fmt.Sprintf(o.message, o.args...)
	}
	return o.message
}
