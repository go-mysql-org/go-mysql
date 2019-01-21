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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"sort"
	"strings"
)

type TableDef struct {
	Database string       `json:"database"`
	Name     string       `json:"name"`
	Columns  []*ColumnDef `json:"columns"`
	Indices  []*IndexDef  `json:"indices"`
	Charset  string       `json:"charset"`
}

func (o *TableDef) Clone() *TableDef {
	no := *o
	no.Columns = make([]*ColumnDef, len(o.Columns))
	no.Indices = make([]*IndexDef, len(o.Indices))
	for i := range no.Columns {
		no.Columns[i] = o.Columns[i].Clone()
	}
	for i := range no.Indices {
		no.Indices[i] = o.Indices[i].Clone()
	}

	return &no
}

func (o *TableDef) addForeignKey(constraint *ast.Constraint) error {
	indexName := constraint.Name
	columns := make([]string, len(constraint.Keys))
	for i, columnName := range constraint.Keys {
		columns[i] = columnName.Column.Name.L
	}
	return o.addImplicitIndex(indexName, columns, IndexType_MUL)
}

func (o *TableDef) findIndexByPrefix(columns []string) *IndexDef {
	for _, indexDef := range o.Indices {
		if indexDef.hasPrefix(columns) {
			return indexDef
		}
	}
	return nil
}

func (o *TableDef) newColumnDef(column *ast.ColumnDef, isExplicitPk bool) *ColumnDef {
	columnName := column.Name.Name.O
	columnType := column.Tp.InfoSchemaStr()
	columnInnerType := column.Tp.Tp
	unsigned := false
	nullable := true
	charset := ""

	if mysql.HasUnsignedFlag(column.Tp.Flag) {
		unsigned = true
	}

	if isStringType(column.Tp) {
		charset = column.Tp.Charset
		if charset == "" {
			// Use table or database charset
			charset = o.Charset
		}
	}

	explicitNull := false
	for _, option := range column.Options {
		if option.Tp == ast.ColumnOptionNotNull {
			nullable = false
		} else if option.Tp == ast.ColumnOptionNull {
			nullable = true
			explicitNull = true
		}
	}
	if isExplicitPk {
		if explicitNull {
			// If this column is explicit pk, it can not be null
			return nil
		}
		// We know this column is one of PRI already, this column must be not nullable
		nullable = false
	}

	columnDef := ColumnDef{
		Name:      columnName,
		Type:      columnType,
		InnerType: columnInnerType,
		Key:       IndexType_NONE,
		Charset:   charset,
		Unsigned:  unsigned,
		Nullable:  nullable,
	}

	return &columnDef

}

func (o *TableDef) adjustColumnPos(oldPos, newPos int) {
	if oldPos == newPos {
		return
	}

	columnDef := o.Columns[oldPos]

	if newPos < oldPos {
		copy(o.Columns[newPos+1:], o.Columns[newPos:oldPos])
	} else {
		// Because oldPos is smaller than newPos, and the elements behind oldPos can move forward,
		// so newPos -= 1
		newPos = newPos - 1
		copy(o.Columns[oldPos:], o.Columns[oldPos+1:newPos+1])
	}

	o.Columns[newPos] = columnDef
}

func (o *TableDef) addColumn(column *ast.ColumnDef, position *ast.ColumnPosition) error {
	// TODO: check point type
	columnName := column.Name.Name.O

	if o.findColumn(columnName) != nil {
		return ErrDupFieldName.Gen(columnName)
	}

	isExplicitPk := false
	for _, option := range column.Options {
		if option.Tp == ast.ColumnOptionPrimaryKey {
			isExplicitPk = true
		}
	}

	columnDef := o.newColumnDef(column, isExplicitPk)
	oldPos := len(o.Columns)
	o.Columns = append(o.Columns, columnDef)

	// Adjust the position
	newPos := oldPos
	if position != nil {
		if position.Tp == ast.ColumnPositionFirst {
			newPos = 0
		} else if position.Tp == ast.ColumnPositionAfter {
			relativeColumnName := position.RelativeColumn.Name.O
			i, relativeColumnDef := o.findColumnInteranl(relativeColumnName)
			if relativeColumnDef == nil {
				return ErrErrBadField.Gen(relativeColumnName, o.Name)
			}
			newPos = i + 1
		}
	}
	o.adjustColumnPos(oldPos, newPos)

	// Parse the column options, we just care PrimaryKey and UniqKey
	for _, option := range column.Options {
		if option.Tp == ast.ColumnOptionPrimaryKey {
			o.addExplicitIndex("", []string{columnName}, IndexType_PRI)
		} else if option.Tp == ast.ColumnOptionUniqKey {
			o.addExplicitIndex("", []string{columnName}, IndexType_UNI)
		}
	}
	return nil
}

// Check if a column is one of PRI’s columns
func (o *TableDef) isExplicitPk(columnName string) bool {
	if len(o.Indices) == 0 {
		return false
	}
	if o.Indices[0].Key != IndexType_PRI {
		return false
	}
	for _, explicitPk := range o.Indices[0].Columns {
		if strings.ToLower(columnName) == strings.ToLower(explicitPk) {
			return true
		}
	}
	return false
}

func (o *TableDef) changeColumn(originalName string, column *ast.ColumnDef, position *ast.ColumnPosition) error {
	newName := column.Name.Name.O

	oldPos, columnDef := o.findColumnInteranl(originalName)
	if columnDef == nil {
		return ErrErrBadField.Gen(originalName, o.Name)
	}
	if strings.ToLower(newName) != strings.ToLower(originalName) {
		if o.findColumn(newName) != nil {
			return ErrDupFieldName.Gen(newName)
		}
	}

	isExplicitPk := false
	if o.isExplicitPk(originalName) {
		isExplicitPk = true
	}
	for _, option := range column.Options {
		if option.Tp == ast.ColumnOptionPrimaryKey {
			isExplicitPk = true
		}
	}

	newColumnDef := o.newColumnDef(column, isExplicitPk)
	// Replace column at i
	o.Columns[oldPos] = newColumnDef

	// Adjust the position
	newPos := oldPos
	if position.Tp == ast.ColumnPositionFirst {
		newPos = 0
	} else if position.Tp == ast.ColumnPositionAfter {
		relativeColumnName := position.RelativeColumn.Name.O
		i, relativeColumnDef := o.findColumnInteranl(relativeColumnName)
		if relativeColumnDef == nil {
			return ErrErrBadField.Gen(relativeColumnName, o.Name)
		}
		newPos = i + 1
	}
	o.adjustColumnPos(oldPos, newPos)

	// Modify the column name in index
	for _, index := range o.Indices {
		index.changeColumn(originalName, newName)
	}

	for _, option := range column.Options {
		if option.Tp == ast.ColumnOptionPrimaryKey {
			o.addExplicitIndex("", []string{newName}, IndexType_PRI)
		} else if option.Tp == ast.ColumnOptionUniqKey {
			o.addExplicitIndex("", []string{newName}, IndexType_UNI)
		}
	}

	// May this stmt change the implict PRI,
	// and we need sortIndices() to update this column's key and nullable field

	return nil

}

// If a column are dropped from a table, the column is also
// removed from any index of which it is a part of.
// If all columns that make up an index are dropped,
// the index is mark as dropped as well.
// User can add an index with the same name with the marked index.
// But, if user add an column with the same name later, the index will unmarked.
// The cleanIndices() will clean these indices that are marked dropped.
func (o *TableDef) dropColumn(columnName string) error {
	i, column := o.findColumnInteranl(columnName)
	if column == nil {
		return ErrCantDropFieldOrKey.Gen(columnName)
	}

	// Delete column at i
	columnN := len(o.Columns)
	copy(o.Columns[i:columnN-1], o.Columns[i+1:])
	o.Columns = o.Columns[:columnN-1]

	return nil
}

// We need to clean indices. Because :
//	1. Some indices' columns may all be dropped
//	2. Some implict indice may be covered by other explicit index
func (o *TableDef) cleanIndices() {
	for _, index := range o.Indices {
		var exists []int
		for i, column := range index.Columns {
			if o.findColumn(column) != nil {
				exists = append(exists, i)
			}
		}
		newColumns := make([]string, len(exists))
		for i, existsI := range exists {
			newColumns[i] = index.Columns[existsI]
		}
		index.Columns = newColumns
	}

	var reserved []*IndexDef

	// Check which empty index can be dropped
	for _, index := range o.Indices {
		if len(index.Columns) == 0 {
			// This index need to drop
			continue
		}
		if index.Name == ignoredIndex {
			continue
		}
		// This index need to reserved
		reserved = append(reserved, index)
	}

	o.Indices = reserved

}

func (o *TableDef) findColumnInteranl(columnName string) (int, *ColumnDef) {
	columnName = strings.ToLower(columnName)
	for i, column := range o.Columns {
		if strings.ToLower(column.Name) == columnName {
			return i, column
		}
	}
	return 0, nil
}

func (o *TableDef) findColumn(columnName string) (column *ColumnDef) {
	_, column = o.findColumnInteranl(columnName)
	return
}

func (o *TableDef) getAnonymousIndex(columnName string) string {
	// columnName should in lower case
	id := 2
	l := len(o.Indices)
	indexName := columnName
	for i := 0; i < l; i++ {
		if strings.ToLower(o.Indices[i].Name) == strings.ToLower(indexName) {
			indexName = fmt.Sprintf("%s_%d", columnName, id)
			i = -1
			id++
		}
	}
	return indexName
}

// Refer: https://dev.mysql.com/doc/refman/8.0/en/show-columns.html
func (o *TableDef) setIndexColumnKey(indexDef *IndexDef, implictPk string) {

	if indexDef.Key == IndexType_PRI {
		// Every one column need to set PRI
		for _, columnName := range indexDef.Columns {
			o.setColumnKey(columnName, IndexType_PRI)
		}
	} else {
		// Only the first column need to set UNI or MUL
		// Sometimes the UNI may be promote to PRI, if there is not PRI and the UNI is not null
		columnName := indexDef.Columns[0]
		if indexDef.Key == IndexType_UNI && len(indexDef.Columns) == 1 {
			if implictPk == columnName {
				o.setColumnKey(columnName, IndexType_PRI)
			} else {
				o.setColumnKey(columnName, IndexType_UNI)
			}
		} else {
			o.setColumnKey(columnName, IndexType_MUL)
		}
	}
}

func (o *TableDef) addExplicitIndex(indexName string, columns []string, key IndexType) error {
	return o.addIndex(indexName, columns, key, 0)
}

// Refer MySQL source: mysql_prepare_create_table() and foreign_key_prefix()
func (o *TableDef) addImplicitIndex(indexName string, columns []string, key IndexType) error {
	// If there is not already an explicitly defined index that can support the foreign key,
	// MySQL implicitly generates a foreign key index that is named according to some rules
	// Refer: https://dev.mysql.com/doc/refman/5.6/en/create-table-foreign-keys.html
	indexDef := o.findIndexByPrefix(columns)
	if indexDef != nil {
		if indexDef.Flag&IndexFlag_Generated == 0 {
			// There is an index can satisfy this fk, and is not generated
			return nil
		}
	}
	return o.addIndex(indexName, columns, key, IndexFlag_Generated)
}

func (o *TableDef) addIndex(indexName string, columns []string, key IndexType, Flag uint) error {
	if key == IndexType_PRI {
		// The primary key's name is PRIMARY
		indexName = "PRIMARY"
	} else if indexName == "" {
		columnDef := o.findColumn(columns[0])
		if columnDef == nil {
			return ErrErrBadField.Gen(columns[0], o.Name)
		}
		indexName = o.getAnonymousIndex(columnDef.Name)
	}

	// check if already exists
	if o.findIndex(indexName) != nil {
		if indexName == "PRIMARY" {
			// Already has a primary key
			return ErrMultiplePriKey
		}
		return ErrDupKeyName.Gen(indexName)
	}

	indexDef := IndexDef{
		Name: indexName,
		Key:  key,
		Flag: Flag,
	}

	// TODO: check columns if duplicated

	// check if all column exists
	for _, columnName := range columns {
		columnDef := o.findColumn(columnName)
		if columnDef == nil {
			return ErrErrBadField.Gen(columnName, o.Name)
		}
		if key == IndexType_PRI {
			// the column of PRI must be not nullable
			columnDef.Nullable = false
		}
		indexDef.Columns = append(indexDef.Columns, columnDef.Name)
	}

	// check if some implicit generated index can ignored(dropped)
	for _, tmp := range o.Indices {
		if tmp.Flag&IndexFlag_Generated == 0 {
			continue
		}
		if indexDef.hasPrefix(tmp.Columns) {
			tmp.Name = ignoredIndex
		}
	}

	o.Indices = append(o.Indices, &indexDef)

	// Need to sort indices

	return nil
}

// Sort the indices, make explicit PRI the first index,
// and make explicit or implict PRI column's UNI indices in the front of others.
// According to the following properties, in decreasing order of importance:
// - PRIMARY KEY
// - UNIQUE with all columns NOT NULL
// - UNIQUE without partial segments
// - UNIQUE
// - without fulltext columns
// - without virtual generated column
// Refer MySQL source: sort_keys()
func (o *TableDef) sortIndices() {
	if len(o.Indices) == 0 {
		return
	}

	for _, indexDef := range o.Indices {
		indexDef.Flag &= ^IndexFlag_HasNullPart
	}

	for _, indexDef := range o.Indices {
		for _, columnName := range indexDef.Columns {
			columnDef := o.findColumn(columnName)
			if columnDef.Nullable {
				indexDef.Flag |= IndexFlag_HasNullPart
				break
			}
		}
	}

	sort.Stable(Indices(o.Indices))

	var implictPk string
	if o.Indices[0].Key != IndexType_PRI {
		for _, indexDef := range o.Indices {
			if indexDef.Key != IndexType_UNI {
				continue
			}
			if indexDef.Flag&IndexFlag_HasNullPart != 0 {
				continue
			}
			if len(indexDef.Columns) != 1 {
				continue
			}
			implictPk = indexDef.Columns[0]
			break
		}
	}

	for i := len(o.Indices) - 1; i >= 0; i-- {
		o.setIndexColumnKey(o.Indices[i], implictPk)
	}

}

func (o *TableDef) setColumnKey(columnName string, key IndexType) error {
	columnDef := o.findColumn(columnName)
	if columnDef == nil {
		return ErrErrBadField.Gen(columnName, o.Name)
	}
	columnDef.setKey(key)

	return nil
}

func (o *TableDef) dropIndex(indexName string) error {
	i, indexDef := o.findIndexInternal(indexName)
	if indexDef == nil {
		// Index not found
		return ErrCantDropFieldOrKey.Gen(indexName)
	}

	// Every one column need to set NONE
	for _, columnName := range indexDef.Columns {
		o.setColumnKey(columnName, IndexType_NONE)
	}

	// Delete index at i
	indexN := len(o.Indices)
	copy(o.Indices[i:indexN-1], o.Indices[i+1:])
	o.Indices = o.Indices[:indexN-1]

	// Other index may covers these columns,
	// need to adjust these columns

	return nil
}

// Find index by name.
// The invalid indices are filtered if ignoreInvalid is true.
func (o *TableDef) findIndexInternal(indexName string) (int, *IndexDef) {
	indexName = strings.ToLower(indexName)
	for i, indexDef := range o.Indices {
		if strings.ToLower(indexDef.Name) == indexName {
			return i, indexDef
		}
	}
	return 0, nil
}

func (o *TableDef) findIndex(indexName string) (index *IndexDef) {
	_, index = o.findIndexInternal(indexName)
	return
}

func (o *TableDef) renameIndex(originalName, newName string) error {
	index := o.findIndex(originalName)
	if index == nil {
		return ErrKeyDoesNotExist.Gen(originalName, o.Name)
	}
	if originalName == newName {
		return nil
	}
	if o.findIndex(newName) != nil && strings.ToLower(originalName) != strings.ToLower(newName) {
		return ErrDupKeyName.Gen(newName)
	}
	index.Name = newName
	return nil
}

// This function tells whether a MySQL type is a string type
func isStringType(ft *types.FieldType) bool {
	// These types may be string, but also may not: TypeTinyBlob TypeMediumBlob TypeLongBlob TypeBlob
	// We need to check the charset, if charset is binary, these types is not string
	if ft.EvalType() != types.ETString {
		return false
	}
	if ft.Charset == "binary" {
		return false
	}
	return true
}
