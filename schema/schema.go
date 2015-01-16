// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schema

import (
	"fmt"
	"github.com/siddontang/go-mysql/client"
	"strings"
)

const (
	TYPE_NUMBER = iota + 1 //tinyint, smallint, mediumint, int, bigint, year
	TYPE_FLOAT             //float, double
	TYPE_STRING            //other
)

type TableColumn struct {
	Name   string
	Type   int
	IsAuto bool
}

type Index struct {
	Name        string
	Columns     []string
	Cardinality []uint64
}

type Table struct {
	Schema string
	Name   string

	Columns   []TableColumn
	Indexes   []*Index
	PKColumns []int
}

func (ta *Table) AddColumn(name string, columnType string, extra string) {
	index := len(ta.Columns)
	ta.Columns = append(ta.Columns, TableColumn{Name: name})

	if strings.Contains(columnType, "int") || strings.HasPrefix(columnType, "year") {
		ta.Columns[index].Type = TYPE_NUMBER
	} else if columnType == "float" || columnType == "double" {
		ta.Columns[index].Type = TYPE_FLOAT
	} else {
		ta.Columns[index].Type = TYPE_STRING
	}

	if extra == "auto_increment" {
		ta.Columns[index].IsAuto = true
	}
}

func (ta *Table) FindColumn(name string) int {
	for i, col := range ta.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

func (ta *Table) GetPKColumn(index int) *TableColumn {
	return &ta.Columns[ta.PKColumns[index]]
}

func (ta *Table) AddIndex(name string) (index *Index) {
	index = NewIndex(name)
	ta.Indexes = append(ta.Indexes, index)
	return index
}

func NewIndex(name string) *Index {
	return &Index{name, make([]string, 0, 8), make([]uint64, 0, 8)}
}

func (idx *Index) AddColumn(name string, cardinality uint64) {
	idx.Columns = append(idx.Columns, name)
	if cardinality == 0 {
		cardinality = uint64(len(idx.Cardinality) + 1)
	}
	idx.Cardinality = append(idx.Cardinality, cardinality)
}

func (idx *Index) FindColumn(name string) int {
	for i, colName := range idx.Columns {
		if name == colName {
			return i
		}
	}
	return -1
}

func NewTable(conn *client.Conn, schema string, name string) (*Table, error) {
	ta := &Table{
		Schema:  schema,
		Name:    name,
		Columns: make([]TableColumn, 0, 16),
		Indexes: make([]*Index, 0, 8),
	}

	if err := ta.fetchColumns(conn); err != nil {
		return nil, err
	}

	if err := ta.fetchIndexes(conn); err != nil {
		return nil, err
	}

	return ta, nil
}

func (ta *Table) fetchColumns(conn *client.Conn) error {
	r, err := conn.Execute(fmt.Sprintf("describe %s.%s", ta.Schema, ta.Name))
	if err != nil {
		return err
	}

	for i := 0; i < r.RowNumber(); i++ {
		name, _ := r.GetString(i, 0)
		colType, _ := r.GetString(i, 1)
		extra, _ := r.GetString(i, 5)

		ta.AddColumn(name, colType, extra)
	}

	return nil
}

func (ta *Table) fetchIndexes(conn *client.Conn) error {
	r, err := conn.Execute(fmt.Sprintf("show index from %s.%s", ta.Schema, ta.Name))
	if err != nil {
		return err
	}
	var currentIndex *Index
	currentName := ""

	for i := 0; i < r.RowNumber(); i++ {
		indexName, _ := r.GetString(i, 2)
		if currentName != indexName {
			currentIndex = ta.AddIndex(indexName)
			currentName = indexName
		}
		cardinality, _ := r.GetUint(i, 6)
		colName, _ := r.GetString(i, 4)
		currentIndex.AddColumn(colName, cardinality)
	}

	if len(ta.Indexes) == 0 {
		return nil
	}

	pkIndex := ta.Indexes[0]
	if pkIndex.Name != "PRIMARY" {
		return nil
	}

	ta.PKColumns = make([]int, len(pkIndex.Columns))
	for i, pkCol := range pkIndex.Columns {
		ta.PKColumns[i] = ta.FindColumn(pkCol)
	}

	return nil
}
