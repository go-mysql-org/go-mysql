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

type DatabaseDef struct {
	Name    string               `json:"name"`
	Tables  map[string]*TableDef `json:tables`
	Charset string               `json:"charset"`
}

func (o *DatabaseDef) findTable(tableName string) *TableDef {
	if table, ok := o.Tables[tableName]; ok {
		return table
	}
	return nil
}

func (o *DatabaseDef) cloneTable(tableName string) *TableDef {
	if table, ok := o.Tables[tableName]; ok {
		return table.Clone()
	}
	return nil
}

func (o *DatabaseDef) setTable(tableName string, tableDef *TableDef) {
	tableDef.Database = o.Name
	o.Tables[tableName] = tableDef
}

func (o *DatabaseDef) dropTable(tableName string) {
	delete(o.Tables, tableName)
}

func (o *DatabaseDef) getTables() []string {
	tables := make([]string, 0)
	for tableName := range o.Tables {
		tables = append(tables, tableName)
	}
	return tables
}
