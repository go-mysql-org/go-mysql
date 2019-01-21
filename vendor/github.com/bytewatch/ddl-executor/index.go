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
	"strings"
)

type IndexType string

const (
	IndexType_NONE IndexType = ""
	IndexType_PRI            = "PRI"
	IndexType_UNI            = "UNI"
	IndexType_MUL            = "MUL"
)

const (
	// Mark whether an index is generated implicitly
	IndexFlag_Generated uint = 1 << iota
	IndexFlag_FullText
	IndexFlag_HasNullPart
)

const (
	ignoredIndex string = ""
	primaryIndex string = "PRIMARY"
)

type IndexDef struct {
	// original case
	Name    string    `json:"name"`
	Columns []string  `json:"columns"`
	Key     IndexType `json:"key"`
	Flag    uint      `json:"flag"`
}

func (o *IndexDef) dropColumn(columnName string) {
	columnName = strings.ToLower(columnName)
	columnN := len(o.Columns)
	i := 0
	for ; i < columnN; i++ {
		if strings.ToLower(o.Columns[i]) == columnName {
			// Delete column at i
			copy(o.Columns[i:columnN-1], o.Columns[i+1:])
			break
		}
	}
	// Column not found
	if i == columnN {
		return
	}
	o.Columns = o.Columns[:columnN-1]
}

func (o *IndexDef) changeColumn(originalName, newName string) {
	originalName = strings.ToLower(originalName)
	for i := range o.Columns {
		if strings.ToLower(o.Columns[i]) == originalName {
			o.Columns[i] = newName
		}
	}
}

func (o *IndexDef) hasPrefix(columns []string) bool {
	if len(o.Columns) < len(columns) {
		return false
	}

	for i := range o.Columns {
		if o.Columns[i] != columns[i] {
			return false
		}
	}
	return true
}

func (o *IndexDef) Clone() *IndexDef {
	no := *o
	no.Columns = make([]string, len(o.Columns))
	copy(no.Columns, o.Columns)
	return &no
}

type Indices []*IndexDef

func (o Indices) Len() int {
	return len(o)
}

func (o Indices) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

// return true if o[i] is before o[j]
func (o Indices) Less(i, j int) bool {

	if o[i].Key == IndexType_PRI {
		return true
	}

	if o[j].Key == IndexType_PRI {
		return false
	}

	if o[i].Key == IndexType_UNI {
		iFlag := o[i].Flag
		jFlag := o[j].Flag
		if o[j].Key != IndexType_UNI {
			return true
		}
		if (iFlag^jFlag)&IndexFlag_HasNullPart != 0 {
			if iFlag&IndexFlag_HasNullPart == 0 {
				return true
			} else {
				// prefer j
				return false
			}
		}
	} else if o[j].Key == IndexType_UNI {
		// prefer j
		return false
	}

	return len(o[i].Columns) < len(o[j].Columns)
}
