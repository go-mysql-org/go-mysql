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

type ColumnDef struct {
	// original case
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	InnerType byte      `json:"inner_type"`
	Key       IndexType `json:"key"`
	Charset   string    `json:"charset"`
	Unsigned  bool      `json:"unsigned"`
	Nullable  bool      `json:"nullable"`
}

func (o *ColumnDef) Clone() *ColumnDef {
	no := *o
	return &no
}

func (o *ColumnDef) setKey(key IndexType) {
	o.Key = key
}
