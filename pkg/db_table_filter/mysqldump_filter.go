/*
 * TencentBlueKing is pleased to support the open source community by making 蓝鲸智云-DB管理系统(BlueKing-BK-DBM) available.
 * Copyright (C) 2017-2023 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at https://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package db_table_filter

import (
	"fmt"

	"github.com/dlclark/regexp2"
	"github.com/jmoiron/sqlx"
)

// GetTablesWithoutDbName 过滤后的表没有dbname
func (c *DbTableFilter) GetTablesWithoutDbName(ip string, port int, user string, password string) ([]string, error) {
	return c.getTablesWithoutDbName(
		ip,
		port,
		user,
		password,
		c.TableFilterRegex(),
	)
}

// TableItem table item
type TableItem struct {
	FullName  string `db:"full_name"`
	TableName string `db:"table_name"`
}

func (c *DbTableFilter) getTablesWithoutDbName(ip string, port int, user string, password string, reg string) (
	[]string,
	error,
) {
	dbh, err := sqlx.Connect(
		"mysql",
		fmt.Sprintf(`%s:%s@tcp(%s:%d)/`, user, password, ip, port),
	)
	if err != nil {
		return nil, err
	}

	defer func() {
		if dbh != nil {
			dbh.Close()
		}
	}()
	var tables []TableItem
	err = dbh.Select(&tables,
		`SELECT CONCAT(table_schema, ".", table_name) AS full_name,table_name as table_name
		from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE="BASE TABLE"`,
	)
	if err != nil {
		return nil, err
	}
	pattern, err := regexp2.Compile(reg, regexp2.None)
	if err != nil {
		return nil, err
	}

	var selectedTables []string
	for _, t := range tables {
		ok, err := pattern.MatchString(t.FullName)
		if err != nil {
			return nil, err
		}
		if ok {
			selectedTables = append(selectedTables, t.TableName)
		}
	}

	return selectedTables, nil
}
