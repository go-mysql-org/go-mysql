/*
 * TencentBlueKing is pleased to support the open source community by making 蓝鲸智云-DB管理系统(BlueKing-BK-DBM) available.
 * Copyright (C) 2017-2023 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at https://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

// Package db_table_filter 库表过滤
package db_table_filter

import (
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql 驱动
)

// DbTableFilter 库表过滤
/*
输入分为 库过滤 和 表过滤 两组
过滤方式是
1. 用 库过滤 筛选库
2. 用 表过滤 筛选上一步得到的库下所有表

example 1:
include db = db%
exclude db = db1
include table = tb%
exclude table = tb1%
语义是:
所有以 db 开头的库, 排除掉 db1; 这些库中所有以 tb 开头的表, 排除掉 tb1 开头的表

需要注意的是, 这里 db1 库被整体排除掉了
*/
type DbTableFilter struct {
	IncludeDbPatterns       []string
	ExcludeDbPatterns       []string
	IncludeTablePatterns    []string
	ExcludeTablePatterns    []string
	dbFilterIncludeRegex    string
	dbFilterExcludeRegex    string
	tableFilterIncludeRegex string
	tableFilterExcludeRegex string

	Compiled *DbTableFilterCompiled
}

// NewFilter 构造函数
// NewFilter 完成后，需要 BuildFilter()
func NewFilter(
	includeDbPatterns []string, includeTablePatterns []string,
	excludeDbPatterns []string, excludeTablePatterns []string) (*DbTableFilter, error) {

	trimEle := func(s []string) []string {
		var r []string
		for _, e := range s {
			te := strings.TrimSpace(e)
			if len(te) > 0 {
				r = append(r, strings.TrimSpace(e))
			}
		}
		return r
	}

	tf := &DbTableFilter{
		IncludeDbPatterns:       trimEle(includeDbPatterns),
		IncludeTablePatterns:    trimEle(includeTablePatterns),
		ExcludeDbPatterns:       trimEle(excludeDbPatterns),
		ExcludeTablePatterns:    trimEle(excludeTablePatterns),
		dbFilterIncludeRegex:    "",
		dbFilterExcludeRegex:    "",
		tableFilterIncludeRegex: "",
		tableFilterExcludeRegex: "",
	}

	err := tf.validate()
	if err != nil {
		return nil, err
	}

	tf.buildFilter()

	return tf, nil
}

func (c *DbTableFilter) buildFilter() {
	c.dbIncludeRegex()
	c.dbExcludeRegex()
	c.tableIncludeRegex()
	c.tableExcludeRegex()
}
