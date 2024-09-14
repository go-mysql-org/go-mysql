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
	"fmt"

	"github.com/dlclark/regexp2"
	_ "github.com/go-sql-driver/mysql" // mysql 驱动
	"github.com/pkg/errors"
)

var impossibleTableName = "@@<<~%~empty db~%~>>@@"

// DbTableFilter 库表过滤
type DbTableFilter struct {
	IncludeDbPatterns       []string
	IncludeTablePatterns    []string
	ExcludeDbPatterns       []string
	ExcludeTablePatterns    []string
	AdditionExcludePatterns []string // 隐式需要强制排除的库, 比如系统库之类的. 这些库的 ExcludeTable 强制为 *
	dbFilterIncludeRegex    string
	dbFilterExcludeRegex    string
	tableFilterIncludeRegex string
	tableFilterExcludeRegex string

	Compiled *DbTableFilterCompiled

	//dbFilterCompiled    *regexp2.Regexp
	//tableFilterCompiled *regexp2.Regexp
}

type DbTableFilterCompiled struct {
	DbFilter *regexp2.Regexp
	TbFilter *regexp2.Regexp
}

// NewDbTableFilter 构造函数
// NewDbTableFilter 完成后，需要 BuildFilter()
func NewDbTableFilter(includeDbPatterns []string, includeTablePatterns []string,
	excludeDbPatterns []string, excludeTablePatterns []string) (*DbTableFilter, error) {

	tf := &DbTableFilter{
		IncludeDbPatterns:       cleanIt(includeDbPatterns),
		IncludeTablePatterns:    cleanIt(includeTablePatterns),
		ExcludeDbPatterns:       cleanIt(excludeDbPatterns),
		ExcludeTablePatterns:    cleanIt(excludeTablePatterns),
		AdditionExcludePatterns: []string{},
		dbFilterIncludeRegex:    "",
		dbFilterExcludeRegex:    "",
		tableFilterIncludeRegex: "",
		tableFilterExcludeRegex: "",
	}

	err := tf.validate()
	if err != nil {
		return nil, err
	}

	return tf, nil
}

// BuildFilter normal build filter
// is different with NewMydumperRegex
func (c *DbTableFilter) BuildFilter() {
	c.buildDbFilterRegex()
	c.buildTableFilterRegex()
}

func (c *DbTableFilter) validate() error {
	if len(c.IncludeDbPatterns) == 0 || len(c.IncludeTablePatterns) == 0 {
		return fmt.Errorf("include patterns can't be empty")
	}
	if !((len(c.ExcludeDbPatterns) > 0 && len(c.ExcludeTablePatterns) > 0) ||
		(len(c.ExcludeDbPatterns) == 0 && len(c.ExcludeTablePatterns) == 0)) {
		return fmt.Errorf("exclude patterns can't be partial empty")
	}

	if err := globCheck(c.IncludeDbPatterns); err != nil {
		return err
	}
	if err := globCheck(c.IncludeTablePatterns); err != nil {
		return err
	}
	if err := globCheck(c.ExcludeDbPatterns); err != nil {
		return err
	}
	if err := globCheck(c.ExcludeTablePatterns); err != nil {
		return err
	}
	return nil
}

func (c *DbTableFilter) buildDbFilterRegex() {
	var includeParts []string
	for _, db := range c.IncludeDbPatterns {
		includeParts = append(includeParts, fmt.Sprintf(`%s$`, ReplaceGlob(db)))
	}

	var excludeParts []string
	for _, db := range c.ExcludeDbPatterns {
		excludeParts = append(excludeParts, fmt.Sprintf(`%s$`, ReplaceGlob(db)))
	}

	for _, db := range c.AdditionExcludePatterns {
		excludeParts = append(excludeParts, fmt.Sprintf(`%s$`, ReplaceGlob(db)))
	}

	c.dbFilterIncludeRegex = buildIncludeRegexp(includeParts)
	c.dbFilterExcludeRegex = buildExcludeRegexp(excludeParts)
}

func (c *DbTableFilter) buildTableFilterRegex() {
	var includeParts []string
	for _, db := range c.IncludeDbPatterns {
		for _, table := range c.IncludeTablePatterns {
			includeParts = append(
				includeParts,
				fmt.Sprintf(`%s\.%s$`, ReplaceGlob(db), ReplaceGlob(table)),
			)
		}
	}

	var excludeParts []string
	for _, db := range c.ExcludeDbPatterns {
		for _, table := range c.ExcludeTablePatterns {
			excludeParts = append(
				excludeParts,
				fmt.Sprintf(`%s\.%s$`, ReplaceGlob(db), ReplaceGlob(table)),
			)
		}
	}

	for _, db := range c.AdditionExcludePatterns {
		excludeParts = append(
			excludeParts,
			fmt.Sprintf(`%s\.%s$`, ReplaceGlob(db), ReplaceGlob("*")),
		)
	}

	c.tableFilterIncludeRegex = buildIncludeRegexp(includeParts)
	c.tableFilterExcludeRegex = buildExcludeRegexp(excludeParts)
}

// TableFilterRegex 返回表过滤正则
func (c *DbTableFilter) TableFilterRegex() string {
	return fmt.Sprintf(`^%s%s`, c.tableFilterIncludeRegex, c.tableFilterExcludeRegex)
}

// DbFilterRegex 返回库过滤正则
func (c *DbTableFilter) DbFilterRegex() string {
	return fmt.Sprintf(`^%s%s`, c.dbFilterIncludeRegex, c.dbFilterExcludeRegex)
}

// DbTableFilterCompile compile regex filter
// will run BuildFilter inside
func (c *DbTableFilter) DbTableFilterCompile() error {
	c.BuildFilter()
	var err error

	c.Compiled = &DbTableFilterCompiled{}
	c.Compiled.DbFilter, err = regexp2.Compile(c.DbFilterRegex(), regexp2.None)
	if err != nil {
		return errors.WithMessage(err, "db filter regex compile")
	}
	c.Compiled.TbFilter, err = regexp2.Compile(c.TableFilterRegex(), regexp2.None)
	if err != nil {
		return errors.WithMessage(err, "table filter regex compile")
	}
	return nil
}
