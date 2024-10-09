package db_table_filter

import (
	"github.com/dlclark/regexp2"
	"github.com/pkg/errors"
)

type DbTableFilterCompiled struct {
	DbFilter *regexp2.Regexp
	TbFilter *regexp2.Regexp
}

// DbTableFilterCompile compile regex filter
func (c *DbTableFilter) DbTableFilterCompile() error {
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
