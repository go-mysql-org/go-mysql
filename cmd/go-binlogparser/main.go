package main

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/go-mysql-org/go-mysql/pkg"
	"github.com/go-mysql-org/go-mysql/pkg/db_table_filter"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jinzhu/copier"
	"github.com/pingcap/errors"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/spf13/viper"
)

type ParallelType string

const (
	TypeDefault      = "" // default
	TypeDatabase     = "database"
	TypeTable        = "table"
	TypeTableHash    = "table_hash"
	TypePrimaryHash  = "primary_hash"
	TypeLogicalClock = "logical_clock"
)

func parseBinlogFile() error {
	printEventInfo := &replication.PrintEventInfo{}
	printEventInfo.Init()

	p := replication.NewBinlogParser()
	p.PrintEventInfo = printEventInfo

	p.Flashback = viper.GetBool("flashback")
	p.ConvUpdateToWrite = viper.GetBool("conv-rows-update-to-write")
	eventTypeFilter := viper.GetStringSlice("rows-event-type")
	if len(eventTypeFilter) > 0 {
		for _, evt := range eventTypeFilter {
			if evt == "delete" {
				p.EventTypeFilter = append(p.EventTypeFilter, replication.DeleteEventType...)
			} else if evt == "update" {
				p.EventTypeFilter = append(p.EventTypeFilter, replication.UpdateEventType...)
			} else if evt == "insert" {
				p.EventTypeFilter = append(p.EventTypeFilter, replication.InsertEventType...)
			} else {
				return errors.Errorf("unknown eventy type %s", evt)
			}
		}
	}

	renameRules := viper.GetStringSlice("rewrite-db")
	if len(renameRules) > 0 {
		if rules, err := pkg.NewRenameRule(renameRules); err != nil {
			return err
		} else {
			p.RenameRule = rules
		}
	}

	timeFilter := &replication.TimeFilter{
		StartPos: viper.GetUint32("start-position"),
		StopPos:  viper.GetUint32("stop-position"),
	}
	if start := viper.GetString("start-datetime"); start != "" {
		startDatetime, err := time.ParseInLocation(time.DateTime, viper.GetString("start-datetime"), time.Local)
		if err != nil {
			return errors.WithMessage(err, "parse start-datetime")
		}
		timeFilter.StartTime = uint32(startDatetime.Local().Unix())
	}
	if stop := viper.GetString("stop-datetime"); stop != "" {
		stopDatetime, err := time.ParseInLocation(time.DateTime, viper.GetString("stop-datetime"), time.Local)
		if err != nil {
			return errors.WithMessage(err, "parse stop-datetime")
		}
		timeFilter.StopTime = uint32(stopDatetime.Local().Unix())
	}
	p.TimeFilter = &replication.TimeFilter{}
	copier.Copy(p.TimeFilter, timeFilter) // 这里因为 startPos, stopPos是针对不同的 file 生效

	var tableFilter *db_table_filter.DbTableFilter
	var err error
	databases := viper.GetStringSlice("databases")
	tables := viper.GetStringSlice("tables")
	excludeDatabases := viper.GetStringSlice("exclude-databases")
	excludeTables := viper.GetStringSlice("exclude-tables")
	if len(databases)+len(tables)+len(excludeDatabases)+len(excludeTables) > 0 {
		if p.Flashback {
			excludeDatabases = append(excludeDatabases, "infodba_schema")
		}
		tableFilter, err = db_table_filter.NewFilter(databases, tables, excludeDatabases, excludeTables)
		if err != nil {
			return err
		}
		p.TableFilter = tableFilter
		if err = p.TableFilter.DbTableFilterCompile(); err != nil {
			return err
		}
	} else if p.Flashback {
		tableFilter, err = db_table_filter.NewFilter([]string{"*"}, []string{"*"}, []string{"infodba_schema"}, []string{})
		p.TableFilter = tableFilter
		if err = p.TableFilter.DbTableFilterCompile(); err != nil {
			return err
		}
	}

	if rowsFilterExpr := viper.GetString("rows-filter"); rowsFilterExpr != "" {
		rowsFilter, err := replication.NewRowsFilter(rowsFilterExpr) // "col[0] == 2"
		if err != nil {
			return err
		}
		p.RowsFilter = rowsFilter
	}

	// file is alias for start-file
	fileName := viper.GetString("file")
	startFile := viper.GetString("start-file")
	stopFile := viper.GetString("stop-file")
	binlogDir := viper.GetString("binlog-dir")
	binlogDir = filepath.Join(binlogDir, filepath.Dir(startFile))

	startFile = filepath.Base(startFile)
	stopFile = filepath.Base(stopFile)
	if stopFile == "" { // 单个文件解析，可以指定输出到 stdout或者 result-file
		p.TimeFilter = timeFilter
		binlogFile := filepath.Join(binlogDir, startFile)
		resultFileName := viper.GetString("result-file")
		err = p.ParseFileAndPrint(binlogFile, resultFileName)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
	} else {
		startSeq := pkg.GetSequenceFromFilename(startFile)
		stopSeq := pkg.GetSequenceFromFilename(stopFile)
		for seq := startSeq; seq <= stopSeq; seq++ {
			fileName = pkg.ConstructBinlogFilename(startFile, seq)
			binlogFile := filepath.Join(binlogDir, fileName)
			if seq == startSeq { // 只有第一个 binlog才需要 startPos
				p.TimeFilter.StartPos = timeFilter.StartPos
			} else {
				p.TimeFilter.StartPos = 0
			}
			if seq == stopSeq { // 只有最后一个 binlog才需要 stopPos
				p.TimeFilter.StopPos = timeFilter.StopPos
			} else {
				p.TimeFilter.StopPos = 0
			}
			resultFileName := fileName + ".go.sql"
			if p.Flashback {
				resultFileName = fileName + ".back.sql"
			}
			err = p.ParseFileAndPrint(binlogFile, resultFileName)
			if err != nil {
				fmt.Println(err.Error())
				return err
			}
		}
	}
	return nil
}
