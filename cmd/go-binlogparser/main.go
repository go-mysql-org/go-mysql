package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/pkg"
	"github.com/go-mysql-org/go-mysql/pkg/db_table_filter"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jinzhu/copier"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
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

	if rowsFilterFromFile := viper.GetString("rows-filter-from-csv"); rowsFilterFromFile != "" {
		csvContent, err := os.ReadFile(rowsFilterFromFile)
		if err != nil {
			return errors.WithMessagef(err, "read file --rows-filter-from-csv=%s", rowsFilterFromFile)
		}
		buf := bytes.NewBuffer(csvContent)
		rowsFilterExpr, err := buildRowsFilterExprFromCsv(buf)
		if err != nil {
			return err
		}
		// compile
		rowsFilter, err := replication.NewRowsFilter(rowsFilterExpr) // "col[0] == 2"
		if err != nil {
			return err
		}
		p.RowsFilter = rowsFilter
	}
	if rowsFilterExpr := viper.GetString("rows-filter"); rowsFilterExpr != "" {
		if guessRowsFilterType(rowsFilterExpr) < 1 { // csv format, will change to go-expr format
			buf := bytes.NewBufferString(rowsFilterExpr)
			rowsFilterExpr, err = buildRowsFilterExprFromCsv(buf)
			if err != nil {
				return err
			}
		}
		// go-expr format
		rowsFilter, err := replication.NewRowsFilter(rowsFilterExpr) // "col[0] == 2"
		if err != nil {
			return err
		}
		p.RowsFilter = rowsFilter
	}

	// file is alias for start-file
	fileNames := viper.GetStringSlice("file")
	startFile := viper.GetString("start-file")
	stopFile := viper.GetString("stop-file")
	binlogDir := viper.GetString("binlog-dir")

	startFile = filepath.Base(startFile)
	stopFile = filepath.Base(stopFile)
	resultFileName := viper.GetString("result-file") // resultFileName is empty will output to stdout

	// 注意这里的输出逻辑
	// 单个文件解析 --files one-file
	//   正向解析/反向解析：都可以指定输出到 stdout 或者 result-file （result-file=""时输出到 stdout）
	// 多个文件解析
	//   如果是反向解析：不允许指定 result-file，不支持直接输出到 stdout，只能输出到默认的 xxx.sql 另存文件
	//   如果是正向解析
	//     --files file1,file2,file3: 精确模式，同单文件解析的输出，按顺序输出到一个 result-file
	//     --start-file file1 --stop-file file3：范围模式，每个文件的结果输出到自己的 xxx.sql 另存文件
	// output-per-file 自动根据 binlog 文件名，生产对应的解析后的 .sql 文件名。相反，输出到 result-file 或者 stdout
	if len(fileNames) == 1 {
		p.TimeFilter = timeFilter
		binlogDir = filepath.Join(binlogDir, filepath.Dir(fileNames[0]))
		fileName := filepath.Base(fileNames[0])
		binlogFile := filepath.Join(binlogDir, fileName)
		err = p.ParseFileAndPrint(binlogFile, resultFileName)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
	} else { // 多文件解析
		if p.Flashback && resultFileName != "" {
			// 多文件解析，正向解析，如果指定 result-file，则都写入同一个文件，如果不指定，则都写入 stdout
			// 多文件解析，反向解析，不允许指定 result-file
			return errors.New("--flashback cannot have --result-file when parsing multiple files")
		}
		binlogDir = filepath.Join(binlogDir, filepath.Dir(startFile))
		startSeq := pkg.GetSequenceFromFilename(startFile)
		stopSeq := pkg.GetSequenceFromFilename(stopFile)
		for seq := startSeq; seq <= stopSeq; seq++ {
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
			fileName := pkg.ConstructBinlogFilename(filepath.Base(startFile), seq)
			binlogFile := filepath.Join(binlogDir, fileName)
			if stopFile != "" { // range mode
				resultFileName = fileName + ".go.sql"
			}
			if p.Flashback { // flashback mode
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

// guessRowsFilterType 1:expr, -1:csv
func guessRowsFilterType(rowsFilterExpr string) int {
	if !strings.Contains(rowsFilterExpr, "\n") {
		return 1
	}
	// https://expr-lang.org/docs/language-definition
	exprKeyword := []string{"==", "!=", "<", ">", "<=", ">=", "&&", "||", "!",
		" and ", " AND ", " or ", " OR ", " not ", " NOT ", " in ", " IN "}
	for _, keyword := range exprKeyword {
		if strings.Contains(rowsFilterExpr, keyword) {
			return 1
		}
	}
	return -1
}

type ColumnDef struct {
	ColumnName string
	// Position in information_schema.columns, start from 1
	Position int
	// DataType original data type from schema definition
	DataType string
	// TypeAlias int, str, hex
	TypeAlias string
}
type TableColumnInfo map[string]*ColumnDef

func parseHeaderToColumnDef(columnName string) (*ColumnDef, error) {
	parts := strings.Split(columnName, ":")
	if len(parts) == 1 {
		return &ColumnDef{
			ColumnName: columnName,
		}, nil
	} else if len(parts) == 2 {
		return &ColumnDef{
			ColumnName: parts[0],
			TypeAlias:  parts[1],
		}, nil
	} else if len(parts) == 3 {
		if pos, err := cast.ToIntE(parts[3]); err == nil {
			return &ColumnDef{
				ColumnName: parts[0],
				TypeAlias:  parts[1],
				Position:   pos,
			}, nil
		} else {
			return nil, errors.WithMessagef(err, "parse column position failed from %s", columnName)
		}
	} else {
		return nil, errors.Errorf("wrong column name format %s", columnName)
	}
}

func wrapValueWithDatatype(value string, typeAlias string) string {
	numeric := []string{"tinyint", "smallint", "mediumint", "int", "integer", "bigint", "decimal", "dec", "float",
		"double", "bool", "boolean", "bit"}
	_ = map[string][]string{
		"int": numeric,
	}

	if slices.Contains(numeric, strings.ToLower(typeAlias)) {
		typeAlias = "int"
	} else {
		typeAlias = "str"
	}
	if typeAlias == "" {
		if _, err := cast.ToUint64E(value); err == nil {
			typeAlias = "int"
		} else if strings.HasPrefix(value, "0x") { // hex
			typeAlias = "hex"
		} else {
			typeAlias = "str"
		}
	}
	if typeAlias == "str" {
		if value == "" || value == "''" || value == "\"\"" {
			return "''"
		} else {
			if value[0] == '\'' && value[len(value)-1] == '\'' {
				return value
			}
			if value[0] == '"' && value[len(value)-1] == '"' {
				return value
			}
			value = fmt.Sprintf("'%s'", value)
			return value
		}
	} else {
		return strings.Trim(strings.Trim(value, "'"), "\"")
	}
}

func buildRowsFilterExprFromCsv(csvInput *bytes.Buffer) (exprOutput string, err error) {
	csvReader := csv.NewReader(csvInput)
	records, err := csvReader.ReadAll()
	if err != nil {
		return "", errors.WithMessagef(err, "Unable to parse file as CSV for")
	}
	if len(records) <= 1 {
		return "", errors.Errorf("error csv format")
	}

	rowsFilterExpr := ""
	var rowsFilterExprs []string
	/*
		var records [][]string
		lines := strings.Split(csvInput, "\n")
		for _, line := range lines {
			cols := strings.Split(line, ",")
			records = append(records, cols)
		}
	*/

	if len(records) <= 1 {
		return "", errors.Errorf("error csv format")
	}
	exprHeader := records[0]
	if len(exprHeader) == 1 {
		colDef, err := parseHeaderToColumnDef(exprHeader[0])
		if err != nil {
			return "", err
		}
		rowsFilterExpr = fmt.Sprintf("%s in ", colDef.ColumnName)
		rowsFilterExpr += "["
		var valueList []string
		for _, line := range records[1:] {
			valueList = append(valueList, wrapValueWithDatatype(line[0], colDef.TypeAlias))
		}
		rowsFilterExpr += strings.Join(valueList, ",")
		rowsFilterExpr += "]"
	} else {
		for _, line := range records[1:] {
			var lineExpr []string
			for i, col := range line {
				//rowsFilterExpr = fmt.Sprintf("%s == %s", exprHeader[i], col)
				colDef, err := parseHeaderToColumnDef(exprHeader[i])
				if err != nil {
					return "", err
				}
				lineExpr = append(lineExpr, fmt.Sprintf("%s == %s",
					colDef.ColumnName, wrapValueWithDatatype(col, colDef.TypeAlias)))
			}
			rowsFilterExprs = append(rowsFilterExprs, "("+strings.Join(lineExpr, " and ")+")")
		}
		rowsFilterExpr = strings.Join(rowsFilterExprs, " or ")
	}
	return rowsFilterExpr, nil
}
