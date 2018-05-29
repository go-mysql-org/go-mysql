package stdlib

import (
	"fmt"
	"log"
	"os"
	"strings"

	"gopkg.in/birkirb/loggers.v1"
	"gopkg.in/birkirb/loggers.v1/mappers"
)

// goLog maps the standard log package logger to an Advanced log interface.
// However it mostly ignores any level info.
type goLog struct {
	logger *log.Logger
	level  mappers.Level
}

// NewDefaultLogger returns a Contextual logger using a log.Logger with stderr output.
func NewDefaultLogger() loggers.Contextual {
	var g goLog
	g.logger = log.New(os.Stderr, "", log.Ldate|log.Ltime)
	g.level = mappers.LevelInfo

	a := mappers.NewContextualMap(&g)
	a.Debug("Now using Go's stdlib log package (via loggers/mappers/stdlib).")

	return a
}

// NewLogger creates a Contextual logger from a log.Logger.
func NewLogger(l *log.Logger) loggers.Contextual {
	var g goLog
	g.logger = l

	a := mappers.NewContextualMap(&g)
	a.Debug("Now using Go's stdlib log package (via loggers/mappers/stdlib).")

	return a
}

// SetLevel implements LevelSetter interface
func (l *goLog) SetLevel(level mappers.Level) {
	l.level = level
}

// LevelPrint is a Mapper method
func (l *goLog) LevelPrint(lev mappers.Level, i ...interface{}) {
	if l.level > lev {
		return
	}

	v := []interface{}{lev}
	v = append(v, i...)
	l.logger.Print(v...)
}

// LevelPrintf is a Mapper method
func (l *goLog) LevelPrintf(lev mappers.Level, format string, i ...interface{}) {
	if l.level > lev {
		return
	}

	f := "%s" + format
	v := []interface{}{lev}
	v = append(v, i...)
	l.logger.Printf(f, v...)
}

// LevelPrintln is a Mapper method
func (l *goLog) LevelPrintln(lev mappers.Level, i ...interface{}) {
	if l.level > lev {
		return
	}

	v := []interface{}{lev}
	v = append(v, i...)
	l.logger.Println(v...)
}

// WithField returns an advanced logger with a pre-set field.
func (l *goLog) WithField(key string, value interface{}) loggers.Advanced {
	return l.WithFields(key, value)
}

// WithFields returns an advanced logger with pre-set fields.
func (l *goLog) WithFields(fields ...interface{}) loggers.Advanced {
	s := make([]string, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i = i + 2 {
		key := fields[i]
		value := fields[i+1]
		s = append(s, fmt.Sprint(key, "=", value))
	}

	r := gologPostfixLogger{l, "[" + strings.Join(s, ", ") + "]"}
	return mappers.NewAdvancedMap(&r)
}

type gologPostfixLogger struct {
	*goLog
	postfix string
}

func (r *gologPostfixLogger) LevelPrint(lev mappers.Level, i ...interface{}) {
	if len(r.postfix) > 0 {
		i = append(i, " ", r.postfix)
	}
	r.goLog.LevelPrint(lev, i...)
}

func (r *gologPostfixLogger) LevelPrintf(lev mappers.Level, format string, i ...interface{}) {
	if len(r.postfix) > 0 {
		format = format + " %s"
		i = append(i, r.postfix)
	}
	r.goLog.LevelPrintf(lev, format, i...)
}

func (r *gologPostfixLogger) LevelPrintln(lev mappers.Level, i ...interface{}) {
	i = append(i, r.postfix)
	r.goLog.LevelPrintln(lev, i...)
}
