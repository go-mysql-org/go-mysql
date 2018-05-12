package stdlib

import (
	"fmt"
	"strings"
	"testing"

	"gopkg.in/birkirb/loggers.v1"
	"gopkg.in/birkirb/loggers.v1/mappers"
)

// goTestLog maps the testing logger to an Advanced log interface.
// However it ignores any level info.
type goTestLog struct {
	logger *testing.T
}

// NewDefaultLogger returns a Contextual logger using a *testing.T with Log/Logf output.
// This allows logging to be redirected to the test where it belongs.
func NewTestingLogger(t *testing.T) loggers.Contextual {
	var g goTestLog
	g.logger = t

	a := mappers.NewContextualMap(&g)
	a.Debug("Now using Go's stdlib testing log (via loggers/mappers/stdlib).")

	return a
}

// LevelPrint is a Mapper method
func (l *goTestLog) LevelPrint(lev mappers.Level, i ...interface{}) {
	v := []interface{}{lev}
	v = append(v, i...)
	l.logger.Log(v...)
}

// LevelPrintf is a Mapper method
func (l *goTestLog) LevelPrintf(lev mappers.Level, format string, i ...interface{}) {
	f := "%s" + format
	v := []interface{}{lev}
	v = append(v, i...)
	l.logger.Logf(f, v...)
}

// LevelPrintln is a Mapper method
func (l *goTestLog) LevelPrintln(lev mappers.Level, i ...interface{}) {
	v := []interface{}{lev}
	v = append(v, i...)
	l.logger.Log(v...)
}

// WithField returns an advanced logger with a pre-set field.
func (l *goTestLog) WithField(key string, value interface{}) loggers.Advanced {
	return l.WithFields(key, value)
}

// WithFields returns an advanced logger with pre-set fields.
func (l *goTestLog) WithFields(fields ...interface{}) loggers.Advanced {
	s := make([]string, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i = i + 2 {
		key := fields[i]
		value := fields[i+1]
		s = append(s, fmt.Sprint(key, "=", value))
	}

	r := goTestLogPostfixLogger{l, "["+strings.Join(s, ", ")+"]"}
	return mappers.NewAdvancedMap(&r)
}

type goTestLogPostfixLogger struct {
	*goTestLog
	postfix string
}

func (r *goTestLogPostfixLogger) LevelPrint(lev mappers.Level, i ...interface{}) {
	if len(r.postfix) > 0 {
		i = append(i, " ", r.postfix)
	}
	r.goTestLog.LevelPrint(lev, i...)
}

func (r *goTestLogPostfixLogger) LevelPrintf(lev mappers.Level, format string, i ...interface{}) {
	if len(r.postfix) > 0 {
		format = format + " %s"
		i = append(i, r.postfix)
	}
	r.goTestLog.LevelPrintf(lev, format, i...)
}

func (r *goTestLogPostfixLogger) LevelPrintln(lev mappers.Level, i ...interface{}) {
	i = append(i, r.postfix)
	r.goTestLog.LevelPrintln(lev, i...)
}
