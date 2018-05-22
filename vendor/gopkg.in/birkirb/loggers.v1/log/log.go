package log

import (
	"gopkg.in/birkirb/loggers.v1"
	"gopkg.in/birkirb/loggers.v1/mappers"
	"gopkg.in/birkirb/loggers.v1/mappers/stdlib"
)

// Logger is an Contextual logger interface.
var Logger loggers.Contextual

func init() {
	Logger = stdlib.NewDefaultLogger()
}

// ParseLevel parses the level
func ParseLevel(s string) mappers.Level {
	return mappers.ParseLevel(s)
}

// SetLevel changes the level
func SetLevel(l mappers.Level) {
	if logger, ok := Logger.(mappers.LevelSetter); ok {
		logger.SetLevel(l)
	}
}

// Debug should be used when logging exessive debug info.
func Debug(v ...interface{}) {
	Logger.Debug(v...)
}

// Debugf works the same as Debug but supports formatting.
func Debugf(format string, v ...interface{}) {
	Logger.Debugf(format, v...)
}

// Debugln works the same as Debug but supports formatting.
func Debugln(v ...interface{}) {
	Logger.Debugln(v...)
}

// Info is a general function to log something.
func Info(v ...interface{}) {
	Logger.Info(v...)
}

// Infof works the same as Info but supports formatting.
func Infof(format string, v ...interface{}) {
	Logger.Infof(format, v...)
}

// Infoln works the same as Info but supports formatting.
func Infoln(v ...interface{}) {
	Logger.Infoln(v...)
}

// Warn is useful for alerting about something wrong.
func Warn(v ...interface{}) {
	Logger.Warn(v...)
}

// Warnf works the same as Warn but supports formatting.
func Warnf(format string, v ...interface{}) {
	Logger.Warnf(format, v...)
}

// Warnln works the same as Warn but prints each value on a line.
func Warnln(v ...interface{}) {
	Logger.Warnln(v...)
}

// Error should be used only if real error occures.
func Error(v ...interface{}) {
	Logger.Error(v...)
}

// Errorf works the same as Error but supports formatting.
func Errorf(format string, v ...interface{}) {
	Logger.Errorf(format, v...)
}

// Errorln works the same as Error but prints each value on a line.
func Errorln(v ...interface{}) {
	Logger.Errorln(v...)
}

// Fatal should be only used when it's not possible to continue program execution.
func Fatal(v ...interface{}) {
	Logger.Fatal(v...)
}

// Fatalf works the same as Fatal but supports formatting.
func Fatalf(format string, v ...interface{}) {
	Logger.Fatalf(format, v...)
}

// Fatalln works the same as Fatal but prints each value on a line.
func Fatalln(v ...interface{}) {
	Logger.Fatalln(v...)
}

// Panic should be used only if real panic is desired.
func Panic(v ...interface{}) {
	Logger.Panic(v...)
}

// Panicf works the same as Panic but supports formatting.
func Panicf(format string, v ...interface{}) {
	Logger.Panicf(format, v...)
}

// Panicln works the same as Panic but prints each value on a line.
func Panicln(v ...interface{}) {
	Logger.Panicln(v...)
}

// Print should be used for information messages.
func Print(v ...interface{}) {
	Logger.Print(v...)
}

// Printf works the same as Print but supports formatting.
func Printf(format string, v ...interface{}) {
	Logger.Printf(format, v...)
}

// Println works the same as Print but prints each value on a line.
func Println(v ...interface{}) {
	Logger.Println(v...)
}

// WithField adds the key value as parameter to log.
func WithField(key string, value interface{}) loggers.Advanced {
	return Logger.WithField(key, value)
}

// WithFields adds the fields as a list of key/value parameters to log. Even number expected.
func WithFields(fields ...interface{}) loggers.Advanced {
	return Logger.WithFields(fields...)
}
