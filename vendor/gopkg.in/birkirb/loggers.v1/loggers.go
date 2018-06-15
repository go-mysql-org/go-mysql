package loggers

// Standard is the interface used by Go's standard library's log package.
type Standard interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Fatalln(args ...interface{})

	Panic(args ...interface{})
	Panicf(format string, args ...interface{})
	Panicln(args ...interface{})

	Print(args ...interface{})
	Printf(format string, args ...interface{})
	Println(args ...interface{})
}

// Advanced is an interface with commonly used log level methods.
type Advanced interface {
	Standard

	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Debugln(args ...interface{})

	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Errorln(args ...interface{})

	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Infoln(args ...interface{})

	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Warnln(args ...interface{})
}

// Contextual is an interface that allows context addition to a log statement before
// calling the final print (message/level) method.
type Contextual interface {
	Advanced

	WithField(key string, value interface{}) Advanced
	WithFields(fields ...interface{}) Advanced
}
