package mappers

import (
	"strings"

	"gopkg.in/birkirb/loggers.v1"
)

type (
	// Level indicates a specific log level.
	Level byte

	// LevelMapper interfaces allows a logger to map to any Advanced Logger.
	LevelMapper interface {
		LevelPrint(Level, ...interface{})
		LevelPrintf(Level, string, ...interface{})
		LevelPrintln(Level, ...interface{})
	}

	// ContextualMapper interfaces allows a logger to map to any Contextual Logger.
	ContextualMapper interface {
		LevelMapper
		WithField(key string, value interface{}) loggers.Advanced
		WithFields(fields ...interface{}) loggers.Advanced
	}

	// LevelSetter changes the level
	LevelSetter interface {
		SetLevel(l Level)
	}
)

const (
	// LevelDebug is a log Level.
	LevelDebug Level = iota
	// LevelInfo is a log Level.
	LevelInfo
	// LevelWarn is a log Level.
	LevelWarn
	// LevelError is a log Level.
	LevelError
	// LevelFatal is a log Level.
	LevelFatal
	// LevelPanic is a log Level.
	LevelPanic
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG "
	case LevelInfo:
		return "INFO  "
	case LevelWarn:
		return "WARN  "
	case LevelError:
		return "ERROR "
	case LevelFatal:
		return "FATAL "
	case LevelPanic:
		return "PANIC "
	default:
		panic("Missing case statement in Level String.")
	}
}

// ParseLevel parses the level from string. Return default Info Level if error.
func ParseLevel(s string) Level {
	switch strings.ToLower(s) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	case "fatal":
		return LevelFatal
	case "panic":
		return LevelPanic
	default:
		return LevelInfo
	}
}
