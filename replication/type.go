package replication

import (
	. "github.com/siddontang/go-mysql/mysql"
)

// IsNumericType returns true if the given type is numeric type. From: sql/log_event.cc and sql/field.h
func IsNumericType(typ byte) bool {
	switch typ {
	case MYSQL_TYPE_TINY,
		MYSQL_TYPE_SHORT,
		MYSQL_TYPE_INT24,
		MYSQL_TYPE_LONG,
		MYSQL_TYPE_LONGLONG,
		MYSQL_TYPE_FLOAT,
		MYSQL_TYPE_DOUBLE,
		MYSQL_TYPE_DECIMAL,
		MYSQL_TYPE_NEWDECIMAL:
		return true

	default:
		return false
	}

}

// IsCharacterType returns true if the given type is character type. From: sql/log_event.cc
func IsCharacterType(typ byte) bool {
	switch typ {
	case MYSQL_TYPE_STRING,
		MYSQL_TYPE_VAR_STRING,
		MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_BLOB:
		return true

	default:
		return false
	}

}
