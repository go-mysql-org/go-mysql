package mysql

// IsNumericType returns true if the given type is numeric.
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
