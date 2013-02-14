package monet

import (
	"database/sql/driver"
	"strconv"
	"strings"
	"time"
	"errors"
)

func unescape(data string) string {
	data = string([]byte(data)[1 : len(data)-1])
	data = strings.Replace(data, "\\\\", "\\", -1)
	data = strings.Replace(data, "\\'", "'", -1)
	return data
}

func goify(data string, tcode string) (s driver.Value, err error) {
	data = strings.Trim(data, " \t")
	if data == "NULL" {
		return nil, nil
	}
	switch tcode {
	case CHAR, VARCHAR, CLOB:
		s = unescape(data)
	case SMALLINT, INT, WRD, BIGINT, SERIAL, TINYINT, SHORTINT, MEDIUMINT, LONGINT:
		s, err = strconv.ParseInt(data, 10, 64)
	case REAL, DOUBLE, FLOAT:
		s, err = strconv.ParseFloat(data, 64)
	case BLOB:
		s = data
	case BOOLEAN:
		s = data == "true"
	case TIMESTAMP:
		s, err = time.Parse(TimeLayout, data)
	default:
		err = errors.New("Type " + tcode + " not supported.")
	}
	return
}

const (
	//type codes in comments currently not supported by go driver
	CHAR     = "char"    // (L) character string with length L
	VARCHAR  = "varchar" // (L) string with atmost length L
	CLOB     = "clob"
	BLOB     = "blob"
	DECIMAL  = "decimal"  // (P,S)
	SMALLINT = "smallint" // 16 bit integer
	INT      = "int"      // 32 bit integer
	BIGINT   = "bigint"   // 64 bit integer
	SERIAL   = "serial"   // special 64 bit integer (sequence generator)
	REAL     = "real"     // 32 bit floating point
	DOUBLE   = "double"   // 64 bit floating point
	BOOLEAN  = "boolean"
	//DATE      = "date"
	//TIME      = "time"      // (T) time of day
	TIMESTAMP = "timestamp" // (T) date concatenated with unique time
	//INTERVAL  = "interval"  // (Q) a temporal interval

	//MONTH_INTERVAL = "month_interval"
	//SEC_INTERVAL   = "sec_interval"
	WRD = "wrd"

	// Not on the website:
	TINYINT   = "tinyint"
	SHORTINT  = "shortint"
	MEDIUMINT = "mediumint"
	LONGINT   = "longint"
	FLOAT     = "float"
	//TIMESTAMPTZ = "timestamptz"

	// full names and aliases, spaces are replaced with underscores
	CHARACTER               = CHAR
	CHARACTER_VARYING       = VARCHAR
	CHARACHTER_LARGE_OBJECT = CLOB
	BINARY_LARGE_OBJECT     = BLOB
	NUMERIC                 = DECIMAL
	DOUBLE_PRECISION        = DOUBLE
)
