package monet

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"
)

const TimeLayout = "2006-01-02 15:04:05.000000"

func monetize(value driver.Value) (s string, err error) {
	if value == nil {
		return "NULL", nil
	}
	switch t := value.(type) {
	case int64:
		s = fmt.Sprintf("%d", t)
	case float64:
		s = fmt.Sprintf("%g", t)
	case string:
		s = escape(t)
	case bool:
		s = fmt.Sprintf("%t", t)
	case []byte:
		s = escape(string(t))
	case time.Time:
		s = escape(t.Format(TimeLayout))
	default:
		err = errors.New(fmt.Sprintf("Value type %T not supported.", t))
	}
	return s, err
}

func escape(s string) string {
	s = strings.Replace(s, "\\", "\\\\", -1)
	s = strings.Replace(s, "'", "\\'", -1)
	return fmt.Sprintf("'%s'", s)
}
