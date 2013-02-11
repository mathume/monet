package monet

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
)

const (
	SQLPlaceholder = "%s"
)

type mstmt struct {
	c      *mconn
	q      string
	closed bool
}

func newStmt(c *mconn, query string) driver.Stmt {
	s := new(mstmt)
	s.c = c
	s.q = query
	return s
}

func (s *mstmt) Close() (err error) {
	if s.closed {
		return
	}
	s.c = nil
	s.closed = true
	return
}

func (s *mstmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.closed {
		return nil, errors.New("Stmt is closed.")
	}
	qry, err := s.bind(args)
	if err != nil {
		return nil, err
	}
	_, err = s.c.cmd(qry)
	return nil, nImpl
}

func (s *mstmt) bind(args []driver.Value) (query string, err error) {
	if strings.Count(s.q, SQLPlaceholder) != len(args) {
		err = errors.New("Cannot bind args to query. Wrong number of args.")
		return
	}
	mon := make([]interface{}, len(args))
	for i, v := range args {
		mon[i], err = monetize(v)
	}
	query = fmt.Sprintf(s.q, mon...)
	return
}

func (s *mstmt) NumInput() int {
	panic(nImpl.Error())
}

func (s *mstmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nImpl
}
