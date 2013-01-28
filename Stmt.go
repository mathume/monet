package monet

import (
	"database/sql/driver"
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
	return nil, nImpl
}

func (s *mstmt) NumInput() int {
	panic(nImpl.Error())
}

func (s *mstmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nImpl
}
