package monet

import (
	"database/sql/driver"
	"errors"
	"strings"
)

type mrows struct {
	c      *mconn
	s *mstmt
	cols   []string
	types  []string
	rows   [][]driver.Value
	closed bool
	qid    string
	off    int64
	cou    int64
}

func (r *mrows) Close() error {
	if r.closed {
		return nil
	}
	r.s = nil
	r.c = nil
	r.closed = true
	return nil
}

func (r *mrows) parse(l string) (err error) {
	f := strings.Split(l[1:], "\t")

	if len(f) == len(r.cols) {
		var ff = make([]driver.Value, len(f))
		for i, v := range f {
			ff[i], err = goify(v, r.types[i])
		}
		r.rows = append(r.rows, ff)
	} else {
		err = errors.New("Length of Row doesn't match Header.")
	}
	return
}

func (r *mrows) store(ll []string) error {
	return nImpl
}

func newRows(c *mconn, s *mstmt)driver.Rows {
	r := new(mrows)
	r.c = c
	r.s = s
	r.rows = make([][]driver.Value, 0)
	return r
}

func (r *mrows) Columns() []string {
	return r.cols
}

func (r *mrows) Next(dest []driver.Value) error {
	return nImpl
}
