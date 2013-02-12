package monet

import (
	"database/sql/driver"
	"errors"
	"strconv"
	"strings"
)

type mrows struct {
	c      *mconn
	s      *mstmt
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

func (r *mrows) store(ll []string) (err error) {
	ll = r.s.skipInfoL(ll)
	ll, err = r.s.checkError(ll)
	if err != nil {
		return err
	}
	switch {
	case strings.HasPrefix(ll[0], MSG_QTABLE):
		meta := r.s.stripws(ll[0][2:])
		r.qid = meta[0]
		r.cou, _ = strconv.ParseInt(meta[1], 10, -1)
		colsno, _ := strconv.ParseInt(meta[2], 10, -1)
		r.cols = make([]string, colsno)
		r.types = make([]string, colsno)

		for _, l := range ll[1:] {
			switch {
			case strings.HasPrefix(l, MSG_HEADER):
				di := strings.Split(l[1:], "#")
				d, i := strings.Split(di[0], ","), di[1]
				for i, v := range d {
					d[i] = strings.Trim(v, " ")
				}
				i = strings.Trim(i, " ")
				switch i {
				case "name":
					r.cols = d
				case "type":
					r.types = d
				}
			case strings.HasPrefix(l, MSG_TUPLE):
				if err = r.parse(l); err != nil {
					return
				}
			}
		}
	case strings.HasPrefix(ll[0], MSG_QBLOCK):
		for _, l := range ll[1:] {
			if strings.HasPrefix(l, MSG_TUPLE) {
				if err = r.parse(l); err != nil {
					return
				}
			}
		}
	default:
		err = errors.New("Unkown state " + ll[0])
	}
	return
}

func newRows(c *mconn, s *mstmt) driver.Rows {
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
