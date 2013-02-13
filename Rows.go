package monet

import (
	"database/sql/driver"
	"errors"
	"io"
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
	row    int
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
	var f []string = strings.Split(l[1:len(l)-1], ",\t")

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
		r.cou, _ = strconv.ParseInt(meta[1], 10, 64)
		colsno, _ := strconv.ParseInt(meta[2], 10, 0)
		r.cols = make([]string, colsno)
		r.types = make([]string, colsno)

		for _, l := range ll[1:] {
			switch {
			case strings.HasPrefix(l, MSG_HEADER):
				di := strings.Split(l[1:], "#")
				d, i := strings.Split(di[0], ","), di[1]
				for i, v := range d {
					d[i] = strings.Trim(v, " \t")
				}
				i = strings.Trim(i, " \t")
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
	if len(dest) != len(r.cols) {
		return errors.New("Next: len(dest) not correct")
	}
	switch {
	case r.row < len(r.rows):
		curr := r.rows[r.row]
		if no := copy(dest, curr); no != len(dest) {
			return errors.New("Next: could not copy into dest.")
		}
		r.row++
		return nil
	case int64(r.row)+r.off >= r.cou:
		return io.EOF
	default:
		r.off += int64(len(r.rows))
		lim := min(r.cou, int64(r.row)+RowsSize)
		sum := lim - r.off
		c := "Xexport " + r.qid + " " + strconv.FormatInt(r.off, 10) + " " + strconv.FormatInt(sum, 10)
		res, err := r.c.mapi(c)
		if err != nil {
			return err
		}
		ll, err := r.s.skipCheckError(res)
		if err != nil {
			return err
		}
		r.rows = make([][]driver.Value, 0)
		r.row = 0
		err = r.store(ll)
		if err != nil {
			return err
		}
		copy(dest, r.rows[r.row])
		return nil
	}
	return errors.New("Next: CODEERROR: should never be returned.")
}

func min(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}
