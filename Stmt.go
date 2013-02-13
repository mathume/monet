package monet

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	SQLPlaceholder = "%s"
	RowsSize       = 100
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
	res, err := s.cmd(args)
	if err != nil {
		return nil, err
	}
	return s.getResult(res)
}

func (s *mstmt) cmd(args []driver.Value) (res string, err error) {
	if s.closed {
		err = errors.New("Stmt is closed.")
		return
	}
	qry, err := s.bind(args)
	if err != nil {
		return
	}
	res, err = s.c.cmd(qry)
	return
}

func (s *mstmt) getResult(res string) (r driver.Result, err error) {
	ll, err := s.skipCheckError(res)
	if err != nil {
		return
	}
	switch {
	case strings.HasPrefix(ll[0], MSG_QTRANS), strings.HasPrefix(ll[0], MSG_QSCHEMA):
		r = driver.ResultNoRows
	case strings.HasPrefix(ll[0], MSG_QUPDATE):
		sai := s.stripws(ll[0][2:])
		if ra, er1 := strconv.ParseInt(sai[0], 10, 64); err != nil {
			err = er1
		} else {
			if liid, er2 := strconv.ParseInt(sai[1], 10, 64); err != nil {
				err = er2
			} else {
				r = &mresult{liid, ra, nil}
			}
		}
	default:
		err = errors.New("Unknown state " + ll[0])
	}
	return
}

func (s *mstmt) skipInfo(res string) (lines []string) {
	ll := strings.Split(res, "\n")
	return s.skipInfoL(ll)
}

func (s *mstmt) skipInfoL(ll []string) (lines []string) {
	for strings.HasPrefix(ll[0], MSG_INFO) {
		ll = ll[1:]
	}
	return ll
}

func (s *mstmt) stripws(l string) []string {
	res := make([]string, 0)
	if l == "" {
		return res
	}
	l = strings.Replace(l, "\t", " ", -1)
	l = strings.Replace(l, "\n", " ", -1)
	l = strings.Replace(l, "\r", " ", -1)
	ss := strings.Split(l, " ")
	for _, l := range ss {
		if strings.Trim(l, " ") != "" {
			res = append(res, l)
		}
	}
	return res
}

func (s *mstmt) bind(args []driver.Value) (query string, err error) {
	if s.NumInput() != len(args) {
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
	return strings.Count(s.q, SQLPlaceholder)
}

func (s *mstmt) Query(args []driver.Value) (driver.Rows, error) {
	if _, err := s.c.mapi("Xreply_size " + strconv.FormatInt(RowsSize, 10)); err != nil {
		return nil, err
	}
	res, err := s.cmd(args)
	if err != nil {
		return nil, err
	}
	return s.getRows(res)
}

func (s *mstmt) getRows(res string) (driver.Rows, error) {
	ll, err := s.skipCheckError(res)
	if err != nil {
		return nil, err
	}
	r := newRows(s.c, s)
	err = r.(*mrows).store(ll)
	return r, err
}

// returns or err != nil or len(ll) > 0
func (s *mstmt) skipCheckError(res string) (ll []string, err error) {
	ll = s.skipInfo(res)
	return s.checkError(ll)
}

func (s *mstmt) checkError(la []string) (ll []string, err error){
	if len(la) == 0 {
		return nil, errors.New("Result empty")
	}
	if strings.HasPrefix(la[0], MSG_ERROR) {
		if len(la[0]) == 1 {
			err = errors.New("NO ERROR SPECS RECEIVED FROM SERVER")
		} else {
			err = errors.New(la[0][1:])
		}
	}
	ll = la
	return
}
