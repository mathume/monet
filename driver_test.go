package monet

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	. "launchpad.net/gocheck"
	"log"
	"strconv"
	"strings"
	"time"
)

var TestErr error = errors.New("TestErr")

type DRIVER struct{}

const (
	NO_c_MSG_INFO = "NO_c_MSG_INFO"
)

var serverResponse = "&1 0 1 6 1\n% sys.alltypes,	sys.alltypes,	sys.alltypes,	sys.alltypes,	sys.alltypes,	sys.alltypes # table_name \n% col1,	col2,	col3,	col4,	col5,	col6 # name\n% bigint,	double,	timestamp,	varchar,	clob,	blob # type\n% 14,	24,	26,	12,	12,	0 # length\n[ 12342524353465,	1.24354e-95,	2013-02-13 13:53:09.000000,	\"kaixo mundua\",	\"kaixo mundua\",	100110	]"

var _ = Suite(&DRIVER{})

func (d *DRIVER) SetUpSuite(c *C) {
	logger = new(writer)
	Logger = log.New(logger, "driver_test ", log.LstdFlags)
}

func (d *DRIVER) SetUpTest(c *C) {
	logger.Clear()
}

func getconnfs(err error) (*mconn, Server) {
	con := new(mconn)
	fs := newFakeServer(err)
	con.srv = fs.(Server)
	return con, fs
}

func getConnFakeServer() (*mconn, Server) {
	return getconnfs(nil)
}

func getConnFakeServerWithError() (*mconn, Server) {
	return getconnfs(TestErr)
}

func (d *DRIVER) TestNewTx(c *C) {
	c.Assert(newTx(new(mconn)), Not(IsNil))
}

func (d *DRIVER) TestBeginSendsStartTransaction(c *C) {
	con, fs := getConnFakeServer()
	con.Begin()
	c.Assert(contains(fs.(*fsrv).received, "sSTART TRANSACTION;"), Equals, true)
}

func (d *DRIVER) TestBeginCanOnlyBeCalledOnce(c *C) {
	con, _ := getConnFakeServer()
	t, err := con.Begin()
	c.Assert(err, IsNil)
	c.Assert(t, Not(IsNil))
	t, err = con.Begin()
	c.Assert(t, IsNil)
	c.Assert(err, Not(IsNil))
}

func (d *DRIVER) TestCloseWillRollbackWhenThereIsATx(c *C) {
	con, fs := getConnFakeServer()
	con.Begin()
	con.Close()
	c.Assert(contains(fs.(*fsrv).received, "sROLLBACK;"), Equals, true)
}

func (d *DRIVER) TestCloseWillNotRollbackWithoutTx(c *C) {
	con, fs := getConnFakeServer()
	con.Close()
	c.Assert(contains(fs.(*fsrv).received, "sROLLBACK;"), Equals, false)
}

func (d *DRIVER) TestCloseDisconnects(c *C) {
	con, fs := getConnFakeServer()
	c.Assert(fs.(*fsrv).disconnected, Equals, false)
	con.Close()
	c.Assert(fs.(*fsrv).disconnected, Equals, true)
}

func (d *DRIVER) TestClosePipesError(c *C) {
	con, _ := getConnFakeServerWithError()
	c.Assert(con.Close(), Equals, TestErr)
}

func (d *DRIVER) TestCommitIsReceived(c *C) {
	con, fs := getConnFakeServer()
	tx, _ := con.Begin()
	tx.Commit()
	c.Assert(contains(fs.(*fsrv).received, "sCOMMIT;"), Equals, true)
}

func (d *DRIVER) TestCanBeginAfterCommit(c *C) {
	con, _ := getConnFakeServer()
	tx, _ := con.Begin()
	tx.Commit()
	_, err := con.Begin()
	c.Assert(err, IsNil)
}

func (d *DRIVER) TestCanBeginAfterRollback(c *C) {
	con, _ := getConnFakeServer()
	tx, _ := con.Begin()
	tx.Rollback()
	_, err := con.Begin()
	c.Assert(err, IsNil)
}

func (d *DRIVER) TestCommitClears(c *C) {
	con, _ := getConnFakeServer()
	tx, _ := con.Begin()
	tx.Commit()
	c.Assert(con.tx, IsNil)
}

func (d *DRIVER) TestRollbackClears(c *C) {
	con, _ := getConnFakeServer()
	tx, _ := con.Begin()
	tx.Rollback()
	c.Assert(con.tx, IsNil)
}

func (d *DRIVER) TestClear(c *C) {
	con := new(mconn)
	tx := new(mtx)
	con.tx = tx
	tx.c = con
	tx.clear()
	c.Assert(tx.c, IsNil)
	c.Assert(con.tx, IsNil)
}

func (d *DRIVER) TestCloseStmt(c *C) {
	s := newStmt(new(mconn), "anyQuery")
	c.Assert(s.(*mstmt).closed, Equals, false)
	c.Assert(s.(*mstmt).c, Not(IsNil))
	for i := 0; i < 2; i++ {
		c.Assert(s.Close(), IsNil)
		c.Assert(s.(*mstmt).closed, Equals, true)
		c.Assert(s.(*mstmt).c, IsNil)
	}
}

func (d *DRIVER) TestPrepareStmtSetsQuery(c *C) {
	con, _ := getConnFakeServer()
	fmtquery := "SELECT COUNT(*) FROM %s"
	stmt, err := con.Prepare(fmtquery)
	c.Assert(err, IsNil)
	c.Assert(stmt.(*mstmt).q, Equals, fmtquery)
}

func (d *DRIVER) TestResult(c *C) {
	r := newResult(1, 2, TestErr)
	rr := r.(*mresult)
	liid, e1 := r.LastInsertId()
	c.Assert(liid, Equals, rr.liid)
	c.Assert(e1, Equals, rr.err)
	ra, e2 := r.RowsAffected()
	c.Assert(ra, Equals, rr.ra)
	c.Assert(e2, Equals, rr.err)
}

func (d *DRIVER) TestConvertFloat64(c *C) {
	var any float64 = 1.22
	s, err := monetize(any)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "1.22")
}

func (d *DRIVER) TestGoifyFloat64(c *C) {
	any := "12e-200"
	s, err := goify(any, FLOAT)
	c.Assert(err, IsNil)
	c.Assert(s.(float64), Equals, float64(12e-200))
}

func (d *DRIVER) TestGoifyFloat64a(c *C) {
	any := "12.22"
	s, err := goify(any, FLOAT)
	c.Assert(err, IsNil)
	c.Assert(s.(float64), Equals, float64(12.22))
}

func (d *DRIVER) TestInvariantFloat64(c *C) {
	var any float64 = 13.33
	any1, err := monetize(any)
	c.Assert(err, IsNil)
	any2, err := goify(any1, FLOAT)
	c.Assert(err, IsNil)
	c.Assert(any2, Equals, any)
}

func (d *DRIVER) TestConvertString(c *C) {
	var any string = "string"
	s, err := monetize(any)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "'"+any+"'")
}

func (d *DRIVER) TestGoifyString(c *C) {
	any := "'string'"
	s, err := goify(any, CHAR)
	c.Assert(err, IsNil)
	c.Assert(s.(string), Equals, "string")
}

func (d *DRIVER) TestConvertInt64(c *C) {
	var any int64 = 12
	s, err := monetize(any)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "12")
}

func (d *DRIVER) TestGoifyInt64(c *C) {
	any := "12345"
	s, err := goify(any, INT)
	c.Assert(err, IsNil)
	c.Assert(s.(int64), Equals, int64(12345))
}

func (d *DRIVER) TestConvertBool(c *C) {
	var f bool = false
	s, err := monetize(f)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "false")
	var t bool = true
	s, err = monetize(t)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "true")
}

func (d *DRIVER) TestGoifyBool(c *C) {
	f := "false"
	s, err := goify(f, BOOLEAN)
	c.Assert(err, IsNil)
	c.Assert(s.(bool), Equals, false)
}

func (d *DRIVER) TestConvertByteSlice(c *C) {
	var anyWithBackSlashes []byte = []byte("a\\b'c")
	s, err := monetize(anyWithBackSlashes)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "'a\\\\b\\'c'")
}

func (d *DRIVER) TestGoifyByteSliceBLOB(c *C) {
	b := "10110"
	s, err := goify(b, BLOB)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "10110")
}

func (d *DRIVER) TestGoifyByteSliceCLOB(c *C) {
	b := "'a\\\\b\\'c\\''"
	s, err := goify(b, CLOB)
	c.Assert(err, IsNil)
	c.Assert(s.(string), DeepEquals, "a\\b'c'")
}

func (d *DRIVER) TestConvertTime(c *C) {
	var t time.Time = time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)
	s, err := monetize(t)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "'"+TimeLayout+"'")
}

func (d *DRIVER) TestGoifyTime(c *C) {
	t := TimeLayout
	s, err := goify(t, TIMESTAMP)
	c.Assert(err, IsNil)
	c.Assert(s.(time.Time).Format(TimeLayout), Equals, TimeLayout)
}

func (d *DRIVER) TestConvertNil(c *C) {
	s, err := monetize(nil)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "NULL")
}

func (d *DRIVER) TestGofiyNil(c *C) {
	n := "NULL"
	s, err := goify(n, "ANYTYPECODE")
	c.Assert(err, IsNil)
	c.Assert(s, IsNil)
}

func (d *DRIVER) TestEscape(c *C) {
	s := "a\\b'c'"
	e := escape(s)
	c.Assert(e, Equals, "'a\\\\b\\'c\\''")
}

func (d *DRIVER) TestUnescape(c *C) {
	s := "'a\\\\b\\'c\\''"
	e := unescape(s)
	c.Assert(e, Equals, "a\\b'c'")
}

func (d *DRIVER) TestClosedStmtCannotExec(c *C) {
	s := newStmt(new(mconn), "anyqry")
	s.Close()
	_, err := s.Exec([]driver.Value{"anyargs"})
	c.Assert(err, Not(IsNil))
	c.Assert(err, ErrorMatches, "Stmt is closed.")
}

func (d *DRIVER) TestStmtBindChecksNoOfArguments(c *C) {
	s := newStmt(new(mconn), "query with on %s")
	zeroL := make([]driver.Value, 0)
	tooL := []driver.Value{1, 2}
	_, err := s.(*mstmt).bind(zeroL)
	c.Assert(err, Not(IsNil))
	c.Assert(strings.Contains(err.Error(), "Wrong number of args."), Equals, true)
	_, err = s.(*mstmt).bind(tooL)
	c.Assert(err, Not(IsNil))
	c.Assert(strings.Contains(err.Error(), "Wrong number of args."), Equals, true)
}

func (d *DRIVER) TestStmtBind(c *C) {
	s := newStmt(new(mconn), "query with monetized time=%s")
	exact := time.Date(2013, time.February, 11, 0, 0, 0, 0, time.UTC)
	qry, err := s.(*mstmt).bind([]driver.Value{exact})
	c.Assert(err, IsNil)
	c.Assert(qry, Equals, "query with monetized time='2013-02-11 00:00:00.000000'")
}

func (d *DRIVER) TestStmtExecCallsConnCmd(c *C) {
	co, sr := getConnFakeServer()
	s := newStmt(co, "time=%s")
	exact := time.Date(2013, time.February, 11, 0, 0, 0, 0, time.UTC)
	s.Exec([]driver.Value{exact})
	c.Assert(sr.(*fsrv).received[0], Equals, "stime='2013-02-11 00:00:00.000000';")
}

func (d *DRIVER) TestStmtSkipInfo(c *C) {
	r := c_MSG_INFO + "any\n" + NO_c_MSG_INFO + "other\n"
	s := new(mstmt)
	ll := s.skipInfo(r)
	c.Assert(ll, Not(IsNil))
	c.Assert(len(ll) > 0, Equals, true)
	c.Assert(ll[0], Equals, NO_c_MSG_INFO+"other")
}

func (d *DRIVER) TestStmtResultReturnsError(c *C) {
	errMsg := "error message"
	r := c_MSG_ERROR + errMsg
	_, err := new(mstmt).getResult(r)
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "error message")
}

func (d *DRIVER) TestStmtResultReturnsResultNoRows(c *C) {
	m := []string{c_MSG_QTRANS, c_MSG_QSCHEMA}
	for _, v := range m {
		msg := v + "c_MSG"
		r, err := new(mstmt).getResult(msg)
		c.Assert(err, IsNil)
		c.Assert(r, Equals, driver.ResultNoRows)
	}
}

func (d *DRIVER) TestStrip(c *C) {
	s := "a b\tc\rd\ne \t\r\nf"
	ss := new(mstmt).stripws(s)
	c.Assert(len(ss), Equals, 6)
	s = ""
	for _, v := range ss {
		s += string(v)
		c.Log(string(v))
	}
	c.Log(s)
	c.Assert(len(ss), Equals, 6)
	c.Assert(s, Equals, "abcdef")
}

func (d *DRIVER) TestStmtResult(c *C) {
	var rowsaff int64 = 123456
	var lastin int64 = 34
	msg := c_MSG_QUPDATE + strconv.FormatInt(rowsaff, 10) + "\t" + strconv.FormatInt(lastin, 10)
	c.Log(msg)
	r, err := new(mstmt).getResult(msg)
	c.Assert(err, IsNil)
	c.Assert(r, Not(IsNil))
	ra, err := r.RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(ra, Equals, rowsaff)
	liid, err := r.LastInsertId()
	c.Assert(err, IsNil)
	c.Assert(liid, Equals, lastin)
}

func (d *DRIVER) TestRowsCloseDetachesConn(c *C) {
	r := new(mrows)
	r.c = new(mconn)
	c.Assert(r.Close(), IsNil)
	c.Assert(r.c, IsNil)
	c.Assert(r.closed, Equals, true)
}

func (d *DRIVER) TestRowsCloseIdem(c *C) {
	r := new(mrows)
	c.Assert(r.Close(), IsNil)
	c.Assert(r.Close(), IsNil)
	c.Assert(r.closed, Equals, true)
}

func (d *DRIVER) TestRowsParse(c *C) {
	msg_tuple := "[ 12342524353465,	1.24354e-95,	2013-02-13 13:53:09.000000,	\"kaixo mundua\",	\"kaixo mundua\",	100110	]"

	r := newRows(new(mconn), new(mstmt))
	r.(*mrows).cols = make([]string, 6)
	r.(*mrows).types = []string{BIGINT, FLOAT, TIMESTAMP, VARCHAR, CLOB, BLOB}
	c.Log(r.(*mrows).types)
	err := r.(*mrows).parse(msg_tuple)

	c.Assert(err, IsNil)
	c.Assert(len(r.(*mrows).rows), Equals, 1)
	c.Log(r.(*mrows).rows)
	c.Check(r.(*mrows).rows[0][0], Equals, int64(12342524353465))
	c.Check(r.(*mrows).rows[0][1], Equals, float64(1.24354e-95))
	c.Check(r.(*mrows).rows[0][2], Equals, time.Date(2013, time.February, 13, 13, 53, 9, 0, time.UTC))
	c.Check(r.(*mrows).rows[0][3], Equals, "kaixo mundua")
	c.Check(r.(*mrows).rows[0][4], Equals, "kaixo mundua")
	c.Check(r.(*mrows).rows[0][5], Equals, "100110")
}

func (d *DRIVER) TestParseDate(c *C) {
	date := "2013-02-13 13:53:09.000000"
	t, _ := time.Parse(TimeLayout, date)
	c.Assert(t.Format(TimeLayout), Equals, date)
}

func (d *DRIVER) TestStore(c *C) {
	r := newRows(new(mconn), new(mstmt))
	ll := strings.Split(serverResponse, "\n")
	err := r.(*mrows).store(ll)
	c.Check(err, IsNil)
	c.Assert(r.(*mrows).qid, Equals, "0")
	c.Assert(r.(*mrows).cou, Equals, int64(1))
	cols := r.Columns()
	c.Assert(len(cols), Equals, 6)
	c.Assert(cols[0], Equals, "col1")
	c.Assert(cols[5], Equals, "col6")
	t := r.(*mrows).types
	c.Assert(len(t), Equals, 6)
	c.Assert(t[0], Equals, BIGINT)
	c.Assert(t[5], Equals, BLOB)
	dest := r.(*mrows).rows[0]
	c.Assert(r.Next(dest), IsNil)
	et, _ := time.Parse(TimeLayout, "2013-02-13 13:53:09.000000")
	c.Assert(dest[2].(time.Time).Equal(et), Equals, true)

}

func (d *DRIVER) TestNextChecksInputLength(c *C) {
	dest := make([]driver.Value, 0)
	r := newRows(new(mconn), new(mstmt))
	r.(*mrows).cols = make([]string, 1)
	err := r.Next(dest)
	c.Assert(err, Not(IsNil))
}

func (d *DRIVER) TestNextCopiesIfExistsData(c *C) {
	r := newRows(new(mconn), new(mstmt))
	row := []driver.Value{1, 2}
	r.(*mrows).rows = append(r.(*mrows).rows, row)
	r.(*mrows).cols = []string{"col1", "col2"}
	dest := make([]driver.Value, 2)
	err := r.Next(dest)
	c.Assert(err, IsNil)
	c.Assert(dest, DeepEquals, row)
}

func (d *DRIVER) TestNextReturnsEOF(c *C) {
	r := newRows(new(mconn), new(mstmt))
	totalrows := 1
	rowsize := 2
	r.(*mrows).rows = make([][]driver.Value, totalrows)
	r.(*mrows).cols = make([]string, rowsize)
	r.(*mrows).row = totalrows
	r.(*mrows).off = 0
	r.(*mrows).cou = int64(totalrows)
	err := r.Next(make([]driver.Value, rowsize))
	c.Assert(err, Equals, io.EOF)
}

func (d *DRIVER) TestNextSet(c *C) {
	//basic test see LIVE.TestExceedRowsSize
}

func (d *DRIVER) TestSQLDoesntCallOpenOnOpen(c *C) {
	_, err := sql.Open(DRV_NAME, "anything")
	c.Assert(err, IsNil)
}
