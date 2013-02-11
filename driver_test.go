package monet

import (
	"database/sql/driver"
	"errors"
	. "launchpad.net/gocheck"
	"log"
	"strings"
	"time"
)

var TestErr error = errors.New("TestErr")

type DRIVER struct{}

var _ = Suite(&DRIVER{})

func (d *DRIVER) SetUpSuite(c *C) {
	logger = new(writer)
	Logger = log.New(logger, "driver_test ", log.LstdFlags)
}

func (d *DRIVER) SetUpTest(c *C) {
	logger.Clear()
}

func getconnfs(err error) (*mconn, fakeServer) {
	con := new(mconn)
	fs := newFakeServer(err)
	con.srv = fs.(Server)
	return con, fs
}

func getConnFakeServer() (*mconn, fakeServer) {
	return getconnfs(nil)
}

func getConnFakeServerWithError() (*mconn, fakeServer) {
	return getconnfs(TestErr)
}

func (d *DRIVER) TestNewTx(c *C) {
	c.Assert(newTx(new(mconn)), Not(IsNil))
}

func (d *DRIVER) TestBeginSendsStartTransaction(c *C) {
	con, fs := getConnFakeServer()
	con.Begin()
	c.Assert(contains(fs.Received(), "sSTART TRANSACTION;"), Equals, true)
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
	c.Assert(contains(fs.Received(), "sROLLBACK;"), Equals, true)
}

func (d *DRIVER) TestCloseWillNotRollbackWithoutTx(c *C) {
	con, fs := getConnFakeServer()
	con.Close()
	c.Assert(contains(fs.Received(), "sROLLBACK;"), Equals, false)
}

func (d *DRIVER) TestCloseDisconnects(c *C) {
	con, fs := getConnFakeServer()
	c.Assert(fs.DisconnectHasBeenCalled(), Equals, false)
	con.Close()
	c.Assert(fs.DisconnectHasBeenCalled(), Equals, true)
}

func (d *DRIVER) TestClosePipesError(c *C) {
	con, _ := getConnFakeServerWithError()
	c.Assert(con.Close(), Equals, TestErr)
}

func (d *DRIVER) TestCommitIsReceived(c *C) {
	con, fs := getConnFakeServer()
	tx, _ := con.Begin()
	tx.Commit()
	c.Assert(contains(fs.Received(), "sCOMMIT;"), Equals, true)
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
	rr := r.(*result)
	liid, e1 := r.LastInsertId()
	c.Assert(liid, Equals, rr.liid)
	c.Assert(e1, Equals, rr.err)
	ra, e2 := r.RowsAffected()
	c.Assert(ra, Equals, rr.ra)
	c.Assert(e2, Equals, rr.err)
}

func (d *DRIVER) TestConvertFloat64(c *C) {
	var any float64 = 1.2
	s, err := monetize(any)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "1.2")
}

func (d *DRIVER) TestGoifyFloat64(c *C) {
	any := "12e-200"
	s, err := goify(any, FLOAT)
	c.Assert(err, IsNil)
	c.Assert(s.(float64), Equals, float64(12e-200))
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
	b := "'a\\\\b\\'c\\''"
	s, err := goify(b, BLOB)
	c.Assert(err, IsNil)
	c.Assert(s.([]byte), DeepEquals, []byte("a\\b'c'"))
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
	t := "'" + TimeLayout + "'"
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
	c.Assert(qry, Equals, "query with monetized time='2013-02-11 00:00:00'")
}

func (d *DRIVER)TestStmtExecCallsConnCmd(c *C){
	co, sr := getConnFakeServer()
	s := newStmt(co, "time=%s")
	exact := time.Date(2013, time.February, 11, 0, 0, 0, 0, time.UTC)
	s.Exec([]driver.Value{exact})
	c.Assert(sr.(*fsrv).received[0], Equals, "stime='2013-02-11 00:00:00';")
}
