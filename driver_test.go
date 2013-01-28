package monet

import (
	"errors"
	. "launchpad.net/gocheck"
	"log"
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