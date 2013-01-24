package monet

import (
	. "launchpad.net/gocheck"
	"log"
)

type DRIVER struct{}

var _ = Suite(&DRIVER{})

func (d *DRIVER) SetUpSuite(c *C) {
	logger = new(writer)
	Logger = log.New(logger, "driver_test ", log.LstdFlags)
}

func (d *DRIVER) SetUpTest(c *C) {
	logger.Clear()
}

func getConnFakeServer() (*mconn, fakeServer) {
	con := new(mconn)
	fs := newFakeServer()
	con.srv = fs.(Server)
	return con, fs
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

func (d *DRIVER) TestCommitIsReceived(c *C){
	con, fs := getConnFakeServer()
	t, _ := con.Begin()
	t.Commit()
	c.Assert(contains(fs.Received(), "sCOMMIT;"), Equals, true)
}

func (d *DRIVER) TestCanBeginAfterCommit(c *C){
	con, _ := getConnFakeServer()
	t, _ := con.Begin()
	t.Commit()
	_, err := con.Begin()
	c.Assert(err, IsNil)
}

func (d *DRIVER) TestCanBeginAfterRollback(c *C){
	con, _ := getConnFakeServer()
	t, _ := con.Begin()
	t.Rollback()
	_, err := con.Begin()
	c.Assert(err, IsNil)
}

func (d *DRIVER) TestCommitClears(c *C) {
	con, _ := getConnFakeServer()
	t, _ := con.Begin()
	t.Commit()
	c.Assert(con.t, IsNil)
}

func (d *DRIVER) TestRollbackClears(c *C) {
	con, _ := getConnFakeServer()
	t, _ := con.Begin()
	t.Rollback()
	c.Assert(con.t, IsNil)
}

func (d *DRIVER) TestClear(c *C) {
	t := new(tx)
	con := new(mconn)
	t.c = con
	t.c.t = t
	t.clear()
	c.Assert(t.c, IsNil)
	c.Assert(con.t, IsNil)
}
