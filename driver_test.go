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

func (d *DRIVER) TestBeginSendsStartTransaction(c *C) {
	con, fs := getConnFakeServer()
	con.Begin()
	c.Assert(fs.Received(), Equals, "sSTART TRANSACTION;")
}

func (d *DRIVER) TestBeginCanOnlyBeCalledOnce(c *C) {
	con, _ := getConnFakeServer()
	tx, err := con.Begin()
	c.Assert(err, IsNil)
	c.Assert(tx, Not(IsNil))
	tx, err = con.Begin()
	c.Assert(tx, IsNil)
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
