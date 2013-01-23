package monet

import (
	. "launchpad.net/gocheck"
	"log"
)

type DRIVER struct{}

func (d *DRIVER) SetUpSuite(c *C) {
	logger = new(writer)
	Logger = log.New(logger, "driver_test ", log.LstdFlags)
}

func (d *DRIVER) SetUpTest(c *C) {
	logger.Clear()
}

func (d *DRIVER)TestBeginSendsStartTransaction(c *C){
	con := new(mconn)
	con.srv = fakeServer
	//setup
	con.Begin()
	c.Assert(fakeServer.Received(), Equals, "sSTART TRANSACTION;")
}

func (d *DRIVER)TestBeginCanOnlyBeCalledOnce(c *C){
	con := new(mconn)
	con.srv = fakeServer
	//setup
	tx, err := con.Begin()
	c.Assert(err, IsNil)
	c.Assert(tx, Not(IsNil))
	tx, err = con.Begin()
	c.Assert(tx, IsNil)
	c.Assert(err, Not(IsNil))
}

func (d *DRIVER)TestCloseWillRollbackWhenThereIsATx(c *C){
	con := new(mconn)
	con.srv = fakeServer
	//setup
	con.Begin()
	con.Close()
	c.Assert(contains(fakeServer.Received(), "sROLLBACK;"), Equals, true)
}

func (d *DRIVER)TestCloseWillNotRollbackWithoutTx(c *C){
	con := new(mconn)
	con.srv = fakeServer
	//setup
	c.Close()
	c.Assert(contains(fakeServer.Received(), "sROLLBACK;"), Equals, false)
}