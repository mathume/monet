package drv_test

import (
	"testing"
	"database/sql"
	"log"
	"time"
	"monet"
	. "monet/drv"
	. "launchpad.net/gocheck"
)

var logger = new(monet.Writer)

func Test(t *testing.T){ TestingT(t) }

type DRIVER struct{}

var _ = Suite(&DRIVER{})

func (d *DRIVER)SetUpSuite(c *C){
	Logger = log.New(logger, "drv test ", log.LstdFlags)
	monet.Logger = log.New(logger, "monet logger in drv test ", log.LstdFlags)
}

func (d *DRIVER)TestSQLDoesntCallOpenOnOpen(c *C){
	_, err := sql.Open(DRV_NAME, "anything")
	c.Assert(err, IsNil)
}

func (d *DRIVER)TestErrorAtBadConnString(c *C){
	dr, _ := sql.Open(DRV_NAME, "bad:connection:string")
	
	res, err := dr.Exec("anyQuery")//triggers call to driver.Open
	c.Log(res)
	c.Log(logger.Msg)
	c.Assert(err, Equals, ErrConnString)
}

func (d *DRIVER)TestErrorAtBadConnString1(c *C){
	dr, _ := sql.Open(DRV_NAME, ConnectionString("localhost", "50000", "monetdb", "monetdb", "voc", 5*time.Second))
	
	res, err := dr.Exec("select count(*) from voc.craftsmen")
	c.Assert(err, IsNil)
	c.Assert(res, Not(IsNil))
}
