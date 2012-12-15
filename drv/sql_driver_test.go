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

var (
	logger = new(monet.Writer)
	validConnString = ConnectionString("localhost", "50000", "monetdb", "monetdb", "voc", 5*time.Second)
)

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

func (d *DRIVER)TestPrepare(c *C){
	dr, _ := Open(validConnString)
	s, err := dr.Prepare("anyQuery")
	c.Error("not finished yet")	
}

func (d *DRIVER)TestMTX(c *C){
	c.Error("not finished yet")
}


type LIVE struct{}

var _ = Suite(&LIVE{})

func (l *LIVE)TestErrorAtBadConnString1(c *C){
	dr, _ := sql.Open(DRV_NAME, defaultValidConnString)
	
	_, err := dr.Exec("select count(*) from voc.craftsmen")
	c.Assert(err, Not(Equals), ErrConnString)
}