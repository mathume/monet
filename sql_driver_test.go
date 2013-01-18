package monet

import (
	"database/sql"
	"log"
	"time"
	"errors"
	. "launchpad.net/gocheck"
)

var (
	validConnString = ConnectionString("localhost", "50000", "monetdb", "monetdb", "voc", 5*time.Second)
)

type DRIVER struct{}

var _ = Suite(&DRIVER{})

func (d *DRIVER)SetUpSuite(c *C){
	Logger = log.New(logger, "drv test ", log.LstdFlags)
	Logger = log.New(logger, "monet logger in drv test ", log.LstdFlags)
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

func (d *DRIVER)TestStrip(c *C){
	s := "a b\tc\rd\ne \t\r\nf"
	ss := SplitWS(s)
	s = ""
	for _,v := range ss {
		s += string(v)
		c.Log(string(v))
	}
	c.Log(s)
	c.Assert(len(ss), Equals, 6)
	c.Assert(s, Equals, "abcdef");
}

func (d *DRIVER)TestLastInsertId(c *C){
	r := createMResult(new(fakeMConn).(MConn))
	r.lid = 1
	err := errors.New("any")
	l, e := r.LastInsertId()
	c.Assert(l, Equals, 1)
	c.Assert(l, Matches, "any")
}

type DRVLIVE struct{}

var _ = Suite(&DRVLIVE{})

func (l *DRVLIVE)TestErrorAtBadConnString1(c *C){
	dr, _ := sql.Open(DRV_NAME, validConnString)

	_, err := dr.Exec("select count(*) from voc.craftsmen")
	c.Assert(err, Not(Equals), ErrConnString)
}
