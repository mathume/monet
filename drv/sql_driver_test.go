package drv

import (
	"testing"
	"database/sql"
	"log"
	"time"
//	"io"
	"monet"
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
	dr, _ := new(MDriver).Open(validConnString)
	s, err := dr.Prepare("anyQuery")
	c.Log(s)
	c.Log(err)
	c.Error("not finished yet")	
}
/*

func (d *DRIVER)TestMTX(c *C){
	t := new (MTX)
	t.c = new(fakeConn)
	t.Commit()
	c.Assert(t.query, Equals, "COMMIT")
	t.Rollback()
	c.Assert(t.query, Equals, "ROLLBACK")
}

func (d *DRIVER)TestMRows(c *C){
	row1 := []int64{1,2}
	row2 := []int64{3,4}
	rows := [][]int64{row1, row2}
	rr := &MRows{nil, rows, -1, []string{"col1", "col2"}, "anyId", 0}
	c.Assert(rr.Columns()[1], Equals, "col2")
	dest := make([]int64) //too short
	err := rr.Next(dest)
	c.Assert(err, Not(IsNil))
	dest := make([]int64, len(rr.Columns()))
	err := rr.Next(dest)
	c.Assert(err, IsNil)
	c.Assert(dest[0], Equals, 1)
	err = rr.Next(dest)
	c.Assert(err, IsNil)
	c.Assert(dest[1], Equals, 3)
	err = rr.Next(dest)
	c.Assert(err, Equals, io.EOF)
	err = nil
	err = rr.Next(dest)
	c.Assert(err, Equals, io.EOF)
}
*/
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

type fakeConn struct{
	MConn
	query string
}

func (c *fakeConn)exec(query string){
	c.query = query
}

type LIVE struct{}

var _ = Suite(&LIVE{})

func (l *LIVE)TestErrorAtBadConnString1(c *C){
	dr, _ := sql.Open(DRV_NAME, validConnString)

	_, err := dr.Exec("select count(*) from voc.craftsmen")
	c.Assert(err, Not(Equals), ErrConnString)
}
