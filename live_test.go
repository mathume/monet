package monet

import (
	"database/sql"
	"database/sql/driver"
	. "gopkg.in/check.v1"
	"time"
)

type LIVE struct{}

var _ = Suite(&LIVE{})

func (s *LIVE) TestLiveConnectDisconnect(c *C) {
	srv := NewServer()
	err := srv.Connect("localhost", ":50000", "monetdb", "monetdb", "voc", "sql", time.Second*10)
	c.Assert(err, IsNil)
	err = srv.Disconnect()
	c.Assert(err, IsNil)
	//c.Error(logger.Msg)
}

func (s *LIVE) TestSQLExecAndQuery(c *C) {
	cs := ConnectionString("localhost", "50000", "monetdb", "monetdb", "voc", time.Second*10)
	db, err := sql.Open(DRV_NAME, cs)
	c.Assert(err, IsNil)
	_, err = db.Exec("DROP TABLE alltypes")
	c.Log(err)

	_, err = db.Exec("CREATE TABLE alltypes ( col1 BIGINT, col2 FLOAT, col3 TIMESTAMP, col4 VARCHAR(255), col5 CLOB, col6 BLOB )")
	c.Assert(err, IsNil)
	var v1 int64 = 12342524353465
	var v2 float64 = 124354e-100
	var v3 time.Time = time.Date(1982, time.April, 27, 22, 32, 0, 0, time.UTC)
	var v4 string = "kaixo mundua"
	var v5 []byte = []byte("kaixo mundua")
	var v6 = "100110"

	r, err := db.Exec("INSERT INTO alltypes VALUES(%s, %s, %s, %s, %s, %s)", v1, v2, v3, v4, v5, v6)
	c.Assert(err, IsNil)

	liid, err := r.LastInsertId()
	c.Assert(err, IsNil)
	c.Assert(liid, Equals, int64(-1))
	ra, err := r.RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(ra, Equals, int64(1))

	rows, err := db.Query("SELECT * FROM alltypes")
	c.Assert(err, IsNil)
	cols, err := rows.Columns()
	c.Assert(err, IsNil)
	for _, v := range cols {
		c.Log(v)
	}
	for rows.Next() {
		var vv1 int64
		var vv2 float64
		var vv3 time.Time
		var vv4 string
		var vv5 []byte
		var vv6 []byte
		err = rows.Scan(&vv1, &vv2, &vv3, &vv4, &vv5, &vv6)
		c.Assert(vv1, Equals, v1)
		c.Assert(vv2, Equals, v2)
		c.Assert(vv3.Equal(v3), Equals, true)
		c.Assert(vv4, Equals, v4)
		c.Assert(vv5, DeepEquals, v5)
		c.Assert(vv6, DeepEquals, []byte(v6))
	}
	c.Assert(rows.Err(), IsNil)
}

func (s *LIVE) TestExceedRowsSize(c *C) {
	cs := ConnectionString("localhost", "50000", "monetdb", "monetdb", "voc", time.Second*10)
	db, err := sql.Open(DRV_NAME, cs)
	c.Assert(err, IsNil)
	_, err = db.Exec("DROP TABLE alltypes")
	c.Log(err)

	_, err = db.Exec("CREATE TABLE alltypes ( col1 BIGINT, col2 FLOAT, col3 TIMESTAMP, col4 VARCHAR(255), col5 CLOB, col6 BLOB )")
	c.Assert(err, IsNil)
	var v1 int64 = 12342524353465
	var v2 float64 = 124354e-100
	var v3 time.Time = time.Date(1982, time.April, 27, 22, 32, 0, 0, time.UTC)
	var v4 string = "kaixo mundua"
	var v5 []byte = []byte("kaixo mundua")
	var v6 = "100110"

	for i := 0; i < RowsSize+3; i++ {
		_, err := db.Exec("INSERT INTO alltypes VALUES(%s, %s, %s, %s, %s, %s)", v1, v2, v3, v4, v5, v6)
		c.Assert(err, IsNil)
	}
	_, err = db.Exec("INSERT INTO alltypes VALUES(0, 1.2, %s, 'k', 'k', '10')", v3)
	c.Assert(err, IsNil)

	rows, err := db.Query("SELECT * FROM alltypes")
	c.Assert(err, IsNil)
	i := 0
	for rows.Next() {
		i++
		var vv1 int64
		var vv2 float64
		var vv3 time.Time
		var vv4 string
		var vv5 []byte
		var vv6 []byte
		c.Assert(rows.Scan(&vv1, &vv2, &vv3, &vv4, &vv5, &vv6), IsNil)
		c.Log([]driver.Value{vv1, vv2, vv3, vv4, vv5, vv6})
		if vv1 == 0 {
			c.Assert(i, Equals, 2+3+RowsSize)
		}
	}
	c.Assert(rows.Err(), IsNil)
	//c.Error()
}
