package monet

import (
	"database/sql"
	"database/sql/driver"
	"strings"
	"errors"
	"time"
)

const (
	SEP = ";"
	DRV_NAME = "monet"
)

func init(){
	sql.Register(DRV_NAME, &mdriver{})
}

func ConnectionString(hostname, port, username, password, database string, timeout time.Duration) string {
	return strings.Join([]string{hostname, port, username, password, database, timeout.String()}, SEP)
}

type mdriver struct {}

func (d *mdriver)Open(ConnectionString string)(driver.Conn, error){
	c := new(mconn)
	c.srv = NewServer()
	s := strings.Split(ConnectionString, SEP)
	if len(s) != 6 {
		return nil, errors.New("Wrong connection string.")
	}
	dur, err := time.ParseDuration(s[5])
	if err != nil {
		return nil, err
	}
	err = c.srv.Connect(s[0], ":"+s[1], s[2], s[3], s[4], "sql", dur)
	return c, err
}
