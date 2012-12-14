package drv

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"log"
	"monet"
	"os"
	"strings"
	"time"
)

const (
	SEP      = "::::" //separator for connection string fields
	DRV_NAME = "monet"
)

var (
	ErrConnString             = errors.New("The given connection string wasn't valid.")
	Logger        *log.Logger = log.New(os.Stdout, "monet/drv ", log.LstdFlags)
)

func init() {
	sql.Register(DRV_NAME, &MDriver{})
}

func ConnectionString(hostname, port, username, password, database string, timeout time.Duration) string {
	return strings.Join([]string{hostname, port, username, password, database, timeout.String()}, SEP)
}

type MDriver struct{
}

func (d *MDriver) Open(monetConnectionString string) (driver.Conn, error) {
	c := new(MConn)
	c.srv = monet.NewServer()
	s := strings.Split(monetConnectionString, SEP)
	Logger.Println("written")
	Logger.Println(monetConnectionString)
	if len(s) != 6 {
		return nil, ErrConnString
	}
	dur, err := time.ParseDuration(s[5])
	if err != nil {
		return nil, err
	}
	err = c.srv.Connect(s[0], ":" + s[1], s[2], s[3], s[4], "sql", dur)
	return c, err
}

type MConn struct {
	srv monet.Server
}

func (c *MConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("Prepare: not impl")
}

func (c *MConn) Close() error {
	return errors.New("Close: not impl")
}

func (c *MConn) Begin() (driver.Tx, error) {
	return nil, errors.New("Begin: not impl")
}
