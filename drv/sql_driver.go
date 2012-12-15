package drv

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
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
	ErrNumInput               = errors.New("The number of arguments isn't equal to the number of placeholders.")
	Logger        *log.Logger = log.New(os.Stdout, "monet/drv ", log.LstdFlags)
)

func init() {
	sql.Register(DRV_NAME, &MDriver{})
}

func ConnectionString(hostname, port, username, password, database string, timeout time.Duration) string {
	return strings.Join([]string{hostname, port, username, password, database, timeout.String()}, SEP)
}

type MDriver struct {
}

func (d *MDriver) Open(MConnString string) (driver.Conn, error) {
	c := new(MConn)
	c.srv = monet.NewServer()
	s := strings.Split(MConnString, SEP)
	Logger.Println(MConnString)

	if len(s) != 6 {
		return nil, ErrConnString
	}
	dur, err := time.ParseDuration(s[5])
	if err != nil {
		return nil, err
	}
	err = c.srv.Connect(s[0], ":"+s[1], s[2], s[3], s[4], "sql", dur)
	return c, err
}

type MConn struct {
	srv monet.Server
}

func (c *MConn) Prepare(query string) (driver.Stmt, error) {
	ni := strings.Count(query, "%")
	return &MStmt{query, c.srv, ni, false}, nil
}

func (c *MConn) Close() error {
	err := c.srv.Disconnect()
	if err != nil {
		return err
	}
	c.srv = nil
	return nil
}

func (c *MConn) Begin() (driver.Tx, error) {
	t := new(MTX)
	t.c = c
	_, err := t.c.exec("START TRANSACTION")
	return t, err
}

func (c *MConn) exec(command string) (string, error) {
	return c.srv.Cmd("s" + command + ";")
}

type MStmt struct {
	query  string
	srv    monet.Server
	ni     int
	closed bool
}

func (s *MStmt) Close() error {
	return nil
}

func (s *MStmt) Exec(args []driver.Value) (driver.Result, error) {
	if len(args) != s.ni {
		return nil, ErrNumInput
	}
	
	cmd := s.bind(args)

	return nil, errors.New("stmt.exec not impl")
}

func (s *MStmt)bind(args []driver.Value) string{
	return fmt.Sprintf(s.query, args)
}

func (s *MStmt) NumInput() int {
	return s.ni
}

func (s *MStmt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) != s.ni {
		return nil, ErrNumInput
	}
	
	cmd := s.bind(args)
	
	return nil, errors.New("stmt.query not impl")
}

type MTX struct {
	c *MConn
}

func (t *MTX) Commit() error {
	_, err := t.c.exec("COMMIT")
	return err
}

func (t *MTX) Rollback() error {
	_, err := t.c.exec("ROLLBACK")
	return err
}

type MResult struct{
}

type MRows struct{
}