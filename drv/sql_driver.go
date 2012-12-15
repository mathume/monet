package drv

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"monet"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	SEP                 = "::::" //separator for connection string fields
	DRV_NAME            = "monet"
	ROWS_BLOCK_SIZE     = 100
	LAST_INSERT_ID_NONE = -1
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
	return &MStmt{query, c, ni, false}, nil
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
	if c.srv == nil {
		return "", driver.ErrBadConn
	}
	return c.srv.Cmd("s" + command + ";")
}

func (c *MConn) cmd(operation string) (string, error) {
	if c.srv == nil {
		return "", driver.ErrBadConn
	}
	return c.srv.Cmd(operation)
}

func (c *MConn) sendReplySize() error {
	_, err := c.cmd("Xreply_size " + string(ROWS_BLOCK_SIZE))
	return err
}

type MStmt struct {
	query  string
	conn   *MConn
	ni     int
	closed bool
}

func (s *MStmt) Close() error {
	if s.closed {
		return nil
	}
	s.conn = nil
	s.closed = true
	return nil
}

func (s *MStmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.isValid(args); err != nil {
		return nil, err
	}
	r := createMResult(s.conn)
	err := r.exec(s.bind(args))
	return &r, err
}

func (s *MStmt) isValid(args []driver.Value) error {
	if len(args) != s.ni {
		return ErrNumInput
	}
	if s.conn == nil {
		return driver.ErrBadConn
	}
	return nil
}

func (s *MStmt) bind(args []driver.Value) string {
	return fmt.Sprintf(s.query, args)
}

func (s *MStmt) NumInput() int {
	return s.ni
}

func (s *MStmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.isValid(args); err != nil {
		return nil, err
	}
	r := createMRows(s.conn)
	err := r.query(s.bind(args))
	return &r, err
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

type MResult struct {
	conn *MConn
	lid  int64
	ra   int64
}

func createMResult(conn *MConn) MResult {
	return MResult{conn, LAST_INSERT_ID_NONE, 0}
}

func (r *MResult) exec(operation string) error {
	if r.conn == nil {
		return driver.ErrBadConn
	}
	if err := r.conn.sendReplySize(); err != nil {
		return err
	}
	res, err := r.conn.exec(operation)
	if err != nil {
		return err
	}
	lines := strings.Split(res, "\n")
	first := lines[0]

	for strings.HasPrefix(first, monet.MSG_INFO) {
		lines = lines[1:]
		first = lines[0]
	}

	if strings.HasPrefix(res, monet.MSG_QUPDATE) {
		sai := SplitWS(first[2:])
		r.ra, err = strconv.ParseInt(sai[0], 10, 64)
		r.lid, err = strconv.ParseInt(sai[1], 10, 64)
		if err != nil {
			return err
		}
		return nil
	}

	if strings.HasPrefix(res, monet.MSG_QSCHEMA) {
		return nil
	}

	if strings.HasPrefix(res, monet.MSG_QTRANS) {
		return nil
	}

	if strings.HasPrefix(res, monet.MSG_PROMPT) {
		return nil
	}

	if strings.HasPrefix(res, monet.MSG_ERROR) {
		return errors.New(first[1:])
	}

	return errors.New("Unknown state " + first)
}

func (r *MResult) LastInsertId() (int64, error) {
	return r.lid, nil
}

func (r *MResult) RowsAffected() (int64, error) {
	return r.ra, nil
}

type MRows struct {
	conn     *MConn
	rows     [][]driver.Value
	curr     int
	cols     []string
	query_id string
	offset   int
}

func createMRows(conn *MConn) MRows {
	return MRows{conn, nil, -1, *new([]string), "", 0}
}

func (r *MRows) query(operation string) error {
	if r.conn == nil {
		return driver.ErrBadConn
	}
	if err := r.conn.sendReplySize(); err != nil {
		return err
	}
	res, err := r.conn.exec(operation)
	if err != nil {
		return err
	}
	lines := strings.Split(res, "\n")
	first := lines[0]

	for strings.HasPrefix(first, monet.MSG_INFO) {
		lines = lines[1:]
		first = lines[0]
	}

	if strings.HasPrefix(first, monet.MSG_QTABLE){
		meta := SplitWS(first[2:])
		
	}
	return errors.New("Unknown state " + first)
}

func (r *MRows) Close() error {
	if r.conn != nil{
		r.conn = nil
	}
	return nil
}

func (r *MRows) Columns() []string {
	return r.cols
}

func (r *MRows) Next(dest []driver.Value) error {
	r.curr++
	if r.curr > len(r.cols) {
		return io.EOF
	}
	if len(dest) < len(r.cols) {
		return errors.New("Not enough space in dest slice.")
	}
	copy(dest, r.rows[r.curr])
	return nil
}

func SplitWS(s string) []string{
	res := make([]string, 0)
	if s == "" {
		return res
	}
	s = strings.Replace(s, "\t", " ", -1)
	s = strings.Replace(s, "\n", " ", -1)
	s = strings.Replace(s, "\r", " ", -1)
	ss := strings.Split(s, " ")
	for _,s := range ss {
		if strings.Trim(s, " ") != "" {
			res = append(res, s)
		}
	}
	return res
}