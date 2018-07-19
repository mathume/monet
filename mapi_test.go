package monet

import (
	"encoding/binary"
	. "gopkg.in/check.v1"
	"net"
	"errors"
	"strings"
	"testing"
	"time"
)

const (
	alwaysUpTcp     = "google.com:80"
	validDatabase   = "test"
	validChallenge  = "a3LRYd2Gu79jniO:merovingian:9:RIPEMD160,SHA256,SHA1,MD5:LIT:SHA512:"
	validResponse   = "BIG::{SHA1}d67566a2f80453a95f9dbec976351bc6345192a1:::"
	validUser       = "monetdb"
	correctPassword = "monetdb"
	hostname        = "localhost"
	validLanguage   = "sql"
)

var (
	logWriter *writer
)

func Test(t *testing.T) { TestingT(t) }

type MAPISERVER struct{}

func (m *MAPISERVER) SetUpSuite(c *C) {
	logWriter = new(writer)
}

func (m *MAPISERVER) SetUpTest(c *C) {
	logWriter.Clear()
}

var _ = Suite(&MAPISERVER{})

func (s *MAPISERVER) TestCorrectConnection(c *C) {
	srv := server{*new(conn), c_STATE_INIT, nil, nil, logWriter}
	err := srv.connect(c_NET, alwaysUpTcp, time.Second)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(logWriter.Msg, "Connection succeeded"), Equals, true)
}

func (s *MAPISERVER) TestCreateServer(c *C) {
	srv := CreateServer(logWriter)
	c.Assert(srv, NotNil)
	c.Assert(strings.Contains(logWriter.Msg, "Server initialized"), Equals, true)
}

func (s *MAPISERVER) TestChallenge_Response(c *C) {
	srv := server{*new(conn), c_STATE_INIT, nil, nil, logWriter}
	response := srv.challenge_response(validChallenge)
	c.Assert(response, Equals, validResponse)
}

func (s *MAPISERVER) TestContains(c *C) {
	st := []string{"RIPEMD160", "SHA256", "SHA1", "MD5"}
	if contains(st, "") {
		c.Fatal("Found empty string")
	}
	if !contains(st, "SHA256") {
		c.Fatal("Didn't find SHA256")
	}
}

func (s *MAPISERVER) TestGetBytes(c *C) {
	srv := server{*new(conn), c_STATE_INIT, nil, nil, logWriter}
	bytesToSend := []byte("01")

	fconn := newFakeConn()
	fconn.Write(bytesToSend)
	srv.conn.netConn = fconn.(net.Conn)
	bb, err := srv.getbytes(2)
	c.Assert(err, IsNil)
	c.Assert(bb[0], Equals, bytesToSend[0])
	c.Assert(bb[1], Equals, bytesToSend[1])
}

func (s *MAPISERVER) TestGetBlockShort(c *C) {
	msg := []byte("hello")
	length, last := len(msg), 1
	flag := make([]byte, 2)
	i_flag := uint16((length << 1) + last)
	binary.LittleEndian.PutUint16(flag, i_flag)
	send := append(flag, msg...)
	con := new(conn)
	con.netConn = newFakeConn().(net.Conn)
	srv := server{*con, c_STATE_INIT, nil, nil, logWriter}
	n, err := srv.conn.netConn.Write(send)
	c.Assert(n, Equals, len(send))
	r, err := srv.getblock()
	c.Log(logWriter.Msg)
	c.Assert(err, IsNil)
	c.Assert(r, Equals, string(msg))
}

func (s *MAPISERVER) TestFakeConn(c *C) {
	//create
	f := newFakeConn()
	c.Assert(f.Closed(), Equals, false)
	c.Assert(f.ReturnsErrorOnClose(), Equals, false)
	//send
	msg := []byte("hello")
	n, err := f.Write(msg)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, len(msg))
	c.Log(logWriter.Msg)
	act, exp := string(f.Received()), string(msg)
	c.Assert(act, Equals, exp)
	//receive
	bytes := make([]byte, len(msg)+2)
	n, err = f.Read(bytes)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, len(msg))
	c.Assert(string(bytes[:n]), Equals, string(msg))
	//close
	f.ReturnErrorOnClose(true)
	c.Assert(f.ReturnsErrorOnClose(), Equals, true)
	err = f.Close()
	c.Assert(err, NotNil)
	c.Assert(f.Closed(), Equals, true)
}

func (s *MAPISERVER) TestMax(c *C) {
	c.Assert(1, Equals, max(1, -1))
}

func (s *MAPISERVER) TestStringComparison(c *C) {
	s1, s2 := "hello", "hello"
	c.Assert(s1, Equals, s2)
}

func (s *MAPISERVER) TestGetBlockLong(c *C) {
	msg1, msg2 := []byte("kaixo"), []byte("mundua")
	length1, length2 := len(msg1), len(msg2)
	flag1, flag2 := make([]byte, 2), make([]byte, 2)
	i_flag1 := uint16((length1 << 1) + 0)
	i_flag2 := uint16((length2 << 1) + 1)
	binary.LittleEndian.PutUint16(flag1, i_flag1)
	binary.LittleEndian.PutUint16(flag2, i_flag2)
	send := append(flag1, msg1...)
	send = append(send, flag2...)
	send = append(send, msg2...)
	con := new(conn)
	con.netConn = newFakeConn().(net.Conn)
	srv := server{*con, c_STATE_INIT, nil, nil, logWriter}
	n, err := srv.conn.netConn.Write(send)
	c.Assert(n, Equals, len(send))

	r, err := srv.getblock()
	c.Log(logWriter.Msg)
	c.Assert(err, IsNil)
	c.Assert(r, Equals, string(msg1)+string(msg2))
}

func (s *MAPISERVER) TestPutBlockShort(c *C) {
	msg := []byte("hello")
	length := len(msg)
	flag := make([]byte, 2)
	i_flag := uint16((length << 1) + 1)
	binary.LittleEndian.PutUint16(flag, i_flag)
	expected := append(flag, msg...)
	conn := new(conn)
	srv := server{*conn, c_STATE_INIT, nil, nil, logWriter}
	fconn := newFakeConn()
	srv.conn.netConn = fconn.(net.Conn)
	err := srv.putblock(msg)
	c.Assert(err, IsNil)
	act, exp := string(fconn.Received()), string(expected)
	c.Assert(act, Equals, exp)
}

func (s *MAPISERVER) TestDisconnect(c *C) {
	conn := new(conn)
	srv := server{*conn, c_STATE_INIT, nil, nil, logWriter}
	fconn := newFakeConn()
	fconn.ReturnErrorOnClose(true)
	srv.conn.netConn = fconn.(net.Conn)
	srv.conn.netConn.Close()
	err := srv.Disconnect()
	c.Assert(err, NotNil)
	c.Assert(srv.state, Equals, c_STATE_INIT)

}

func (s *MAPISERVER) TestCmd(c *C) {
	conn := new(conn)
	//check state
	srv := server{*conn, c_STATE_INIT, nil, nil, logWriter}
	_, err := srv.Cmd("anyCommand")
	c.Assert(strings.Contains(err.Error(), "not ready"), Equals, true)
	srv.state = c_STATE_READY
	//no response no error
	fconn := newFakeConn()
	srv.conn.netConn = fconn.(net.Conn)
	willBePutToFakeConn := ""
	response, err := srv.Cmd(willBePutToFakeConn)
	c.Assert(err, IsNil)
	c.Assert(response, Equals, "")
	//non empty response no error
	well := []string{c_MSG_Q, c_MSG_HEADER, c_MSG_TUPLE}
	for _,v := range well {
		willBePutToFakeConn = string(v) + "anyTestResponse"
		fconn = newFakeConn()
		response, err = srv.Cmd(willBePutToFakeConn)
		c.Assert(err, IsNil)
		c.Assert(response, Equals, willBePutToFakeConn)
	}
	//response error message
	expErrMsg := "expected error message"
	willBePutToFakeConn = string(c_MSG_ERROR) + expErrMsg
	fconn = newFakeConn()
	response, err = srv.Cmd(willBePutToFakeConn)
	c.Assert(err, Not(IsNil))
	c.Assert(strings.Contains(err.Error(), expErrMsg), Equals, true)

}

type writer struct {
	Msg string
}

func (w *writer) Clear() {
	w.Msg = ""
	return
}

func (w *writer) Write(p []byte) (n int, err error) {
	w.Msg += string(p)
	n, err = len(p), nil
	return
}

func (w *writer) Debug(m string)(err error){
	_, err = w.Write([]byte(m))
	return
}

func (w *writer) Info(m string)(err error){
	_, err = w.Write([]byte(m))
	return
}

func (w *writer) Warning(m string)(err error){
	_, err = w.Write([]byte(m))
	return
}

func (w *writer) Err(m string)(err error){
	_, err = w.Write([]byte(m))
	return
}

type FakeConn interface {
	net.Conn
	Received() []byte
	Closed() bool
	ReturnsErrorOnClose() bool
	ReturnErrorOnClose(b bool)
}

func newFakeConn() FakeConn {
	fc := new(fakeConn)
	return fc
}

type fakeConn struct {
	received           []byte
	closed             bool
	returnErrorOnClose bool
}

func (f *fakeConn) ReturnErrorOnClose(b bool) {
	f.returnErrorOnClose = b
	return
}

func (f *fakeConn) ReturnsErrorOnClose() bool {
	return f.returnErrorOnClose
}

func (f *fakeConn) Closed() bool {
	return f.closed
}

func (f *fakeConn) Received() []byte {
	return f.received
}

func (f *fakeConn) Read(bytes []byte) (n int, err error) {
	n = copy(bytes, f.received)
	f.received = f.received[n:]
	return
}

func (f *fakeConn) Write(bytes []byte) (n int, err error) {
	f.received = append(f.received, bytes...)
	n = len(bytes)
	return
}

func (f *fakeConn) LocalAddr() (laddr net.Addr)              { return }
func (f *fakeConn) RemoteAddr() (raddr net.Addr)             { return }
func (f *fakeConn) SetDeadline(t time.Time) (err error)      { return }
func (f *fakeConn) SetWriteDeadline(t time.Time) (err error) { return }
func (f *fakeConn) SetReadDeadline(t time.Time) (err error)  { return }
func (f *fakeConn) Close() (err error) {
	f.closed = true
	if f.returnErrorOnClose {
		err = errors.New("error at close")
	}
	return
}
