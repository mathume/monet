package monet

import (
	"echo"
	"encoding/binary"
	"errors"
	. "launchpad.net/gocheck"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

var (
	logger    *writer
	testsrv   *echo.TestSrv
	validPort = echo.TEST_PORT
)

const (
	validDatabase   = "test"
	validChallenge  = "a3LRYd2Gu79jniO:merovingian:9:RIPEMD160,SHA256,SHA1,MD5:LIT:SHA512:"
	validResponse   = "BIG::{SHA1}d67566a2f80453a95f9dbec976351bc6345192a1:::"
	validUser       = "monetdb"
	correctPassword = "monetdb"
	hostname        = "localhost"
	validLanguage   = "sql"
)

func Test(t *testing.T) { TestingT(t) }

type MAPI struct{}

func (m *MAPI) SetUpSuite(c *C) {
	logger = new(writer)
	Logger = *log.New(logger, "monet_test ", log.LstdFlags)
	testsrv = echo.NewTestSrv()
	go func() {
		testsrv.Start()
	}()
	time.Sleep(time.Second)
}

func (m *MAPI) SetUpTest(c *C) {
	logger.Clear()
}

var _ = Suite(&MAPI{})

func (s *MAPI) TestNewServer(c *C) {
	srv := NewServer()
	c.Assert(srv, NotNil)
	c.Assert(strings.Contains(logger.Msg, "Server initialized"), Equals, true)
}

func (s *MAPI) TestCorrectConnection(c *C) {
	srv := server{*new(conn), STATE_INIT, nil, nil}
	err := srv.connect(NET, hostname+validPort, time.Second)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(logger.Msg, "Connection succeeded"), Equals, true)
}

func (s *MAPI) TestChallenge_Response(c *C) {
	srv := server{*new(conn), STATE_INIT, nil, nil}
	response := srv.challenge_response(validChallenge)
	c.Assert(response, Equals, validResponse)
}

func (s *MAPI) TestContains(c *C) {
	st := []string{"RIPEMD160", "SHA256", "SHA1", "MD5"}
	if contains(st, "") {
		c.Fatal("Found empty string")
	}
	if !contains(st, "SHA256") {
		c.Fatal("Didn't find SHA256")
	}
}

func (s *MAPI) TestGetBytes(c *C) {
	srv := server{*new(conn), STATE_INIT, nil, nil}
	bytesToSend := []byte("01")

	testsrv.Set(string(bytesToSend))
	err := srv.connect(NET, hostname+validPort, time.Second)
	if err != nil {
		c.Fatal(err)
	}
	bb, err := srv.getbytes(2)
	c.Assert(err, IsNil)
	c.Assert(bb[0], Equals, bytesToSend[0])
	c.Assert(bb[1], Equals, bytesToSend[1])
}

func (s *MAPI) TestGetBlockShort(c *C) {
	msg := []byte("hello")
	length, last := len(msg), 1
	flag := make([]byte, 2)
	i_flag := uint16((length << 1) + last)
	binary.LittleEndian.PutUint16(flag, i_flag)
	send := append(flag, msg...)
	msg_ := string(send)
	testsrv.Set(msg_)
	srv := server{*new(conn), STATE_INIT, nil, nil}
	err := srv.connect(NET, hostname+validPort, time.Second)
	if err != nil {
		c.Fatal(err)
	}
	r, err := srv.getblock()
	c.Log(logger.Msg)
	c.Assert(err, IsNil)
	c.Assert(r, Equals, string(msg))
}

func (s *MAPI) TestGetBlockLong(c *C) {
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
	msg_ := string(send)
	testsrv.Set(msg_)
	srv := server{*new(conn), STATE_INIT, nil, nil}
	err := srv.connect(NET, hostname+validPort, time.Second)
	if err != nil {
		c.Fatal(err)
	}
	r, err := srv.getblock()
	c.Log(logger.Msg)
	c.Assert(err, IsNil)
	c.Assert(r, Equals, string(msg1)+string(msg2))
}

func (s *MAPI) TestPutBlockShort(c *C) {
	msg := []byte("hello")
	length := len(msg)
	flag := make([]byte, 2)
	i_flag := uint16((length << 1) + 1)
	binary.LittleEndian.PutUint16(flag, i_flag)
	expected := append(flag, msg...)
	conn := new(conn)
	srv := server{*conn, STATE_INIT, nil, nil}
	err := srv.connect(NET, hostname+validPort, time.Second)
	if err != nil {
		c.Fatal(err)
	}
	fconn := NewFakeConn()
	srv.conn.netConn = fconn.(net.Conn)
	err = srv.putblock(msg)
	act, exp := string(fconn.Received()), string(expected)
	c.Assert(act, Equals, exp)
}

func (s *MAPI) TestDisconnect(c *C) {
	conn := new(conn)
	srv := server{*conn, STATE_INIT, nil, nil}
	err := srv.connect(NET, hostname+validPort, time.Second)
	srv.state = STATE_READY
	if err != nil {
		c.Fatal(err)
	}
	fconn := NewFakeConn()
	fconn.ReturnErrorOnClose(true)
	srv.conn.netConn = fconn.(net.Conn)
	srv.conn.netConn.Close()
	err = srv.Disconnect()
	c.Assert(err, NotNil)
	c.Assert(srv.state, Equals, STATE_INIT)

}

func (s *MAPI) TestStringComparison(c *C) {
	s1, s2 := "hello", "hello"
	c.Assert(s1, Equals, s2)
}

func (s *MAPI) TestMax(c *C) {
	c.Assert(1, Equals, max(1, -1))
}

func (s *MAPI) TestFakeConn(c *C) {
	f := NewFakeConn()
	c.Assert(f.Closed(), Equals, false)
	c.Assert(f.ReturnsErrorOnClose(), Equals, false)
	msg := []byte("hello")
	n, err := f.Write(msg)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, len(msg))
	act, exp := string(f.Received()), string(msg)
	c.Assert(act, Equals, exp)
	f.ReturnErrorOnClose(true)
	c.Assert(f.ReturnsErrorOnClose(), Equals, true)
	err = f.Close()
	c.Assert(err, NotNil)
	c.Assert(f.Closed(), Equals, true)
}

type FakeConn interface {
	net.Conn
	Received() []byte
	Closed() bool
	ReturnsErrorOnClose() bool
	ReturnErrorOnClose(b bool)
}

func NewFakeConn() FakeConn {
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

func (f *fakeConn) Read([]byte) (n int, err error) {
	err = errors.New("Read not impl")
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

type LIVE struct{}

var _ = Suite(&LIVE{})

func (s *LIVE)TestLiveConnectDisconnect(c *C){
	srv := NewServer()
	err := srv.Connect("localhost", ":50000", "monetdb", "monetdb", "voc", "sql", time.Second*10)
	c.Assert(err, IsNil)
	err = srv.Disconnect()
	c.Assert(err, IsNil)
	//c.Error(logger.Msg)
}

