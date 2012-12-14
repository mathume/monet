package monet

import (
	"encoding/binary"
	. "launchpad.net/gocheck"
	"log"
	"net"
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
	logger *writer
)

func Test(t *testing.T) { TestingT(t) }

type MAPISERVER struct{}

func (m *MAPISERVER) SetUpSuite(c *C) {
	logger = new(writer)
	Logger = log.New(logger, "monet_test ", log.LstdFlags)
}

func (m *MAPISERVER) SetUpTest(c *C) {
	logger.Clear()
}

var _ = Suite(&MAPISERVER{})

func (s *MAPISERVER) TestCorrectConnection(c *C) {
	srv := server{*new(conn), STATE_INIT, nil, nil, Logger}
	err := srv.connect(NET, alwaysUpTcp, time.Second)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(logger.Msg, "Connection succeeded"), Equals, true)
}

func (s *MAPISERVER) TestNewServer(c *C) {
	srv := NewServer()
	c.Assert(srv, NotNil)
	c.Assert(strings.Contains(logger.Msg, "Server initialized"), Equals, true)
}

func (s *MAPISERVER) TestChallenge_Response(c *C) {
	srv := server{*new(conn), STATE_INIT, nil, nil, Logger}
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
	srv := server{*new(conn), STATE_INIT, nil, nil, Logger}
	bytesToSend := []byte("01")

	fconn := NewFakeConn()
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
	Logger.Println("send", send)
	con := new(conn)
	con.netConn = NewFakeConn().(net.Conn)
	srv := server{*con, STATE_INIT, nil, nil, Logger}
	n, err := srv.conn.netConn.Write(send)
	c.Assert(n, Equals, len(send))
	r, err := srv.getblock()
	c.Log(logger.Msg)
	c.Assert(err, IsNil)
	c.Assert(r, Equals, string(msg))
}

func (s *MAPISERVER) TestFakeConn(c *C) {
	//create
	f := NewFakeConn()
	c.Assert(f.Closed(), Equals, false)
	c.Assert(f.ReturnsErrorOnClose(), Equals, false)
	//send
	msg := []byte("hello")
	n, err := f.Write(msg)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, len(msg))
	c.Log(logger.Msg)
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
	Logger.Println("send", send)
	con := new(conn)
	con.netConn = NewFakeConn().(net.Conn)
	srv := server{*con, STATE_INIT, nil, nil, Logger}
	n, err := srv.conn.netConn.Write(send)
	c.Assert(n, Equals, len(send))

	r, err := srv.getblock()
	c.Log(logger.Msg)
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
	srv := server{*conn, STATE_INIT, nil, nil, Logger}
	fconn := NewFakeConn()
	srv.conn.netConn = fconn.(net.Conn)
	err := srv.putblock(msg)
	c.Assert(err, IsNil)
	act, exp := string(fconn.Received()), string(expected)
	c.Assert(act, Equals, exp)
}

func (s *MAPISERVER) TestDisconnect(c *C) {
	conn := new(conn)
	srv := server{*conn, STATE_INIT, nil, nil, Logger}
	fconn := NewFakeConn()
	fconn.ReturnErrorOnClose(true)
	srv.conn.netConn = fconn.(net.Conn)
	srv.conn.netConn.Close()
	err := srv.Disconnect()
	c.Assert(err, NotNil)
	c.Assert(srv.state, Equals, STATE_INIT)

}

func (s *MAPISERVER) TestCmd(c *C) {
	conn := new(conn)
	//check state
	srv := server{*conn, STATE_INIT, nil, nil, Logger}
	_, err := srv.Cmd("anyCommand")
	c.Assert(strings.Contains(err.Error(), "not ready"), Equals, true)
	srv.state = STATE_READY
	//no response no error
	fconn := NewFakeConn()
	srv.conn.netConn = fconn.(net.Conn)
	willBePutToFakeConn := ""
	response, err := srv.Cmd(willBePutToFakeConn)
	c.Assert(err, IsNil)
	c.Assert(response, Equals, "")
	//non empty response no error
	well := []string{MSG_Q, MSG_HEADER, MSG_TUPLE}
	for _,v := range well {
		willBePutToFakeConn = string(v) + "anyTestResponse"
		fconn = NewFakeConn()
		response, err = srv.Cmd(willBePutToFakeConn)
		c.Assert(err, IsNil)
		c.Assert(response, Equals, willBePutToFakeConn)
	}
	//response error message
	expErrMsg := "expected error message"
	willBePutToFakeConn = string(MSG_ERROR) + expErrMsg
	fconn = NewFakeConn()
	response, err = srv.Cmd(willBePutToFakeConn)
	c.Assert(err, Not(IsNil))
	c.Assert(strings.Contains(err.Error(), expErrMsg), Equals, true)

}

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
