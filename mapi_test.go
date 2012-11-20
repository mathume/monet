package monet

import (
	"echo"
	. "launchpad.net/gocheck"
	"log"
	"strings"
	"testing"
	"time"
)

var (
	logger  *writer
	testsrv *echo.TestSrv
	validPort = echo.TEST_PORT
)

const (
	validDatabase = "test"
	validChallenge = "a3LRYd2Gu79jniO:merovingian:9:RIPEMD160,SHA256,SHA1,MD5:LIT:SHA512:"
	validResponse = "BIG::{SHA1}d67566a2f80453a95f9dbec976351bc6345192a1:::"
	validUser = "monetdb"
	correctPassword = "monetdb"
	hostname = "localhost"
	validLanguage = "sql"
)

func Test(t *testing.T) { TestingT(t) }

type MAPI struct{}

func (m *MAPI) SetUpSuite(c *C) {
	logger = new(writer)
	Logger = *log.New(logger, "monet_test ", log.LstdFlags)
	testsrv = echo.NewTestSrv()
	go func() { testsrv.Start() }()
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
	srv := NewServer()
	anyDatabase  := ""
	err := srv.Connect(hostname, validPort, validUser, correctPassword, anyDatabase, validLanguage, time.Second)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(logger.Msg, "Connection succeeded"), Equals, true)
}

func (s *MAPI) TestWrongConnection(c *C) {
	srv := NewServer()
	anyDatabase := ""
	invalidhostname := "invalid"
	err := srv.Connect(invalidhostname, validPort, validUser, correctPassword, anyDatabase, validLanguage, time.Second)
	c.Assert(err, NotNil)
}

func (s *MAPI) TestLoginFails(c *C) {
	srv := NewServer()
	err := srv.Connect(hostname, validPort, validUser, correctPassword, validDatabase, validLanguage, time.Second)
	c.Assert(err, Equals, LoginErr)
}

func (s *MAPI) TestChallenge_Response(c *C){
	var cc conn
	srv := server{cc, STATE_INIT, nil, nil}
	response := srv.challenge_response(validChallenge)
	c.Assert(response, Equals, validResponse)
}

func (s *MAPI) TestContains(c *C){
	st := []string {"RIPEMD160","SHA256","SHA1","MD5"}
	if contains(st, "") { 
		c.Fatal("Found empty string")
	}
	if !contains(st, "SHA256") {
		c.Fatal("Didn't find SHA256")
	}
}