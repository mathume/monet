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
)

const (
	TESTDB = "test"
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
	validUser, correctPassword := "monetdb", "monetdb"
	anyDatabase, validLanguage := "", "en"
	hostname, port := "localhost", echo.TEST_PORT
	err := srv.Connect(hostname, port, validUser, correctPassword, anyDatabase, validLanguage, 0*time.Second)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(logger.Msg, "Connection succeeded"), Equals, true)
}

func (s *MAPI) TestWrongConnection(c *C) {
	srv := NewServer()
	validUser, correctPassword := "monetdb", "monetdb"
	anyDatabase, validLanguage := "", "en"
	hostname, port := "invalid", echo.TEST_PORT
	err := srv.Connect(hostname, port, validUser, correctPassword, anyDatabase, validLanguage, 0*time.Second)
	c.Assert(err, NotNil)
}

func (s *MAPI) TestLoginFails(c *C) {
	srv := NewServer()
	validUser, correctPW := "monetdb", "monetdb"
	hostname, port := "localhost", echo.TEST_PORT
	validDatabase, validLanguage := TESTDB, "en"
	err := srv.Connect(hostname, port, validUser, correctPW, validDatabase, validLanguage, 1*time.Second)
	c.Assert(err, Equals, LoginErr)
}
