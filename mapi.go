package monet

import (
	"errors"
	"log"
	"os"
	"net"
	"time"
)

var Logger log.Logger = *log.New(os.Stdout, "monetdb ", log.LstdFlags)
var LoginErr error = errors.New("Login failed.")

const (
	MAX_PACKAGE_LENGTH = (1024 * 8) - 2

	MSG_PROMPT   = ""
	MSG_INFO     = "#"
	MSG_ERROR    = "!"
	MSG_Q        = "&"
	MSG_QTABLE   = "&1"
	MSG_QUPDATE  = "&2"
	MSG_QSCHEMA  = "&3"
	MSG_QTRANS   = "&4"
	MSG_QPREPARE = "&5"
	MSG_QBLOCK   = "&6"
	MSG_HEADER   = "%"
	MSG_TUPLE    = "["
	MSG_REDIRECT = "^"

	STATE_INIT  = 0
	STATE_READY = 1

	NET = "tcp"
)

var notImplemented error = errors.New("Not implemented yet.")

type Server interface {
	Cmd(operation string) error
	Connect(hostname, port, username, password, database, language string, timeout time.Duration) error
	Disconnect() error
}

type server struct {
	conn
	state  int
	result interface{}
	socket interface{}
}

type conn struct {
	hostname string
	port     string
	username string
	password string
	database string
	language string
	netConn  net.Conn
}

func NewServer() Server {
	var c conn
	s := server{c, STATE_INIT, nil, nil}
	Logger.Println("Server initialized.")
	return &s
}

func (srv *server) Cmd(operation string) (err error) {
	err = notImplemented
	return
}

func (srv *server) Connect(hostname, port, username, password, database, language string, timeout time.Duration) (err error) {
	srv.setConn(hostname, port, username, password, database, language)
	srv.netConn, err = net.DialTimeout(NET, hostname+port, timeout)
	if err != nil {
		return
	}
	Logger.Println("Connection succeeded.")
	err = srv.login(0)
	return
}

func (srv *server) login(iteration int) (err error) {
	err = LoginErr
	return
}

func (srv *server) setConn(hostname, port, username, password, database, language string) {
	srv.hostname = hostname
	srv.port = port
	srv.username = username
	srv.password = password
	srv.database = database
	srv.language = language
	return
}

func (srv *server) Disconnect() (err error) {
	return
}
