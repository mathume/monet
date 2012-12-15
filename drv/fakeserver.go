package drv

import (
	"monet"
	"time"
	"net"
)

type fakeserver struct{
	cs string
	cr string
	dc bool
	conn
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

func NewFakeServer() monet.Server{
	return new(fakeserver)
}

func (fs *fakeserver)Connect(hostname, port, username, password, database, language string, timeout time.Duration) error{
	fs.setConn(hostname, port, username, password, database, language)
	return nil
}

func (fs *fakeserver) setConn(hostname, port, username, password, database, language string) {
	fs.hostname = hostname
	fs.port = port
	fs.username = username
	fs.password = password
	fs.database = database
	fs.language = language
	fs.netConn = monet.NewFakeConn().(net.Conn)
	return
}

func (fs *fakeserver)Cmd(operation string) (response string, err error){
	fs.cr = operation
	return
}

func (fs *fakeserver)Disconnect() error{
	fs.dc = true
	return nil
}