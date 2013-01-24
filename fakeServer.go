package monet

import (
	"time"
)

type fakeServer interface {
	Server
	Received() []string
}

func newFakeServer() fakeServer {
	return new(fsrv)
}

type fsrv struct {
	received  []string
	err       error
	response  string
	connected bool
	conn
}

func (fs *fsrv) Cmd(operation string) (string, error) {
	fs.received = append(fs.received, operation)
	return fs.response, fs.err
}

func (fs *fsrv) Connect(hostname, port, username, password, database, language string, timeout time.Duration) error {
	fs.hostname = hostname
	fs.port = port
	fs.username = username
	fs.password = password
	fs.database = database
	fs.language = language
	return fs.err
}

func (fs *fsrv) Disconnect() error {
	fs.connected = false
	return fs.err
}

func (fs *fsrv) Received() []string {
	return fs.received
}
