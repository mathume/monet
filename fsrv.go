package monet

import (
	"time"
)

func newFakeServer(err error) Server {
	f := new(fsrv)
	f.err = err
	return f
}

type fsrv struct {
	received  []string
	err       error
	response  string
	disconnected bool
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
	fs.disconnected = true
	return fs.err
}
