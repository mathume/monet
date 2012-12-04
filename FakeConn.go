package monet

import (
	"errors"
	"net"
	"time"
)

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
