package monet

import (
	"crypto"
	_ "crypto/md5"
	_ "crypto/sha1"
	_ "crypto/sha256"
	_ "crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"
)

var MapiLogger LogWriter = new(nolog)

var c_PyHashToGo = map[string]crypto.Hash{
	"MD5":    crypto.MD5,
	"SHA1":   crypto.SHA1,
	"SHA224": crypto.SHA224,
	"SHA256": crypto.SHA256,
	"SHA384": crypto.SHA384,
	"SHA512": crypto.SHA512,
}

const (
	MAX_PACKAGE_LENGTH = (1024 * 8) - 2

	c_MSG_PROMPT   = ""
	c_MSG_INFO     = "#"
	c_MSG_ERROR    = "!"
	c_MSG_Q        = "&"
	c_MSG_QTABLE   = "&1"
	c_MSG_QUPDATE  = "&2"
	c_MSG_QSCHEMA  = "&3"
	c_MSG_QTRANS   = "&4"
	c_MSG_QPREPARE = "&5"
	c_MSG_QBLOCK   = "&6"
	c_MSG_HEADER   = "%"
	c_MSG_TUPLE    = "["
	c_MSG_REDIRECT = "^"

	c_STATE_INIT  = 0
	c_STATE_READY = 1

	c_NET = "tcp"
)

// A low level connection to the monetdb mapi server.
type Server interface {
	Cmd(operation string) (string, error)
	Connect(hostname, port, username, password, database, language string, timeout time.Duration) error
	Disconnect() error
}

type server struct {
	conn
	state  int
	result interface{}
	socket interface{}
	logger LogWriter
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
	return CreateServer(MapiLogger)
}

func CreateServer(logger LogWriter) Server {
	var c conn
	s := server{c, c_STATE_INIT, nil, nil, logger}
	s.logger.Info("Server initialized.")
	return &s
}

func (srv *server) Cmd(operation string) (response string, err error) {
	srv.logger.Debug("II: executing command" + operation)

	if srv.state != c_STATE_READY {
		err = errors.New("Programming error: not ready for command")
		return
	}

	srv.putblock([]byte(operation))
	response, err = srv.getblock()
	if err != nil {
		return
	}
	if len(response) == 0 {
		return
	}
	well := []string{c_MSG_Q, c_MSG_HEADER, c_MSG_TUPLE}
	var isWell bool
	for _, v := range well {
		isWell = isWell || strings.HasPrefix(response, v)
	}
	if isWell {
		return

	} else if string(response[0]) == c_MSG_ERROR {
		err = errors.New("OperationalError: " + response[1:])
		return
	} else {
		err = errors.New("ProgrammingError: unknown state " + string(response[0]))
	}
	return
}

func (srv *server) connect(protocol, host string, timeout time.Duration) (err error) {
	srv.netConn, err = net.DialTimeout(protocol, host, timeout)
	if err != nil {
		return
	}
	srv.logger.Info("Connection succeeded.")
	return
}

func (srv *server) Connect(hostname, port, username, password, database, language string, timeout time.Duration) (err error) {
	srv.setConn(hostname, port, username, password, database, language)
	err = srv.connect(c_NET, hostname+port, timeout)
	if err != nil {
		return
	}
	err = srv.login(0, timeout)
	return
}

func (srv *server) login(iteration int, timeout time.Duration) (err error) {
	challenge, err := srv.getblock()
	if err != nil {
		return
	}
	response := srv.challenge_response(challenge)
	srv.putblock([]byte(response))
	block, err := srv.getblock()
	if err != nil {
		return
	}
	prompt := strings.TrimSpace(block)

	if len(prompt) == 0 {
	} else if strings.HasPrefix(prompt, c_MSG_INFO) {
		srv.logger.Debug("II " + prompt[1:])
	} else if strings.HasPrefix(prompt, c_MSG_ERROR) {
		srv.logger.Err(prompt[1:])
		err = errors.New("DatabaseError " + prompt[1:])
	} else if strings.HasPrefix(prompt, c_MSG_REDIRECT) {
		redirect := strings.Split(strings.Split(prompt, " \t\r\n")[0][1:], ":")
		if redirect[1] == "merovingian" {
			srv.logger.Info("II: merovingian proxy, restarting authentication")
			if iteration <= 10 {
				srv.login(iteration+1, timeout)
			} else {
				err = errors.New("OperationalError, maximal number of redirects reached (10)")
			}
		} else if redirect[1] == "monetdb" {
			srv.hostname = redirect[2][2:]
			pd := strings.Split(redirect[3], "/")
			srv.password, srv.database = pd[0], pd[1]
			srv.logger.Warning("II: merovingian redirect to monetdb:" + srv.hostname + ";" + srv.port + ";" + srv.database)
			srv.conn.netConn.Close()
			srv.Connect(srv.hostname, srv.port, srv.username, srv.password, srv.database, srv.language, timeout)
		} else {
			srv.logger.Err(prompt)
			err = errors.New("ProgrammingError, unknown redirect: " + prompt[1:])
		}
	} else {
		srv.logger.Err(prompt)
		err = errors.New("ProgrammingError, unknown state: " + prompt[1:])
	}

	srv.state = c_STATE_READY
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
	err = srv.conn.netConn.Close()
	srv.state = c_STATE_INIT
	return
}

func (srv *server) challenge_response(challenge string) (response string) {
	//""" generate a response to a mapi login challenge """
	challenges := strings.Split(challenge, ":")
	salt, protocol, hashes := challenges[0], challenges[2], challenges[3]
	password := srv.password

	if protocol == "9" {
		algo := challenges[5]
		h := c_PyHashToGo[algo].New()
		h.Write([]byte(password))
		password = hex.EncodeToString(h.Sum(nil))
	} else if protocol != "8" {
		srv.logger.Err("We only speak protocol v8 and v9")
	}

	var pwhash string
	hh := strings.Split(hashes, ",")
	if contains(hh, "SHA1") {
		s := crypto.SHA1.New()
		s.Write([]byte(password))
		s.Write([]byte(salt))
		pwhash = "{SHA1}" + hex.EncodeToString(s.Sum(nil))
	} else if contains(hh, "MD5") {
		s := crypto.MD5.New()
		s.Write([]byte(password))
		s.Write([]byte(salt))
		pwhash = "{MD5}" + hex.EncodeToString(s.Sum(nil))
	} else if contains(hh, "crypt") {
		pwhash, err := Crypt((password + salt)[:8], salt[len(salt)-2:])
		if err != nil {
			srv.logger.Err("Error calculating response in crypt:")
			srv.logger.Err(err.Error())
		}
		pwhash = "{crypt}" + pwhash
	} else {
		pwhash = "{plain}" + password + salt
	}
	response = strings.Join([]string{"BIG", srv.username, pwhash, srv.language, srv.database}, ":") + ":"
	return
}

func (srv *server) getblock() (result string, err error) {
	var r []byte
	last := 0
	for last != 1 {
		flag, er := srv.getbytes(2)
		if er != nil {
			err = er
			return
		}
		unpacked := int(binary.LittleEndian.Uint16(flag))
		length := unpacked >> 1
		last = unpacked & 1
		srv.logger.Info("II: reading " + strconv.Itoa(length) + " bytes, last: " + strconv.Itoa(last))

		read, er := srv.getbytes(length)
		if er != nil {
			err = er
			return
		}
		r = append(r, read...)
	}
	result = string(r)
	srv.logger.Debug("RX: " + result)
	return
}

func (srv *server) getbytes(many int) (bytes []byte, err error) {
	bytes = make([]byte, many)
	n, err := srv.conn.netConn.Read(bytes)
	if n != many {
		err = errors.New("didn't receive enough bytes")
		srv.logger.Err(err.Error())
	}
	return
}

func (srv *server) putblock(bytes []byte) (err error) {
	last := 0
	for last == 0 {
		var data []byte
		length := len(bytes)
		if length < MAX_PACKAGE_LENGTH {
			last = 1
			data = bytes
		} else {
			data = bytes[:MAX_PACKAGE_LENGTH]
			bytes = bytes[MAX_PACKAGE_LENGTH:]
		}
		flag := make([]byte, 2)
		i_flag := uint16((length << 1) + 1)
		binary.LittleEndian.PutUint16(flag, i_flag)
		srv.logger.Info("II: sending " + strconv.Itoa(length) + " bytes, last: " + strconv.Itoa(last))
		srv.logger.Debug("TX:" + string(data))
		n1, err1 := srv.conn.netConn.Write(flag)
		n2, err2 := srv.conn.netConn.Write(data)
		if n1 != len(flag) || n2 != len(data) {
			err = errors.New("putblock: not all data was trasmitted")
			srv.logger.Err(err.Error())
		}
		if err1 != nil {
			err = errors.New("putblock: not all data was transmitted")
			srv.logger.Err("putblock: " + err1.Error())
		}
		if err2 != nil {
			err = errors.New("putblock: not all data was transmitted")
			srv.logger.Err("putblock: " + err2.Error())
		}
	}
	return
}

func max(n1, n2 int) (m int) {
	m = n1
	if n1 < n2 {
		m = n2
	}
	return
}

func contains(list []string, item string) (b bool) {
	for _, v := range list {
		if v == item {
			b = true
		}
	}
	return
}
