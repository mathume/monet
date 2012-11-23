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
	"log"
	"monet/crypt"
	"net"
	"os"
	"strings"
	"time"
)

var Logger log.Logger = *log.New(os.Stdout, "monetdb ", log.LstdFlags)
var LoginErr error = errors.New("Login failed.")
var PyHashToGo = map[string]crypto.Hash{
	"MD5":    crypto.MD5,
	"SHA1":   crypto.SHA1,
	"SHA224": crypto.SHA224,
	"SHA256": crypto.SHA256,
	"SHA384": crypto.SHA384,
	"SHA512": crypto.SHA512,
}

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
	err = errors.New("Cmd not impl")
	return
}

func (srv *server) connect(protocol, host string, timeout time.Duration) (err error) {
	srv.netConn, err = net.DialTimeout(protocol, host, timeout)
	if err != nil {
		return
	}
	Logger.Println("Connection succeeded.")
	return
}

func (srv *server) Connect(hostname, port, username, password, database, language string, timeout time.Duration) (err error) {
	srv.setConn(hostname, port, username, password, database, language)
	err = srv.connect(NET, hostname+port, timeout)
	if err != nil {
		return
	}
	err = srv.login(0, timeout)
	return
}

func (srv *server) login(iteration int, timeout time.Duration) (err error) {
	challenge, err := srv.getblock()
	if err != nil { return }
	response := srv.challenge_response(challenge)
	srv.putblock([]byte(response))
	block, err := srv.getblock()
	if err != nil { return }
	prompt := strings.TrimSpace(block)

	if len(prompt) == 0 {
	} else if strings.HasPrefix(prompt, MSG_INFO){
		Logger.Println("II", prompt[1:])
	} else if strings.HasPrefix(prompt, MSG_ERROR){
		Logger.Println(prompt[1:])
		err = errors.New("DatabaseError " + prompt[1:])
	} else if strings.HasPrefix(prompt, MSG_REDIRECT){
		redirect := strings.Split(strings.Split(prompt, " \t\r\n")[0][1:], ":")
		if redirect[1] == "merovingian"{
			Logger.Println("II: merovingian proxy, restarting authentication")
			if iteration <= 10 {
				srv.login(iteration + 1, timeout)
			} else {
				err = errors.New("OperationalError, maximal number of redirects reached (10)")
			}
		} else if redirect[1] == "monetdb"{
			srv.hostname = redirect[2][2:]
			pd := strings.Split(redirect[3], "/")
			srv.password, srv.database = pd[0], pd[1]
			Logger.Println("II: merovingian redirect to monetdb:", srv.hostname, srv.port, srv.database)
			srv.conn.netConn.Close()
			srv.Connect(srv.hostname, srv.port, srv.username, srv.password, srv.database, srv.language, timeout)
		} else {
			Logger.Println("!", prompt[0])
			err = errors.New("ProgrammingError, unknown redirect" + prompt)
		}
	} else {
		Logger.Println("!", prompt[0])
		err = errors.New("ProgrammingError, unknown state:" + prompt)
	}

	srv.state = STATE_READY
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
	srv.state = STATE_INIT
	return
}

func (srv *server) challenge_response(challenge string) (response string) {
	//""" generate a response to a mapi login challenge """
	challenges := strings.Split(challenge, ":")
	salt, protocol, hashes := challenges[0], challenges[2], challenges[3]
	password := srv.password

	if protocol == "9" {
		algo := challenges[5]
		h := PyHashToGo[algo].New()
		h.Write([]byte(password))
		password = hex.EncodeToString(h.Sum(nil))
	} else if protocol != "8" {
		Logger.Fatalln("We only speak protocol v8 and v9")
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
		pwhash, err := crypt.Crypt((password + salt)[:8], salt[len(salt)-2:])
		if err != nil {
			Logger.Println("Error calculating response in crypt:")
			Logger.Fatalln(err)
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
		Logger.Println("II: reading", length, "bytes, last:", last)

		read, er := srv.getbytes(length)
		if er != nil {
			err = er
			return
		}
		r = append(r, read...)
	}
	result = string(r)
	Logger.Println("RX:", result)
	return
}

func (srv *server) getbytes(many int) (bytes []byte, err error) {
	bytes = make([]byte, many)
	n, err := srv.conn.netConn.Read(bytes)
	if n != many {
		err = errors.New("didn't receive enought bytes")
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
		Logger.Println("II: sending", length, "bytes, last:", last)
		Logger.Println("TX:", data)
		n1, err1 := srv.conn.netConn.Write(flag)
		n2, err2 := srv.conn.netConn.Write(data)
		if n1 != len(flag) || n2 != len(data) {
			err = errors.New("putblock: not all data was trasmitted")
			Logger.Println("putblock: n1=", n1, ", n2=", n2)
		}
		if err1 != nil {
			err = errors.New("putblock: not all data was transmitted")
			Logger.Println("putblock: ", err1.Error())
		}
		if err2 != nil {
			err = errors.New("putblock: not all data was transmitted")
			Logger.Println("putblock: ", err2.Error())
		}
	}
	return
}

func max(n1, n2 int) (m int){
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
