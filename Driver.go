/*
	Package monet implements the database/sql/driver interfaces for the monetdb database (http://monetdb.org).

	example:

		import (
			"database/sql"
			"monet"
			"time"
		)

		db, err := sql.Open(monet.DRV_NAME, monet.ConnectionString("localhost", "50000", "monetdb", "monetdb", "voc", time.Second*10))
		...
	
	Currently only default driver.Value data types are supported.
	Server implements the mapi protocol as defined for the monetdb. You usually don't need it.
	Placeholders currently only work for driver.Value.
	The Server has a global logger for all connections. By default nothing is logged. You can switch globally to another logger:

		monet.MapiLogger = syslog.New(...)
	or
		monet.MapiLogger = monet.DebugToStderr
	or
		monet.MapiLogger = monet.New(writer, prefix, logFlags, logLevel)
*/
package monet

import (
	"database/sql"
	"database/sql/driver"
	"strings"
	"errors"
	"time"
)

const (
	c_SEP = ";"
	DRV_NAME = "monet"
)

func init(){
	sql.Register(DRV_NAME, &mdriver{})
}

// Returns the dataSourceName for
//	db, err := sql.Open(monet.DRV_NAME, dataSourceName)
func ConnectionString(hostname, port, username, password, database string, timeout time.Duration) string {
	return strings.Join([]string{hostname, port, username, password, database, timeout.String()}, c_SEP)
}

type mdriver struct {}

func (d *mdriver)Open(ConnectionString string)(driver.Conn, error){
	c := new(mconn)
	c.srv = NewServer()
	s := strings.Split(ConnectionString, c_SEP)
	if len(s) != 6 {
		return nil, errors.New("Wrong connection string.")
	}
	dur, err := time.ParseDuration(s[5])
	if err != nil {
		return nil, err
	}
	err = c.srv.Connect(s[0], ":"+s[1], s[2], s[3], s[4], "sql", dur)
	return c, err
}
