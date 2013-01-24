package monet

import (
	"database/sql/driver"
	"errors"
)

var nImpl error = errors.New("Not implemented.")

type mconn struct {
	t  driver.Tx
	srv Server
}

func (m *mconn) Begin() (driver.Tx, error) {
	if m.t != nil {
		return nil, errors.New("There's currently another transaction. End that first or open a new connection.")
	}
	m.t = newTx(m)
	m.srv.Cmd("sSTART TRANSACTION;")
	return m.t, nil
}

func (m *mconn) Close() error {
	if m.t != nil {
		if err := m.t.Rollback(); err != nil {
			return err
		}
	}
	return m.srv.Disconnect()
}

func (m *mconn) Prepare(query string) (driver.Stmt, error) {
	return nil, nImpl
}
