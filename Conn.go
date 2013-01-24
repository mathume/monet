package monet

import (
	"database/sql/driver"
	"errors"
)

var nImpl error = errors.New("Not implemented.")

type mconn struct{
	tx driver.Tx
	srv Server
}

func (m *mconn)Begin() (driver.Tx, error){
	if m.tx != nil {
		return nil, errors.New("There's currently another transaction. End that first or open a new connection.")
	}
	m.tx = newTx()
	m.srv.Cmd("sSTART TRANSACTION;")
	return m.tx, nil
}

func (m *mconn)Close() error{
	return nImpl
}