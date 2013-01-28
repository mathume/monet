package monet

import (
	"database/sql/driver"
	"errors"
)

var nImpl error = errors.New("Not implemented.")

type mconn struct {
	tx  driver.Tx
	srv Server
}

func (m *mconn) Begin() (driver.Tx, error) {
	if m.tx != nil {
		return nil, errors.New("There's currently another transaction. End that first or open a new connection.")
	}
	m.tx = newTx(m)
	_, err := m.cmd("START TRANSACTION")
	return m.tx, err
}

func (m *mconn) Close() error {
	if m.tx != nil {
		if err := m.tx.Rollback(); err != nil {
			return err
		}
	}
	return m.srv.Disconnect()
}

func (m *mconn) Prepare(fmtquery string) (driver.Stmt, error) {
	return newStmt(m, fmtquery), nil
}

func (m *mconn) cmd(operation string) (response string, err error){
	return m.srv.Cmd("s" + operation + ";")
}

func (m *mconn) clear(){
	m.tx = nil
	return
}