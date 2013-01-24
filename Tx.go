package monet

import (
	"database/sql/driver"
)

type tx struct{
	c *mconn
}

func (t *tx)Commit() error {
	t.c.srv.Cmd("sCOMMIT;")
	t.clear()
	return nil
}

func (t *tx)Rollback() error {
	t.c.srv.Cmd("sROLLBACK;")
	t.clear()
	return nil
}

func (t *tx)clear(){
	t.c.t = nil
	t.c  = nil
}

func newTx(c *mconn) driver.Tx{
	t := new(tx)
	t.c = c
	return t
}