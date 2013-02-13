package monet

import (
	"database/sql/driver"
)

type mtx struct{
	c *mconn
}

func (t *mtx)Commit() error {
	t.c.cmd("COMMIT")
	t.clear()
	return nil
}

func (t *mtx)Rollback() error {
	t.c.cmd("ROLLBACK")
	t.clear()
	return nil
}

func (t *mtx)clear(){
	t.c.clear()
	t.c = nil
}

func newTx(c *mconn) driver.Tx{
	t := new(mtx)
	t.c = c
	return t
}