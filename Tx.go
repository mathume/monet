package monet

import (
	"database/sql/driver"
)

type tx struct{}

func newTx() driver.Tx{
	var tx driver.Tx
	return tx
}