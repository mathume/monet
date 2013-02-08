package monet

import (
	"database/sql/driver"
)

type result struct{
	liid int64
	ra int64
	err error
}

func newResult(liid, ra int64, err error)driver.Result{
	return &result{liid, ra, err}
}

func (r *result)LastInsertId()(int64, error){
	return r.liid, r.err
}

func (r *result)RowsAffected()(int64, error){
	return r.ra, r.err
}
