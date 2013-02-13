package monet

import (
	"database/sql/driver"
)

type mresult struct{
	liid int64
	ra int64
	err error
}

func newResult(liid, ra int64, err error)driver.Result{
	return &mresult{liid, ra, err}
}

func (r *mresult)LastInsertId()(int64, error){
	return r.liid, r.err
}

func (r *mresult)RowsAffected()(int64, error){
	return r.ra, r.err
}
