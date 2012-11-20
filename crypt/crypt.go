package crypt

/*
#cgo CFLAGS: -l/usr/include
#cgo LDFLAGS: -lcrypt
#include <stdlib.h>
#include <errno.h>
#include <crypt.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

func Crypt(key, slash string) (digest string, err error) {
	if len(slash) != 2 {
		err = errors.New("Slash invalid.")
		return
	}
	k, s := C.CString(key), C.CString(slash)
	defer func() {
		C.free(unsafe.Pointer(k))
		C.free(unsafe.Pointer(s))
	}()
	d, err := C.crypt(k, s)
	if err != nil {
		return
	}
	digest = C.GoString(d)
	return
}


