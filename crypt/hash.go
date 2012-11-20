package crypt

import (
	"errors"
	"hash"
)

var SlashMissing error = errors.New("Slash not set. Use HashDES.SetSlash.")

// bytes / length of string
const Size = 13

// arbitrary
const BlockSize = 1024

type HashDES interface {
	hash.Hash
	//sets the two byte slash. if slash hasn't length equal 2, it will cut the string or pad it with "."
	SetSlash(slash string)
}

type digest struct {
	s     string
	h     string
	slash string
}

func New() HashDES {
	d := new(digest)
	return d
}

func (d *digest) SetSlash(slash string) {
	if l := len(slash); l < 2 {
		slash = slash + ".."
	}
	d.slash = slash[0:2]
}

func (d *digest) Reset() {
	d = new(digest)
}

func (d *digest) Size() int {
	return Size
}

func (d *digest) BlockSize() int {
	return BlockSize
}

func (d *digest) Write(b []byte) (n int, err error) {
	if d.slash == "" {
		err = SlashMissing
		return
	}
	d.s += string(b)
	n = len(b)
	d.h, err = Crypt(d.s, d.slash)
	return
}

func (d *digest) Sum(b []byte) []byte {
	if d.slash == "" {
		return *new([]byte)
	}
	s := d.h + string(b)
	return []byte(s)[0:d.Size()]
}
