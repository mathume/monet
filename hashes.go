package monet

import (
	"crypto"
)

var HashPyToGo =  map[string]crypto.Hash {
	//  md5(), sha1(), sha224(), sha256(), sha384(), and sha512()
	"MD5"		: crypto.MD5,
	"SHA1"		: crypto.SHA1,
	"SHA224"	: crypto.SHA224,
	"SHA256"	: crypto.SHA256,
	"SHA384"	: crypto.SHA384,
	"SHA512"	: crypto.SHA512,
}

type Hash struct{
	ch crypto.Hash
}

func (h *Hash)Update(with string){
}

func (h *Hash)Hexdigest() string{
}