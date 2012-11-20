package crypt

import (
    "testing"
    _ "crypto"
)

func TestHashDES(t *testing.T) {
	h := New()
	h.SetSlash("./")
	h.Write([]byte("27041982_mathume"))
	dig := string(h.Sum(nil))
	previously_calculated := "./jcdiX3nUJlM"
	if dig != previously_calculated {
		t.Fatal(dig)
	}
}

