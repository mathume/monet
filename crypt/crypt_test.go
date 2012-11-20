package crypt

import "testing"

func Test_crypt(t *testing.T){
	key, slash := "27041982_sebastian_mitterle", "./"
	digest, err := Crypt(key, slash)
	if err != nil {
		t.Fatal(err)
	}
	previously_calculated := "./jcdiX3nUJlM"
	if digest != previously_calculated {
		t.Fatal(digest)
	}
}
