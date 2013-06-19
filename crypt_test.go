package monet

import "testing"

func Test_crypt(t *testing.T){
	key, slash := "27041982_mathume", "./"
	digest, err := Crypt(key, slash)
	if err != nil {
		t.Fatal(err)
	}
	previously_calculated := "./jcdiX3nUJlM"
	if digest != previously_calculated {
		t.Fatal(digest)
	}
}
