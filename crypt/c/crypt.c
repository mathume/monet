#include <crypt.h>
#include <stdio.h>

int main(){
	char* key = "27041982_sebastian_mitterle";
	char* slash = "./";
	char* digest = crypt(key, slash);
	printf("%s", digest);
	printf("%s", "\n");
	return 0;
}