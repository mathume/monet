package main
import (
	"database/sql"
	"monet/drv"
	"fmt"
)

func main(){
	driver, err := sql.Open(drv.DRV_NAME, "bad:connection:string")
	fmt.Println(err)
	fmt.Println(driver)
	
}
