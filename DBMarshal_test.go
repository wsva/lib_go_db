package db_test

import (
	"fmt"
	"testing"

	wl_db "github.com/wsva/lib_go_db"
)

func TestMarshal(T *testing.T) {
	db := wl_db.DB{
		Type: wl_db.DBTypeOracle,
		Oracle: wl_db.Oracle{
			Username: "",
		},
	}
	_, jsonString, err := wl_db.MarshalDB(db, true)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(jsonString)
	}
}
