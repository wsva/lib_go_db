package db

import (
	"database/sql"
	"errors"
	"regexp"
	"strconv"
	"strings"

	go_ora "github.com/sijms/go-ora/v2"
)

type Oracle struct {
	Username      string `json:"Username"`
	Password      string `json:"Password"`
	ConnectString string `json:"ConnectString"`

	DB *sql.DB `json:"-"`
}

/*
ConnectString支持两种格式

1，兼容godror
dbhost:1521/orclpdb1

2，兼容JDBC，go-ora/v2支持
(DESCRIPTION=
    (ADDRESS_LIST=
    	(LOAD_BALANCE=OFF)
        (FAILOVER=ON)
    	(address=(PROTOCOL=tcps)(host=localhost)(PORT=2484))
    	(address=(protocol=tcp)(host=localhost)(port=1521))
    )
    (CONNECT_DATA=
    	(SERVICE_NAME=service)
        (SERVER=DEDICATED)
    )
    (SOURCE_ROUTE=yes)
)
*/
//`user="scott" password="tiger" connectString="dbhost:1521/orclpdb1"`
func (o *Oracle) getDSN() (string, error) {
	if strings.Contains(o.ConnectString, `DESCRIPTION=`) {
		//https://github.com/sijms/go-ora
		return go_ora.BuildJDBC(o.Username, o.Password,
			o.ConnectString, nil), nil
	}
	reg := regexp.MustCompile(`^([\w\.-]+):(\d+)/(\w+)$`)
	submatch := reg.FindStringSubmatch(o.ConnectString)
	if len(submatch) == 0 {
		return "", errors.New("invalid ConnectString: " + o.ConnectString)
	}
	port, err := strconv.ParseInt(submatch[2], 10, 32)
	if err != nil {
		return "", errors.New("parse port error")
	}
	return go_ora.BuildUrl(submatch[1], int(port), submatch[3],
		o.Username, o.Password, nil), nil
}

func (o *Oracle) EscapeString(value string) string {
	value = strings.ReplaceAll(value, "'", "''")
	value = strings.ReplaceAll(value, "&", "' || chr(38) || '")
	return value
	//return "'" + value + "'"
}

func (o *Oracle) InitDB() error {
	dsn, err := o.getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}
	o.DB = db
	return nil
}
