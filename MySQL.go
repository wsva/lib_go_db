package db

import (
	"database/sql"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

/*
DSN(data source name): username:password@127.0.0.1:3306/database
*/
type MySQL struct {
	DSN string `json:"DSN"`

	//或者使用DSN，或者配置以下信息
	Username string `json:"Username"`
	Password string `json:"Password"`
	Address  string `json:"Address"`
	DBName   string `json:"DBName"`

	DB *sql.DB `json:"-"`
}

func (m *MySQL) InitDB() error {
	if m.DSN == "" {
		config := mysql.NewConfig()
		config.User = m.Username
		config.Passwd = m.Password
		config.Net = "tcp"
		config.Addr = m.Address
		config.DBName = m.DBName
		config.Loc = time.Local
		m.DSN = config.FormatDSN()
	}

	db, err := sql.Open("mysql", m.DSN)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}
	/*
		db.SetConnMaxLifetime(time.Minute * 3) will cause:

		Error 1461: Can't create more than max_prepared_stmt_count
		statements (current value: 16382)
	*/
	db.SetConnMaxLifetime(time.Second * 10)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	m.DB = db
	return nil
}

func (o *MySQL) EscapeString(value string) string {
	value = strings.ReplaceAll(value, "'", `\'`)
	return value
	//return "'" + value + "'"
}
