package db

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

type PostgreSQL struct {
	Host     string `json:"Host"`
	Port     int    `json:"Port"`
	Username string `json:"Username"`
	Password string `json:"Password"`
	Database string `json:"Database"`
	SSLMode  bool   `json:"SSLMode"`

	DB *sql.DB `json:"-"`
}

func (o *PostgreSQL) getDSN() (string, error) {
	dsn := fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v",
		o.Host, o.Port, o.Username, o.Password, o.Database)
	if o.SSLMode {
		return dsn + " sslmode=enable", nil
	}
	return dsn + " sslmode=disable", nil
}

func (o *PostgreSQL) EscapeString(value string) string {
	value = strings.ReplaceAll(value, "'", "''")
	value = strings.ReplaceAll(value, "&", "' || chr(38) || '")
	return value
	//return "'" + value + "'"
}

func (o *PostgreSQL) InitDB() error {
	dsn, err := o.getDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("postgres", dsn)
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
