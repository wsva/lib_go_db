package db

import (
	"database/sql"

	_ "modernc.org/sqlite"
)

type SQLite struct {
	Datafile string `json:"Datafile"`

	DB *sql.DB `json:"-"`
}

func (d *SQLite) InitDB() error {
	if d.DB != nil {
		return nil
	}
	db, err := sql.Open("sqlite", d.Datafile)
	if err != nil {
		return err
	}
	d.DB = db
	return nil
}
