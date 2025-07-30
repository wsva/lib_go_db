package db

import (
	"database/sql"
	"fmt"
)

/*
*************************数据库预定义*************************
 */

/*
将多种数据库集成在一个struct里，便于函数间传值

ID是为了在有多个数据库的时候，组成DBMap时能够方便区分
*/
type DB struct {
	ID         string     `json:"ID"`
	Type       DBType     `json:"Type"`
	Oracle     Oracle     `json:"Oracle"`
	MySQL      MySQL      `json:"MySQL"`
	PostgreSQL PostgreSQL `json:"PostgreSQL"`
	SQLite     SQLite     `json:"SQLite"`

	DB *sql.DB `json:"-"`
}

func (d *DB) InitDB() error {
	switch d.Type {
	case DBTypeMySQL:
		if d.MySQL.DB == nil {
			err := d.MySQL.InitDB()
			if err != nil {
				return err
			}
		}
		d.DB = d.MySQL.DB
	case DBTypeOracle:
		if d.Oracle.DB == nil {
			err := d.Oracle.InitDB()
			if err != nil {
				return err
			}
		}
		d.DB = d.Oracle.DB
	case DBTypeSQLite:
		if d.SQLite.DB == nil {
			err := d.SQLite.InitDB()
			if err != nil {
				return err
			}
		}
		d.DB = d.SQLite.DB
	default:
		return fmt.Errorf("unsupported db type: %v", d.Type)
	}
	return nil
}

func (d *DB) Close() error {
	return d.DB.Close()
}

func (d *DB) Begin() (*sql.Tx, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return d.DB.Begin()
}

func (d *DB) ExecInTransaction(tx *sql.Tx, sqltext string) error {
	err := d.InitDB()
	if err != nil {
		return err
	}
	return execInTransaction(d.DB, tx, sqltext)
}

func (d *DB) QueryRow(sqlstr string) (*sql.Row, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return queryRow(d.DB, sqlstr)
}

func (d *DB) Query(sqlstr string) (*sql.Rows, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return query(d.DB, sqlstr)
}

func (d *DB) QueryWithArgs(sqlstr string, args ...interface{}) (*sql.Rows, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return queryWithArgs(d.DB, sqlstr, args...)
}

func (d *DB) Query2MapList(sqlstr string, limit int) ([]interface{}, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return query2MapList(d.DB, sqlstr, limit)
}

// affected, err := result.RowsAffected()
func (d *DB) Exec(sqlstr string) (sql.Result, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return exec(d.DB, sqlstr)
}

func (d *DB) ExecWithArgs(sqlstr string, args ...interface{}) (sql.Result, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return execWithArgs(d.DB, sqlstr, args...)
}

func (d *DB) GetUUID() (string, error) {
	var sqltext string
	switch d.Type {
	case DBTypeMySQL:
		sqltext = "select uuid()"
	case DBTypeOracle:
		sqltext = "select rawtohex(sys_guid()) from dual"
	case DBTypeSQLite:
		sqltext = "select hex(randomblob(16))"
	case DBTypePostgreSQL:
		sqltext = "select replace(uuid_generate_v4()::text, '-', '')"
	default:
		return "", fmt.Errorf("unsupported db type: %v", d.Type)
	}
	row, err := d.QueryRow(sqltext)
	if err != nil {
		return "", err
	}
	var f1 sql.NullString
	err = row.Scan(&f1)
	if err != nil {
		return "", err
	}
	return f1.String, nil
}

func (d *DB) EscapeString(value string) string {
	switch d.Type {
	case DBTypeMySQL:
		return d.MySQL.EscapeString(value)
	case DBTypeOracle:
		return d.Oracle.EscapeString(value)
	}
	return value
}
