package db

import (
	"database/sql"
	"fmt"
	"strings"

	wl_uuid "github.com/wsva/lib_go/uuid"
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
	case DBTypePostgreSQL:
		if d.PostgreSQL.DB == nil {
			err := d.PostgreSQL.InitDB()
			if err != nil {
				return err
			}
		}
		d.DB = d.PostgreSQL.DB
	default:
		return fmt.Errorf("unsupported db type: %v", d.Type)
	}
	return nil
}

func (d *DB) Close() error {
	return d.DB.Close()
}

/*
Begin inits a transaction

	tx, err := db.Begin()
	if err != nil {
	    log.Fatal("begin tx error:", err)
	}
	_, err = tx.Exec("INSERT INTO users (name) VALUES ($1)", "Alice")
	if err != nil {
	    tx.Rollback()
	    log.Fatal("insert failed, rollback:", err)
	}
	_, err = tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE id = $1", 1)
	if err != nil {
	    tx.Rollback()
	    log.Fatal("update failed, rollback:", err)
	}
	if err := tx.Commit(); err != nil {
	    log.Fatal("commit failed:", err)
	}
*/
func (d *DB) Begin() (*sql.Tx, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return d.DB.Begin()
}

func (d *DB) QueryRow(query string, args ...any) (*sql.Row, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return d.DB.QueryRow(query, args...), nil
}

func (d *DB) Query(query string, args ...any) (*sql.Rows, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return d.DB.Query(query, args...)
}

func (d *DB) Query2MapList(limit int, query string, args ...any) ([]any, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	if limit == 0 {
		limit = 10
	}
	rows, err := d.Query(query, args...)
	if err != nil {
		return nil, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	count := len(columnTypes)
	result := []interface{}{}

	lineCount := 0
	for rows.Next() {
		lineCount++
		if lineCount > limit {
			break
		}

		scanArgs := make([]interface{}, count)
		for i, v := range columnTypes {
			switch v.DatabaseTypeName() {
			case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
				scanArgs[i] = new(sql.NullString)
			case "BOOL":
				scanArgs[i] = new(sql.NullBool)
			case "INT4":
				scanArgs[i] = new(sql.NullInt64)
			default:
				scanArgs[i] = new(sql.NullString)
			}
		}
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		rowMap := map[string]interface{}{}
		for i, v := range columnTypes {
			if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
				rowMap[v.Name()] = z.Bool
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				rowMap[v.Name()] = z.String
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				rowMap[v.Name()] = z.Int64
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
				rowMap[v.Name()] = z.Float64
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
				rowMap[v.Name()] = z.Int32
				continue
			}
			rowMap[v.Name()] = scanArgs[i]
		}
		result = append(result, rowMap)
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return result, nil
}

// affected, err := result.RowsAffected()
func (d *DB) Exec(query string, args ...any) (sql.Result, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return d.DB.Exec(query, args...)
}

func (d *DB) GetUUID() string {
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
		return strings.ReplaceAll(wl_uuid.New(), "-", "")
	}
	row, err := d.QueryRow(sqltext)
	if err != nil {
		return strings.ReplaceAll(wl_uuid.New(), "-", "")
	}
	var f1 sql.NullString
	err = row.Scan(&f1)
	if err != nil {
		return strings.ReplaceAll(wl_uuid.New(), "-", "")
	}
	return f1.String
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
