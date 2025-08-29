package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
	wl_uuid "github.com/wsva/lib_go/uuid"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type Config struct {
	Driver   string
	User     string
	Password string
	Host     string
	Port     string
	Database string
	Schema   string
	Params   map[string]string

	DB *sql.DB
}

func (d *Config) InitDB() error {
	var err error
	switch d.Driver {
	case "postgres":
		dsn := fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v",
			d.Host, d.Port, d.User, d.Password, d.Database)
		d.DB, err = sql.Open("postgres", dsn)
	case "mysql":
		dsn := fmt.Sprintf("%v:%v@%v:%v/%v",
			d.User, d.Password, d.Host, d.Port, d.Database)
		d.DB, err = sql.Open("mysql", dsn)
		/*
			db.SetConnMaxLifetime(time.Minute * 3) will cause:

			Error 1461: Can't create more than max_prepared_stmt_count
			statements (current value: 16382)
		*/
		if err == nil {
			d.DB.SetConnMaxLifetime(time.Second * 10)
			d.DB.SetMaxOpenConns(10)
			d.DB.SetMaxIdleConns(10)
		}
	case "sqlite", "file":
		d.DB, err = sql.Open("sqlite", d.Database)
	case "oracle":
		port, _ := strconv.ParseInt(d.Port, 10, 32)
		connStr := ""
		if v, ok := d.Params["service_name"]; ok && v != "" {
			connStr = go_ora.BuildUrl(d.Host, int(port), v, d.User, d.Password, nil)
		} else if v, ok := d.Params["sid"]; ok && v != "" {
			urlOptions := map[string]string{"SID": v}
			connStr = go_ora.BuildUrl(d.Host, int(port), "", d.User, d.Password, urlOptions)
		} else if v, ok := d.Params["jdbc"]; ok && v != "" {
			urlOptions := map[string]string{"connStr": v}
			connStr = go_ora.BuildUrl(d.Host, int(port), "", d.User, d.Password, urlOptions)
		} else {
			return errors.New("build connection string error")
		}
		d.DB, err = sql.Open("oracle", connStr)
	default:
		return fmt.Errorf("unsupported db type: %v", d.Driver)
	}

	if err != nil {
		return err
	}
	return d.DB.Ping()
}

func (d *Config) Close() error {
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
func (d *Config) Begin() (*sql.Tx, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return d.DB.Begin()
}

func (d *Config) QueryRow(query string, args ...any) (*sql.Row, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return d.DB.QueryRow(query, args...), nil
}

func (d *Config) Query(query string, args ...any) (*sql.Rows, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return d.DB.Query(query, args...)
}

func (d *Config) Query2MapList(limit int, query string, args ...any) ([]any, error) {
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
func (d *Config) Exec(query string, args ...any) (sql.Result, error) {
	err := d.InitDB()
	if err != nil {
		return nil, err
	}
	return d.DB.Exec(query, args...)
}

func (d *Config) GetUUID() string {
	var sqltext string
	switch d.Driver {
	case "mysql":
		sqltext = "select uuid()"
	case "oracle":
		sqltext = "select rawtohex(sys_guid()) from dual"
	case "sqlite":
		sqltext = "select hex(randomblob(16))"
	case "postgres":
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

func (d *Config) EscapeString(value string) string {
	switch d.Driver {
	case "mysql":
		value = strings.ReplaceAll(value, "'", `\'`)
		return value
	case "oracle":
		value = strings.ReplaceAll(value, "'", "''")
		value = strings.ReplaceAll(value, "&", "' || chr(38) || '")
		return value
	}
	return value
}
