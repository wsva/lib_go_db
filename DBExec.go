package db

import (
	"database/sql"
)

func execInTransaction(db *sql.DB, tx *sql.Tx, sqltext string) error {
	stmt, err := tx.Prepare(sqltext)
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		tx.Rollback()
		return err
	}
	err = stmt.Close()
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func queryRow(db *sql.DB, sqlstr string) (*sql.Row, error) {
	stmt, err := db.Prepare(sqlstr)
	if err != nil {
		return nil, err
	}
	return stmt.QueryRow(), nil
}

func query(db *sql.DB, sqlstr string) (*sql.Rows, error) {
	stmt, err := db.Prepare(sqlstr)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Query()
}

func queryWithArgs(db *sql.DB, sqlstr string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := db.Prepare(sqlstr)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Query(args...)
}

func query2MapList(db *sql.DB, sqlstr string, limit int) ([]interface{}, error) {
	if limit == 0 {
		limit = 10
	}
	rows, err := query(db, sqlstr)
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
func exec(db *sql.DB, sqlstr string) (sql.Result, error) {
	stmt, err := db.Prepare(sqlstr)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Exec()
}

func execWithArgs(db *sql.DB, sqlstr string, args ...interface{}) (sql.Result, error) {
	stmt, err := db.Prepare(sqlstr)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Exec(args...)
}
