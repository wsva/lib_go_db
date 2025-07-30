package db

import "strings"

type DBType string

const (
	DBTypeOracle        DBType = "Oracle"
	DBTypeMySQL         DBType = "MySQL"
	DBTypeElasticSearch DBType = "ElasticSearch"
	DBTypeMongoDB       DBType = "MongoDB"
	DBTypeSQLite        DBType = "SQLite"
	DBTypePostgreSQL    DBType = "PostgreSQL"
)

var DBTypeAll = []DBType{
	DBTypeOracle,
	DBTypeMySQL,
	DBTypeElasticSearch,
	DBTypeMongoDB,
	DBTypeSQLite,
	DBTypePostgreSQL,
}

func EscapeSqlString(s string) string {
	result := strings.ReplaceAll(s, `\`, `\\`)
	result = strings.ReplaceAll(result, `'`, `\'`)
	return result
}

/*
将字符串处理成适合放在单引号中间样子
1）一个单引号变成两个单引号
2）&符号转换成'||'&'||'
*/
func EscapeOracle(s string) string {
	result := strings.ReplaceAll(s, `'`, `''`)
	result = strings.ReplaceAll(result, `&`, `'||'&'||'`)
	return result
}
