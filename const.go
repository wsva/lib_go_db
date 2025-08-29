package db

type Driver string

const (
	DriverPostgreSQL Driver = "postgres"
	DriverOracle     Driver = "oracle"
	DriverMySQL      Driver = "mysql"
	DriverSQLite     Driver = "sqlite"
)
