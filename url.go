package db

import (
	"net/url"
	"strings"
)

/*
postgresql://USER:PASSWORD@HOST:PORT/DATABASE?schema=SCHEMA
mysql://USER:PASSWORD@HOST:PORT/DATABASE
file:./dev.db
sqlserver://HOST:PORT;database=DBNAME;user=USER;password=PASSWORD;trustServerCertificate=true;
mongodb+srv://USER:PASSWORD@HOST/DATABASE?retryWrites=true&w=majority

Oracle:
oracle://USER:PASSWORD@HOST:PORT/?service_name=service_name&sid=sid&jdbc=description

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
type URL string

func (s URL) Parse() (*Config, error) {
	dbURL := string(s)

	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Driver: strings.ToLower(u.Scheme),
		Params: make(map[string]string),
	}

	if u.User != nil {
		cfg.User = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}

	cfg.Host = u.Hostname()
	cfg.Port = u.Port()

	if u.Path != "" && u.Path != "/" {
		cfg.Database = strings.TrimPrefix(u.Path, "/")
	}

	for k, v := range u.Query() {
		if len(v) > 0 {
			cfg.Params[k] = v[0]
		}
	}

	switch cfg.Driver {
	case "postgresql", "postgres":
		cfg.Driver = "postgres"
		if schema, ok := cfg.Params["schema"]; ok {
			cfg.Schema = schema
		}
	case "sqlserver":
		for _, seg := range strings.Split(dbURL, ";") {
			parts := strings.SplitN(seg, "=", 2)
			if len(parts) == 2 {
				key := strings.ToLower(strings.TrimSpace(parts[0]))
				val := strings.TrimSpace(parts[1])
				switch key {
				case "database":
					cfg.Database = val
				case "user":
					cfg.User = val
				case "password":
					cfg.Password = val
				}
				cfg.Params[key] = val
			}
		}
	case "sqlite", "file":
		cfg.Driver = "sqlite"
		cfg.Database = strings.TrimPrefix(u.Path, "/")
	case "oracle":
		// service name or instance id
		if sid := cfg.Params["sid"]; sid != "" {
			cfg.Database = sid
		}
	}

	return cfg, nil
}
