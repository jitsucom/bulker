package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("redshift-data", &redshiftDataDriver{})
}

type redshiftDataDriver struct{}

func (d *redshiftDataDriver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d *redshiftDataDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &redshiftDataConnector{
		d:   d,
		cfg: cfg,
	}, nil
}
