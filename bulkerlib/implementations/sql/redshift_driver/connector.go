package driver

import (
	"context"
	"database/sql/driver"
)

func NewRedshiftConnector(cfg RedshiftConfig) driver.Connector {
	cfg.Sanitize()
	return &redshiftDataConnector{
		d:   &redshiftDataDriver{},
		cfg: &cfg,
	}
}

type redshiftDataConnector struct {
	d   *redshiftDataDriver
	cfg *RedshiftConfig
}

func (c *redshiftDataConnector) Connect(ctx context.Context) (driver.Conn, error) {
	loadOpts, err := c.cfg.LoadOpts(ctx)
	if err != nil {
		return nil, err
	}
	client, err := newRedshiftDataClient(ctx, c.cfg, loadOpts...)
	if err != nil {
		return nil, err
	}
	return newConnection(client, c.cfg), nil
}

func (c *redshiftDataConnector) Driver() driver.Driver {
	return c.d
}
