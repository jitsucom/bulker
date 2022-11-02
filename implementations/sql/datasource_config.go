package sql

import "errors"

// DataSourceConfig dto for deserialized datasource config (e.g. in Postgres or AwsRedshift destination)
type DataSourceConfig struct {
	Host       string            `mapstructure:"host,omitempty" json:"host,omitempty" yaml:"host,omitempty"`
	Port       int               `mapstructure:"port,omitempty" json:"port,omitempty" yaml:"port,omitempty"`
	Db         string            `mapstructure:"database,omitempty" json:"database,omitempty" yaml:"database,omitempty"`
	Schema     string            `mapstructure:"default_schema,omitempty" json:"default_schema,omitempty" yaml:"default_schema,omitempty"`
	Username   string            `mapstructure:"username,omitempty" json:"username,omitempty" yaml:"username,omitempty"`
	Password   string            `mapstructure:"password,omitempty" json:"password,omitempty" yaml:"password,omitempty"`
	Parameters map[string]string `mapstructure:"parameters,omitempty" json:"parameters,omitempty" yaml:"parameters,omitempty"`
}

// Validate required fields in DataSourceConfig
func (dsc *DataSourceConfig) Validate() error {
	if dsc == nil {
		return errors.New("Datasource config is required")
	}
	if dsc.Host == "" {
		return errors.New("Datasource host is required parameter")
	}
	if dsc.Db == "" {
		return errors.New("Datasource db is required parameter")
	}
	if dsc.Username == "" {
		return errors.New("Datasource username is required parameter")
	}

	if dsc.Parameters == nil {
		dsc.Parameters = map[string]string{}
	}

	return nil
}
