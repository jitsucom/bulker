package types

import "errors"

// DataSourceConfig dto for deserialized datasource config (e.g. in Postgres or AwsRedshift destination)
type DataSourceConfig struct {
	Host       string            `mapstructure:"host,omitempty" json:"host,omitempty" yaml:"host,omitempty"`
	Port       int               `mapstructure:"port,omitempty" json:"port,omitempty" yaml:"port,omitempty"`
	Db         string            `mapstructure:"db,omitempty" json:"db,omitempty" yaml:"db,omitempty"`
	Schema     string            `mapstructure:"schema,omitempty" json:"schema,omitempty" yaml:"schema,omitempty"`
	Username   string            `mapstructure:"username,omitempty" json:"username,omitempty" yaml:"username,omitempty"`
	Password   string            `mapstructure:"password,omitempty" json:"password,omitempty" yaml:"password,omitempty"`
	Parameters map[string]string `mapstructure:"parameters,omitempty" json:"parameters,omitempty" yaml:"parameters,omitempty"`
	//SSLConfiguration *SSLConfig        `mapstructure:"ssl,omitempty" json:"ssl,omitempty" yaml:"ssl,omitempty"`
	//S3               *S3Config         `mapstructure:"s3,omitempty" json:"s3,omitempty" yaml:"s3,omitempty"`
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

	//if dsc.SSLConfiguration != nil {
	//	if err := dsc.SSLConfiguration.Validate(); err != nil {
	//		return err
	//	}
	//}
	return nil
}
