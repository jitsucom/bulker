package eventslog

import (
	"github.com/jitsucom/bulker/jitsubase/appbase"
)

type EventsLogConfig struct {
	ClickhouseHost     string `mapstructure:"CLICKHOUSE_HOST"`
	ClickhouseDatabase string `mapstructure:"CLICKHOUSE_DATABASE"`
	ClickhouseUsername string `mapstructure:"CLICKHOUSE_USERNAME"`
	ClickhousePassword string `mapstructure:"CLICKHOUSE_PASSWORD"`
	ClickhouseSSL      bool   `mapstructure:"CLICKHOUSE_SSL"`
}

func (e *EventsLogConfig) PostInit(settings *appbase.AppSettings) error {
	return nil
}
