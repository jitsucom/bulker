package eventslog

import (
	"github.com/jitsucom/bulker/jitsubase/appbase"
)

type EventsLogConfig struct {
	ClickhouseURL      string `mapstructure:"CLICKHOUSE_URL"`
	ClickhouseDatabase string `mapstructure:"CLICKHOUSE_DATABASE"`
	ClickhouseUsername string `mapstructure:"CLICKHOUSE_USERNAME"`
	ClickhousePassword string `mapstructure:"CLICKHOUSE_PASSWORD"`
}

func (e *EventsLogConfig) PostInit(settings *appbase.AppSettings) error {
	return nil
}
