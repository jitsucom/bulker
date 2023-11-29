package main

import (
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"os"
)

func main() {
	settings := &appbase.AppSettings{
		ConfigPath: os.Getenv("INGEST_CONFIG_PATH"),
		Name:       "ingest",
		EnvPrefix:  "INGEST",
		ConfigName: "ingest",
		ConfigType: "env",
	}
	application := appbase.NewApp[Config](&Context{}, settings)
	application.Run()
}
