package main

import (
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"os"
)

func main() {
	settings := &appbase.AppSettings{
		ConfigPath: os.Getenv("SYNCCTL_CONFIG_PATH"),
		Name:       "syncctl",
		EnvPrefix:  "SYNCCTL",
		ConfigName: "syncctl",
		ConfigType: "env",
	}
	application := appbase.NewApp[Config](&Context{}, settings)
	application.Run()
}
