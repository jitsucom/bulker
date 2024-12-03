package main

import (
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"os"
)

func main() {
	settings := &appbase.AppSettings{
		ConfigPath: os.Getenv("CFGKPR_CONFIG_PATH"),
		Name:       "cfgkpr",
		EnvPrefix:  "CFGKPR",
		ConfigName: "cfgkpr",
		ConfigType: "env",
	}
	application := appbase.NewApp[Config](&Context{}, settings)
	application.Run()
}
