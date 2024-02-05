package main

import (
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"os"
)

func main() {
	settings := &appbase.AppSettings{
		ConfigPath: os.Getenv("INGMGR_CONFIG_PATH"),
		Name:       "ingmgr",
		EnvPrefix:  "INGMGR",
		ConfigName: "ingmgr",
		ConfigType: "env",
	}
	application := appbase.NewApp[Config](&Context{}, settings)
	application.Run()
}
