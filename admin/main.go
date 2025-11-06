package main

import (
	"os"

	"github.com/jitsucom/bulker/jitsubase/appbase"
)

func main() {
	settings := &appbase.AppSettings{
		ConfigPath: os.Getenv("ADMIN_CONFIG_PATH"),
		Name:       "admin",
		EnvPrefix:  "ADMIN",
		ConfigName: "admin",
		ConfigType: "env",
	}
	application := appbase.NewApp[Config](&Context{}, settings)
	application.Run()
}
