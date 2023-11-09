package main

import (
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"os"
)

func main() {
	if os.Getenv("SYNCCTL_SYNCS_ENABLED") == "false" || os.Getenv("SYNCCTL_SYNCS_ENABLED") == "0" {
		fmt.Println("[syncctl] Syncs are disabled. Exiting...")
		os.Exit(0)
	}
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
