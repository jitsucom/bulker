package main

import (
	"github.com/jitsucom/bulker/bulkerapp/app"
	_ "github.com/jitsucom/bulker/bulkerlib/implementations/file_storage"
	_ "github.com/jitsucom/bulker/bulkerlib/implementations/sql"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"os"
)

func main() {
	settings := &appbase.AppSettings{
		ConfigPath: os.Getenv("BULKER_CONFIG_PATH"),
		Name:       "bulker",
		EnvPrefix:  "BULKER",
		ConfigName: "bulker",
		ConfigType: "env",
	}
	application := appbase.NewApp[app.Config](&app.Context{}, settings)
	application.Run()
}
