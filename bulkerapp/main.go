package main

import (
	"github.com/jitsucom/bulker/bulkerapp/app"
	_ "github.com/jitsucom/bulker/bulkerlib/implementations/api_based"
	_ "github.com/jitsucom/bulker/bulkerlib/implementations/file_storage"
	_ "github.com/jitsucom/bulker/bulkerlib/implementations/sql"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"os"
)

var Commit string
var Timestamp string

func main() {
	logging.Infof("Starting bulker app. Version: %s Build timestamp: %s", Commit, Timestamp)

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
