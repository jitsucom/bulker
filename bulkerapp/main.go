package main

import (
	"github.com/jitsucom/bulker/bulkerapp/app"
	_ "github.com/jitsucom/bulker/bulkerlib/implementations/file_storage"
	_ "github.com/jitsucom/bulker/bulkerlib/implementations/sql"
)

func main() {
	app.Run()
}
