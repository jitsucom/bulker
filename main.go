package main

import (
	"github.com/jitsucom/bulker/app"
	_ "github.com/jitsucom/bulker/implementations/sql"
)

func main() {
	app.Run()
}
