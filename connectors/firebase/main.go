package main

import (
	airbyte "github.com/jitsucom/bulker/airbytecdk"
	"log"
	"os"
)

func main() {
	hsrc := NewFirebaseSource()
	runner := airbyte.NewSourceRunner(hsrc, os.Stdout)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
