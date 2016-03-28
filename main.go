package main

import (
	"os"

	"sqliteonecbeat/beater"

	"github.com/elastic/beats/libbeat/beat"
)

var Name = "sqliteonecbeat"

func main() {
	if err := beat.Run(Name, "", beater.New()); err != nil {
		os.Exit(1)
	}
}
