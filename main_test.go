package main

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {

	if os.Getenv("PRETTY") == "1" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	if os.Getenv("DEBUG") == "1" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Debug().Msg("Debugging logging activated")
	}

	os.Exit(m.Run())
}
