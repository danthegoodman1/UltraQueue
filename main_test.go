package main

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestMain(ma *testing.M) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro

	log.Logger = log.Logger.Hook(CallerHook{})

	if os.Getenv("PRETTY") == "1" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	if os.Getenv("DEBUG") == "1" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Debug().Msg("Debugging logging activated")
	}

	os.Exit(ma.Run())
}
