package main

import (
	"os"
	"path"
	"runtime"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro

	log.Logger = log.Logger.Hook(CallerHook{})

	if os.Getenv("PRETTY") == "1" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	if os.Getenv("DEBUG") == "1" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Debug().Msg("Debugging logging activated")
	}

	log.Info().Msg("Starting UltraQueue node")

}

type CallerHook struct {
}

func (h CallerHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if pc, file, line, ok := runtime.Caller(3); ok {
		e.Str("file", path.Base(file)).Int("line", line).Str("func", runtime.FuncForPC(pc).Name())
	}
}
