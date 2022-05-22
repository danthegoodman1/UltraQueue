package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"
	"time"

	"github.com/danthegoodman1/UltraQueue/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
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

	uq, err := NewUltraQueue("testpart", 100)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start new ultra queue")
	}

	gm, err := NewGossipManager("testpart", "0.0.0.0", uq, 0, "127.0.0.1", "9999", []string{})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start new gossip manager")
	}

	port := utils.GetEnvOrDefault("PORT", "8080")
	log.Debug().Msg("Starting cmux listener on port " + port)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal().Err(err).Str("port", port).Msg("Failed to start cmux listener")
	}

	m := cmux.New(lis)
	httpL := m.Match(cmux.HTTP2(), cmux.HTTP1Fast())
	go StartHTTPServer(httpL, uq, gm)

	go m.Serve()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	log.Warn().Msg("Received shutdown signal!")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	err = httpServer.Echo.Shutdown(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to shutdown HTTP server")
	} else {
		log.Info().Msg("Successfully shutdown HTTP server")
	}

	gm.Shutdown()
	log.Info().Msg("Shut down gossip manager")

}

type CallerHook struct {
}

func (h CallerHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if pc, file, line, ok := runtime.Caller(3); ok {
		e.Str("file", path.Base(file)).Int("line", line).Str("func", runtime.FuncForPC(pc).Name())
	}
}
