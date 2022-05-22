package main

import (
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
)

type HTTPServer struct {
	Echo *echo.Echo
}

var (
	Server *HTTPServer
)

func StartHTTPServer(lis net.Listener) {
	echoInstance := echo.New()
	Server = &HTTPServer{
		Echo: echoInstance,
	}
	Server.Echo.HideBanner = true
	Server.Echo.HidePort = true
	// Server.Echo.Use(middleware.Logger())
	config := middleware.LoggerConfig{
		Skipper: middleware.DefaultSkipper,
		Format: `{"time":"${time_rfc3339_nano}","id":"${id}","remote_ip":"${remote_ip}",` +
			`"host":"${host}","method":"${method}","uri":"${uri}","user_agent":"${user_agent}",` +
			`"status":${status},"error":"${error}","latency":${latency},"latency_human":"${latency_human}"` +
			`,"bytes_in":${bytes_in},"bytes_out":${bytes_out},"proto":"${protocol}"}` + "\n",
		CustomTimeFormat: "2006-01-02 15:04:05.00000",
		Output:           log.Logger,
	}
	Server.Echo.Use(middleware.LoggerWithConfig(config))

	// Health Check route
	Server.Echo.GET("/hc", HealthCheck)

	SetupMetrics()

	log.Info().Msg("Starting HTTP API")
	Server.Echo.Listener = lis
	server := &http2.Server{}
	Server.Echo.StartH2CServer("", server)
}

func ValidateRequest(c echo.Context, s interface{}) error {
	if err := c.Bind(s); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err := c.Validate(s); err != nil {
		return err
	}
	return nil
}

func HealthCheck(c echo.Context) error {
	return c.NoContent(http.StatusOK)
}
