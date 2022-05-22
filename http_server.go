package main

import (
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/danthegoodman1/UltraQueue/utils"
	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
)

type HTTPServer struct {
	Echo *echo.Echo
	UQ   *UltraQueue
	GM   *GossipManager
}

type CustomValidator struct {
	validator *validator.Validate
}

var (
	Server *HTTPServer
)

func StartHTTPServer(lis net.Listener, uq *UltraQueue, gm *GossipManager) {
	echoInstance := echo.New()
	Server = &HTTPServer{
		Echo: echoInstance,
		UQ:   uq,
		GM:   gm,
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
	Server.Echo.Validator = &CustomValidator{validator: validator.New()}

	// Health Check route
	Server.Echo.GET("/hc", Server.HealthCheck)

	Server.Echo.POST("/enqueue", Server.Enqueue)
	Server.Echo.POST("/dequeue", Server.Dequeue)
	Server.Echo.POST("/ack", Server.Ack)
	Server.Echo.POST("/nack", Server.Nack)

	debugGroup := Server.Echo.Group("/debug")
	debugGroup.GET("/localTopics.json", Server.DebugGetLocalTopics)
	debugGroup.GET("/remoteTopics.json", Server.DebugGetRemoteTopics)

	SetupMetrics()

	log.Info().Msg("Starting HTTP API")
	Server.Echo.Listener = lis
	server := &http2.Server{}
	Server.Echo.StartH2CServer("", server)
}

func (cv *CustomValidator) Validate(i interface{}) error {
	if err := cv.validator.Struct(i); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	return nil
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

func (HTTPServer) HealthCheck(c echo.Context) error {
	return c.NoContent(http.StatusOK)
}

func (s *HTTPServer) Enqueue(c echo.Context) error {
	body := HTTPEnqueueRequest{}
	err := ValidateRequest(c, &body)
	if err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	err = s.UQ.Enqueue(body.Topics, []byte(body.Payload), utils.DefaultInt32(body.Priority, 10), utils.DefaultInt32(body.DelaySeconds, 0))
	if err != nil {
		log.Error().Err(err).Interface("body", body).Msg("failed to enqueue message from http")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	return c.String(http.StatusAccepted, http.StatusText(http.StatusAccepted))
}

func (s *HTTPServer) Dequeue(c echo.Context) error {
	body := HTTPDequeueRequest{}
	err := ValidateRequest(c, &body)
	if err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	tasks, err := s.UQ.Dequeue(body.Topic, int(body.Tasks), int(body.InFlightTTLSeconds))
	if err != nil {
		log.Error().Err(err).Interface("body", body).Msg("failed to dequeue message from http")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	if len(tasks) == 0 {
		return c.NoContent(http.StatusNoContent)
	}

	return c.JSON(http.StatusOK, tasks)
}

func (s *HTTPServer) DebugGetLocalTopics(c echo.Context) error {
	topicLengths := s.UQ.getTopicLengths()
	return c.JSON(http.StatusOK, topicLengths)
}

func (s *HTTPServer) DebugGetRemoteTopics(c echo.Context) error {
	s.GM.RemotePartitionTopicIndexMu.Lock()
	defer s.GM.RemotePartitionTopicIndexMu.Unlock()

	return c.JSON(http.StatusOK, s.GM.RemotePartitionTopicIndex)
}

func (s *HTTPServer) Ack(c echo.Context) error {
	body := HTTPAckRequest{}
	err := ValidateRequest(c, &body)
	if err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	err = s.UQ.Ack(body.TaskID)
	if err != nil {
		log.Error().Err(err).Interface("body", body).Msg("failed to ack from http")
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusAccepted)
}

func (s *HTTPServer) Nack(c echo.Context) error {
	body := HTTPNackRequest{}
	err := ValidateRequest(c, &body)
	if err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	err = s.UQ.Nack(body.TaskID, int(utils.DefaultInt32(body.DelaySeconds, 0)))
	if err != nil {
		log.Error().Err(err).Interface("body", body).Msg("failed to nack from http")
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusAccepted)
}
