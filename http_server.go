package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/danthegoodman1/UltraQueue/pb"
	"github.com/danthegoodman1/UltraQueue/utils"
	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	httpServer *HTTPServer
)

func StartHTTPServer(lis net.Listener, uq *UltraQueue, gm *GossipManager) {
	echoInstance := echo.New()
	httpServer = &HTTPServer{
		Echo: echoInstance,
		UQ:   uq,
		GM:   gm,
	}
	httpServer.Echo.HideBanner = true
	httpServer.Echo.HidePort = true
	// httpServer.Echo.Use(middleware.Logger())
	config := middleware.LoggerConfig{
		Skipper: middleware.DefaultSkipper,
		Format: `{"time":"${time_rfc3339_nano}","id":"${id}","remote_ip":"${remote_ip}",` +
			`"host":"${host}","method":"${method}","uri":"${uri}","user_agent":"${user_agent}",` +
			`"status":${status},"error":"${error}","latency":${latency},"latency_human":"${latency_human}"` +
			`,"bytes_in":${bytes_in},"bytes_out":${bytes_out},"proto":"${protocol}"}` + "\n",
		CustomTimeFormat: "2006-01-02 15:04:05.00000",
		Output:           log.Logger,
	}
	httpServer.Echo.Use(middleware.LoggerWithConfig(config))
	httpServer.Echo.Validator = &CustomValidator{validator: validator.New()}

	// Health Check route
	httpServer.Echo.GET("/hc", httpServer.HealthCheck)

	httpServer.Echo.POST("/enqueue", httpServer.Enqueue)
	httpServer.Echo.POST("/dequeue", httpServer.Dequeue)
	httpServer.Echo.POST("/ack", httpServer.Ack)
	httpServer.Echo.POST("/nack", httpServer.Nack)

	debugGroup := httpServer.Echo.Group("/debug")
	debugGroup.GET("/localTopics.json", httpServer.DebugGetLocalTopics)
	debugGroup.GET("/remoteTopics.json", httpServer.DebugGetRemoteTopics)
	debugGroup.GET("/remotePartitions.json", httpServer.DebugGetRemotePartitions)

	SetupMetrics()

	log.Info().Msg("Starting HTTP API at " + lis.Addr().String())
	httpServer.Echo.Listener = lis
	server := &http2.Server{}
	err := httpServer.Echo.StartH2CServer("", server)
	if err != nil && err != http.ErrServerClosed && err != cmux.ErrServerClosed {
		log.Fatal().Err(err).Msg("Failed to start h2c server")
	}
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

	tasks, err := s.UQ.Dequeue(body.Topic, body.Tasks, body.InFlightTTLSeconds)
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

func (s *HTTPServer) DebugGetRemotePartitions(c echo.Context) error {
	s.GM.PartitionIndexMu.Lock()
	defer s.GM.PartitionIndexMu.Unlock()

	return c.JSON(http.StatusOK, s.GM.PartitionIndex)
}

func (s *HTTPServer) Ack(c echo.Context) error {
	body := HTTPAckRequest{}
	err := ValidateRequest(c, &body)
	if err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	// Check partition
	_, partition, _, err := GetTaskIDParts(body.TaskID)
	if err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	if partition == s.UQ.Partition {
		// We are the partition
		err = s.UQ.Ack(body.TaskID)
		if err != nil {
			log.Error().Err(err).Interface("body", body).Msg("failed to ack from http")
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.NoContent(http.StatusAccepted)
	}
	// Otherwise we need to route to the correct partition
	// Find the partition address
	remoteNode := s.GM.getRemotePartitionAddress(partition)
	if remoteNode == nil {
		log.Warn().Str("taskID", body.TaskID).Str("remotePartition", partition).Str("partition", s.UQ.Partition).Msg("did not find remote partition for partition")
		return c.String(http.StatusNotFound, "did not find remote partition for partition")
	}

	// Use the internal grpc interface to ack
	// TODO: Make secure based on TLS config
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", remoteNode.AdvertiseAddress, remoteNode.AdvertisePort), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Error().Err(err).Str("taskID", body.TaskID).Str("remotePartition", partition).Str("partition", s.UQ.Partition).Msg("failed to dial remote partition")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(c.Request().Context(), time.Second*20)
	defer cancel()

	client := pb.NewUltraQueueInternalClient(conn)
	_, err = client.Ack(ctx, &pb.AckRequest{
		TaskID: body.TaskID,
	})
	if err != nil {
		log.Error().Err(err).Str("taskID", body.TaskID).Str("remotePartition", partition).Str("partition", s.UQ.Partition).Msg("failed to ack remote partition")
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

	// Check partition
	_, partition, _, err := GetTaskIDParts(body.TaskID)
	if err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	if partition == s.UQ.Partition {
		err = s.UQ.Nack(body.TaskID, utils.DefaultInt32(body.DelaySeconds, 0))
		if err != nil {
			log.Error().Err(err).Interface("body", body).Msg("failed to nack from http")
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.NoContent(http.StatusAccepted)
	}

	// Otherwise we need to route to the correct partition
	// Find the partition address
	remoteNode := s.GM.getRemotePartitionAddress(partition)
	if remoteNode == nil {
		log.Warn().Str("taskID", body.TaskID).Str("remotePartition", partition).Str("partition", s.UQ.Partition).Msg("did not find remote partition for partition")
		return c.String(http.StatusNotFound, "did not find remote partition for partition")
	}

	// Use the internal grpc interface to ack
	// TODO: Make secure based on TLS config
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", remoteNode.AdvertiseAddress, remoteNode.AdvertisePort), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Error().Err(err).Str("taskID", body.TaskID).Str("remotePartition", partition).Str("partition", s.UQ.Partition).Msg("failed to dial remote partition")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(c.Request().Context(), time.Second*20)
	defer cancel()

	client := pb.NewUltraQueueInternalClient(conn)
	_, err = client.Nack(ctx, &pb.NackRequest{
		TaskID: body.TaskID,
	})
	if err != nil {
		log.Error().Err(err).Str("taskID", body.TaskID).Str("remotePartition", partition).Str("partition", s.UQ.Partition).Msg("failed to nack remote partition")
		return c.String(http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(http.StatusAccepted)
}
