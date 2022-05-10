package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	InFlightMessages       int64 = 0
	InFlightMessagesMetric       = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "in_flight_messages",
		Help: "The current number of in-flight messages",
	})

	QueuedMessages       int64 = 0
	QueuedMessagesMetric       = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "queued_messages",
		Help: "The current number of queued messages",
	})

	QueueMessageSizeMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "queue_message_size",
		Help:    "The size in bytes of the messages queued",
		Buckets: prometheus.ExponentialBuckets(10, 2, 8),
	})

	QueueMaxLen       int64 = 0
	QueueMaxLenMetric       = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "queue_max_len",
		Help: "The max number of queued messages allowed in the partition",
	})

	DelayedMessages       int64 = 0
	DelayedMessagesMetric       = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "delayed_messages",
		Help: "The current number of delayed messages",
	})

	TimedoutMessages       int64 = 0
	TimedoutMessagesMetric       = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "timedout_messages",
		Help: "The number of timedout messages",
	})

	AckedMessages        int64 = 0
	AckedMessagesCounter       = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "acked_messages",
		Help: "The number of acked messages",
	})

	NackedMessages        int64 = 0
	NackedMessagesCounter       = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "nacked_messages",
		Help: "The number of nacked messages",
	})

	PostRecordRequests       int64 = 0
	PostRecordRequestCounter       = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "post_record_reqs",
		Help: "The total number of POST /record requests",
	})

	PostRecordLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "post_record_latency",
		Help:    "The latency of POST /record requests in milliseconds",
		Buckets: prometheus.ExponentialBuckets(1, 3, 10),
	})

	GetRecordRequests       int64 = 0
	GetRecordRequestCounter       = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "get_record_reqs",
		Help: "The total number of GET /record requests",
	})

	GetRecordLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "get_record_latency",
		Help:    "The latency of GET /record requests in milliseconds",
		Buckets: prometheus.ExponentialBuckets(1, 3, 10),
	})

	AckMisses        int64 = 0
	AckMissesCounter       = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ack_misses",
		Help: "The number of POST /ack requests that miss",
	})

	AckLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "ack_record_latency",
		Help:    "The latency of POST /ack requests in milliseconds",
		Buckets: prometheus.ExponentialBuckets(1, 3, 10),
	})

	NackMisses        int64 = 0
	NackMissesCounter       = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "nack_misses",
		Help: "The number of POST /nack requests that miss",
	})

	NackLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "nack_record_latency",
		Help:    "The latency of POST /nack requests in milliseconds",
		Buckets: prometheus.ExponentialBuckets(1, 3, 10),
	})

	EmptyQueueResponses        int64 = 0
	EmptyQueueResponsesCounter       = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "empty_queue_responses",
		Help: "Number of empty queue responses",
	})

	FullQueueResponses        int64 = 0
	FullQueueResponsesCounter       = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "full_queue_responses",
		Help: "Number of full queue responses",
	})

	TotalRequestsCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "total_reqs",
		Help: "Total number of http requests",
	})

	HTTPResponsesMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_responses",
		Help: "Total number of http requests",
	}, []string{"code", "endpoint"})
)

func SetupMetrics() {
	prometheus.Register(InFlightMessagesMetric)
	prometheus.Register(QueuedMessagesMetric)
	prometheus.Register(QueueMessageSizeMetric)
	prometheus.Register(QueueMaxLenMetric)
	prometheus.Register(DelayedMessagesMetric)
	prometheus.Register(TimedoutMessagesMetric)
	prometheus.Register(AckedMessagesCounter)
	prometheus.Register(NackedMessagesCounter)
	prometheus.Register(PostRecordRequestCounter)
	prometheus.Register(PostRecordLatency)
	prometheus.Register(GetRecordRequestCounter)
	prometheus.Register(GetRecordRequestCounter)
	prometheus.Register(AckMissesCounter)
	prometheus.Register(AckLatency)
	prometheus.Register(NackLatency)
	prometheus.Register(FullQueueResponsesCounter)
	prometheus.Register(EmptyQueueResponsesCounter)
	prometheus.Register(TotalRequestsCounter)
	prometheus.Register(HTTPResponsesMetric)
}
