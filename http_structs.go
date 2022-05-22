package main

type HTTPEnqueueRequest struct {
	Topics []string `json:"topics"`

	// b64 encoded byte string
	Payload string `json:"payload"`

	DelaySeconds *int32 `json:"delay_seconds"`
	Priority     *int32 `json:"priority"`
}

type HTTPDequeueRequest struct {
	Topic              string `json:"topic"`
	InFlightTTLSeconds int32  `json:"in_flight_ttl"`
	Tasks              int32  `json:"tasks"`
}

type HTTPAckRequest struct {
	TaskID string `json:"task_id"`
}

type HTTPNackRequest struct {
	TaskID       string `json:"task_id"`
	DelaySeconds *int32 `json:"delay_seconds"`
}
