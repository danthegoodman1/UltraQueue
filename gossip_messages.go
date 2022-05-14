package main

type PartitionTopicLengthUpdate struct {
	Topic     string
	Partition string
	Length    int

	Type string
}

func NewPartitionTopicLengthUpdate(topic, partition string, length int) *PartitionTopicLengthUpdate {
	return &PartitionTopicLengthUpdate{
		Topic:     topic,
		Partition: partition,
		Length:    length,
		Type:      "ptlu",
	}
}
