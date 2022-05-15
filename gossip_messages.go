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

type PartitionAddressAdvertise struct {
	Partition string
	Address   string
}

// On new node join event, we advertise our address and port
func NewPartitionAddressAdvertise(partition, address string) *PartitionAddressAdvertise {
	return &PartitionAddressAdvertise{
		Partition: partition,
		Address:   address,
	}
}
