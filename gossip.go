package main

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	mtx   sync.RWMutex
	items = map[string]string{}
)

type GossipManager struct {
	NodeID string

	MemberList *memberlist.Memberlist

	// Mapping of partition
	PartitionIndex map[string]*GossipNode
	broadcasts     *memberlist.TransmitLimitedQueue

	UltraQ *UltraQueue

	topicPollStopChan chan chan struct{}

	// To store the last known length of the local topic, compare to see whether to send update over gossip
	topicLenCache map[string]int

	// topic->[]partition->len
	RemotePartitionTopicIndex   map[string]map[string]int
	RemotePartitionTopicIndexMu *sync.RWMutex
}

type GossipNode struct {
	NodeID           string
	AdvertiseAddress string
	LastUpdated      time.Time
}

type update struct {
	Action string // add, del
	Data   map[string]string
}

func NewGossipManager(partitionID, advertiseAddress string, uq *UltraQueue, port int, existingMembers []string) (gm *GossipManager, err error) {
	myNode := &GossipNode{
		NodeID:           partitionID,
		AdvertiseAddress: advertiseAddress,
		LastUpdated:      time.Now(),
	}

	gm = &GossipManager{
		NodeID: partitionID,
		PartitionIndex: map[string]*GossipNode{
			partitionID: myNode,
		},
		UltraQ:                      uq,
		topicPollStopChan:           make(chan chan struct{}, 1),
		topicLenCache:               make(map[string]int),
		RemotePartitionTopicIndex:   make(map[string]map[string]int),
		RemotePartitionTopicIndexMu: &sync.RWMutex{},
	}

	// Initialize memberlist
	var config *memberlist.Config
	if os.Getenv("GOSSIP_LOCAL") == "1" {
		config = memberlist.DefaultLocalConfig()
	} else {
		config = memberlist.DefaultLANConfig()
		if err != nil {
			log.Error().Err(err).Msg("Error getting gossip port from env var")
			return nil, err
		}
	}

	config.BindPort = port
	config.Events = &eventDelegate{
		NodeID: gm.NodeID,
	}
	config.Delegate = &delegate{
		GossipManager: gm,
	}
	config.Name = partitionID

	gm.MemberList, err = memberlist.Create(config)
	if err != nil {
		log.Error().Err(err).Msg("Error creating memberlist")
		return nil, err
	}

	if len(existingMembers) > 0 {
		// Join existing nodes
		joinedHosts, err := gm.MemberList.Join(existingMembers)
		if err != nil {
			log.Error().Err(err).Str("existingMembers", strings.Join(existingMembers, ",")).Msg("Error joining existing members")
			return nil, err
		}
		log.Info().Int("joinedHosts", joinedHosts).Msg("Successfully joined memberlist")
	} else {
		log.Info().Msg("Starting new memberlist cluster")
	}

	gm.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return gm.MemberList.NumMembers()
		},
		RetransmitMult: 3,
	}

	node := gm.MemberList.LocalNode()
	log.Info().Str("name", node.Name).Str("addr", node.Address()).Int("port", int(node.Port)).Msg("Node started")

	go gm.pollTopicLen(*time.NewTicker(time.Millisecond * 500))

	return gm, nil
}

func (gm *GossipManager) pollTopicLen(t time.Ticker) {
	for {
		select {
		case <-t.C:
			log.Debug().Str("partition", gm.UltraQ.Partition).Msg("Polling for changed topic lengths...")
			// poll topic lengths and add operations to queue
			topicLengths := gm.UltraQ.getTopicLengths()
			for topicName, length := range topicLengths {
				// No lock needed because this is the only thing accessing it
				if lastKnownLength, exists := gm.topicLenCache[topicName]; !exists || (exists && lastKnownLength != length) {
					// TODO: Remove this log line
					log.Debug().Str("topic", topicName).Str("partition", gm.UltraQ.Partition).Msg("Topic changed length or is new, sending over gossip")
					// Create broadcast message with length
					msg := NewPartitionTopicLengthUpdate(topicName, gm.UltraQ.Partition, length)
					b, err := msgpack.Marshal(msg)
					if err != nil {
						log.Error().Err(err).Msg("Error marshalling topic length update")
						continue
					}
					gm.broadcasts.QueueBroadcast(&broadcast{
						msg:    b,
						notify: nil,
					})
					gm.topicLenCache[topicName] = length
				}
			}
		case returnChan := <-gm.topicPollStopChan:
			log.Info().Str("partition", gm.UltraQ.Partition).Msg("Topic length poll got stop channel, exiting")
			returnChan <- struct{}{}
			return
		}
	}
}

func (gm *GossipManager) Shutdown() {
	log.Info().Str("partition", gm.UltraQ.Partition).Msg("Shutting down gossip manager...")
	returnChan := make(chan struct{}, 1)
	gm.topicPollStopChan <- returnChan
	<-returnChan
	log.Info().Str("partition", gm.UltraQ.Partition).Msg("Shut down gossip manager")
}

// Sets the local index of known remote partition topic lengths
func (gm *GossipManager) putIndexRemotePartitionTopicLength(partition, topicName string, length int) {
	gm.RemotePartitionTopicIndexMu.Lock()
	defer gm.RemotePartitionTopicIndexMu.Unlock()

	if partitionLengthMap, exists := gm.RemotePartitionTopicIndex[topicName]; exists {
		// TODO: Remove log line
		log.Debug().Str("partition", gm.UltraQ.Partition).Str("remote partition", partition).Str("topic", topicName).Int("topicLen", length).Msg("Updating existing local remote partition topic length")
		// Less operations to just set rather than read then set if not the same
		partitionLengthMap[partition] = length
	} else {
		// Create it
		// TODO: Remove log line
		log.Debug().Str("partition", gm.UltraQ.Partition).Str("remote partition", partition).Str("topic", topicName).Int("topicLen", length).Msg("Set local remote partition topic length")
		gm.RemotePartitionTopicIndex[topicName] = map[string]int{
			partition: length,
		}
	}
}
