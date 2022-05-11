package gossip

import (
	"flag"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog/log"
)

var (
	mtx        sync.RWMutex
	members    = flag.String("members", "", "comma seperated list of members")
	port       = flag.Int("port", 4001, "http port")
	items      = map[string]string{}
	broadcasts *memberlist.TransmitLimitedQueue
)

type GossipManager struct {
	NodeID string

	MemberList *memberlist.Memberlist

	// Mapping of partition
	PartitionIndex map[string]*GossipNode
	broadcasts     *memberlist.TransmitLimitedQueue
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

// func init() {
// 	flag.Parse()
// }

// func start() error {
// 	hostname, _ := os.Hostname()
// 	c := memberlist.DefaultLocalConfig()
// 	c.Events = &eventDelegate{}
// 	c.Delegate = &delegate{}
// 	c.BindPort = 0
// 	c.Name = hostname + "-" + nanoid.Must()
// 	m, err := memberlist.Create(c)
// 	if err != nil {
// 		return err
// 	}
// 	if len(*members) > 0 {
// 		parts := strings.Split(*members, ",")
// 		_, err := m.Join(parts)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	broadcasts = &memberlist.TransmitLimitedQueue{
// 		NumNodes: func() int {
// 			return m.NumMembers()
// 		},
// 		RetransmitMult: 3,
// 	}
// 	node := m.LocalNode()
// 	fmt.Printf("Local member %s:%d\n", node.Addr, node.Port)
// 	return nil
// }

func NewGossipManager(partitionID, advertiseAddress string, port int, existingMembers []string) (gm *GossipManager, err error) {
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

	return gm, nil
}
