package main

import (
	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog/log"
)

type eventDelegate struct {
	gm *GossipManager
}

func (ed *eventDelegate) NotifyJoin(node *memberlist.Node) {
	log.Debug().Str("nodeID", ed.gm.NodeID).Msg("A node has joined: " + node.String())
	if ed.gm.broadcasts != nil {
		// Broadcast out advertise address and port
		go ed.gm.broadcastAdvertiseAddress()
	}
}

func (ed *eventDelegate) NotifyLeave(node *memberlist.Node) {
	log.Debug().Str("nodeID", ed.gm.NodeID).Msg("A node has left: " + node.Name)
	if node.Name != ed.gm.NodeID {
		ed.gm.deletePartitionFromIndex(node.Name)
		ed.gm.deletePartitionFromTopicIndex(node.Name)
	}
}

func (ed *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	log.Debug().Str("nodeID", ed.gm.NodeID).Msg("A node was updated: " + node.String())
}
