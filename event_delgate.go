package main

import (
	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog/log"
)

type eventDelegate struct {
	NodeID string
}

func (ed *eventDelegate) NotifyJoin(node *memberlist.Node) {
	log.Debug().Str("nodeID", ed.NodeID).Msg("A node has joined: " + node.String())
}

func (ed *eventDelegate) NotifyLeave(node *memberlist.Node) {
	log.Debug().Str("nodeID", ed.NodeID).Msg("A node has left: " + node.String())
}

func (ed *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	log.Debug().Str("nodeID", ed.NodeID).Msg("A node was updated: " + node.String())
}
