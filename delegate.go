package main

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

type delegate struct {
	GossipManager *GossipManager
}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {
	log.Debug().Str("nodeID", d.GossipManager.NodeID).Str("broadcast", string(b)).Msg("Got msg")
	if len(b) == 0 {
		return
	}

	dec := msgpack.NewDecoder(bytes.NewBuffer(b))
	msgType, err := dec.Query("Type")
	if err != nil {
		log.Error().Err(err).Str("partition", d.GossipManager.UltraQ.Partition).Msg("failed to get delegate msg 'Type'")
		return
	}

	msgTypeStr, ok := msgType[0].(string)
	if !ok {
		log.Error().Err(err).Str("partition", d.GossipManager.UltraQ.Partition).Interface("msgType", msgType).Msg("failed to parse msg type to string")
		return
	}

	// TODO: Handle duplicate messages from gossip so we don't triple process them?
	switch msgTypeStr {
	case "ptlu":
		var ptlu *PartitionTopicLengthUpdate
		err := msgpack.Unmarshal(b, &ptlu)
		if err != nil {
			log.Error().Err(err).Str("partition", d.GossipManager.UltraQ.Partition).Msg("failed to unmarshal msgpack")
			return
		}
		// TODO: Remove log line
		log.Debug().Str("partition", d.GossipManager.UltraQ.Partition).Interface("ptlu", ptlu).Msg("unpacked partition topic length update")
		go d.GossipManager.putIndexRemotePartitionTopicLength(ptlu.Partition, ptlu.Topic, ptlu.Length)

	case "paa":
		var paa *PartitionAddressAdvertise
		err := msgpack.Unmarshal(b, &paa)
		if err != nil {
			log.Error().Err(err).Str("partition", d.GossipManager.UltraQ.Partition).Msg("failed to unmarshal msgpack")
			return
		}
		// TODO: Remove log line
		log.Debug().Str("partition", d.GossipManager.UltraQ.Partition).Interface("paa", paa).Msg("unpacked partition advertise address message")
		if paa.Partition == d.GossipManager.UltraQ.Partition {
			log.Warn().Str("partition", d.GossipManager.UltraQ.Partition).Msg("Got paa for self, ignoring")
			return
		}
		d.GossipManager.PartitionIndexMu.Lock()
		defer d.GossipManager.PartitionIndexMu.Unlock()
		d.GossipManager.PartitionIndex[paa.Partition] = &GossipNode{
			NodeID:           paa.Partition,
			AdvertiseAddress: paa.Address,
			AdvertisePort:    paa.Port,
			LastUpdated:      time.Now(),
		}

	default:
		log.Error().Err(err).Str("partition", d.GossipManager.UltraQ.Partition).Str("msgType", msgTypeStr).Msg("unknown message type")
		return
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.GossipManager.broadcasts.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(join bool) []byte {
	mtx.RLock()
	m := items
	mtx.RUnlock()
	b, _ := json.Marshal(m)
	return b
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	log.Debug().Str("partition", d.GossipManager.UltraQ.Partition).Msg("merging remote state")
	if len(buf) == 0 {
		return
	}
	if !join {
		return
	}
	var m map[string]string
	if err := json.Unmarshal(buf, &m); err != nil {
		return
	}
	mtx.Lock()
	for k, v := range m {
		items[k] = v
	}
	mtx.Unlock()
}
