package server

import (
	"context"
	"sync"
	"time"

	"github.com/algorand/go-deadlock"
	log "github.com/sirupsen/logrus"
)

type NetworkGraphMessage struct {
	MessageType string           `json:"message"`
	Nodes       []string         `json:"nodes"`
	Edges       []nwGraphMsgEdge `json:"edges"`
}

type nwGraphMsgEdge struct {
	SourceNode string `json:"source"`
	DestNode   string `json:"target"`
}

type NetworkGraph struct {
	ngMutex        *deadlock.RWMutex
	networkMap     map[string]map[string]bool
	invNetworkMap  map[string]map[string]bool
	nodeMap        map[string]int
	handler        *ConnectionHandler
	esClient       *ESClient
	dequeChan      chan Hit
	messageLatency float64
	catchup        bool
}

func MakeNetworkGraph(handler *ConnectionHandler, esClient *ESClient) *NetworkGraph {
	ng := &NetworkGraph{
		ngMutex:       &deadlock.RWMutex{},
		networkMap:    make(map[string]map[string]bool),
		invNetworkMap: make(map[string]map[string]bool),
		nodeMap:       make(map[string]int),
		handler:       handler,
		esClient:      esClient,
		dequeChan:     make(chan Hit),
		catchup:       true,
	}
	return ng
}

func (ng *NetworkGraph) dequeueTelemetryEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Debug("Leaving dequeueTelemetryEvents")
			return
		default:
		}
		message, err := ng.esClient.telemetryMessageQueue.DequeueOrWaitForNextElement()
		if err != nil {
			log.Error("Error when dequeing telemetry events: ", err)
		}
		hit, ok := message.(Hit)
		if !ok {
			log.Error("Cannot convert ", message, "to Hit type")
		}
		ng.esClient.metrics.metricsMutex.Lock()
		lastTimestamp, err := time.Parse(time.RFC3339Nano, ng.esClient.metrics.latestEnqueuedTimestamp)
		if err != nil {
			log.Error("time.Parse(): ", err)
			lastTimestamp = time.Time{}
		}
		hitTimestamp, err := time.Parse(time.RFC3339Nano, hit.Source.Timestamp)
		if err != nil {
			log.Error("time.Parse(): ", err)
			hitTimestamp = time.Time{}
		}
		if !lastTimestamp.IsZero() && !hitTimestamp.IsZero() {
			ng.messageLatency = lastTimestamp.Sub(hitTimestamp).Seconds()
		}
		// if ng.messageLatency < 0 {
		// 	log.Debug("Last TS: ", lastTimestamp.Format(time.RFC3339Nano))
		// 	log.Debug("Hit TS: ", hitTimestamp.Format(time.RFC3339Nano))
		// }
		ng.esClient.metrics.metricsMutex.Unlock()
		ng.dequeChan <- hit
	}
}

func (ng *NetworkGraph) calculateMapDelta(queryHit Hit) ([]interface{}, []interface{}) {
	// ng.ngMutex.Lock()
	// defer ng.ngMutex.Unlock()

	eventHost := queryHit.Source.Host
	var additions []interface{}
	var deletions []interface{}
	switch queryHit.Source.Message {
	case ConnectPeerMessage:
		connectPeerDetails, ok := queryHit.Source.ParsedData.(ConnectPeerDetails)
		if !ok {
			log.Error("QueryHit type conversion error: ", queryHit.Source.Message)
			return additions, deletions
		}
		sourceNode := eventHost
		destNode := connectPeerDetails.Details.HostName
		networkMap := &ng.networkMap

		if connectPeerDetails.Details.Incoming {
			networkMap = &ng.invNetworkMap
		}
		_, exists := ng.nodeMap[sourceNode]
		if !exists {
			// source node doesn't exist, send add node message
			ng.nodeMap[sourceNode] = 0
			addNodeMsg := MakeAddNode(sourceNode)
			additions = append(additions, addNodeMsg)
		}
		_, exists = (*networkMap)[sourceNode]
		if !exists {
			(*networkMap)[sourceNode] = make(map[string]bool)
		}
		_, exists = ng.nodeMap[destNode]
		if !exists {
			// dest node doesn't exist, send add node message
			ng.nodeMap[destNode] = 0
			addNodeMsg := MakeAddNode(destNode)
			additions = append(additions, addNodeMsg)
		}
		_, exists = (*networkMap)[sourceNode][destNode]
		if !exists {
			(*networkMap)[sourceNode][destNode] = true
			ng.nodeMap[sourceNode]++
			ng.nodeMap[destNode]++
			addEdgeMsg := MakeAddEdge(sourceNode, destNode)
			additions = append(additions, addEdgeMsg)
		}
	case DisconnectPeerMessage:
		disconnectPeerDetails, ok := queryHit.Source.ParsedData.(DisconnectPeerDetails)
		if !ok {
			log.Error("QueryHit type conversion error: ", queryHit.Source.Message)
			return additions, deletions
		}
		sourceNode := eventHost
		destNode := disconnectPeerDetails.Details.HostName
		networkMap := &ng.networkMap

		if disconnectPeerDetails.Details.Incoming {
			networkMap = &ng.invNetworkMap
		}
		_, exists := (*networkMap)[sourceNode]
		// source not present, return
		if !exists {
			break
		}
		_, exists = (*networkMap)[destNode]
		if exists {
			delete((*networkMap), destNode)
			ng.nodeMap[sourceNode]--
			if ng.nodeMap[sourceNode] == 0 {
				removeNodeMsg := MakeRemoveNode(sourceNode)
				deletions = append(deletions, removeNodeMsg)
			}
			ng.nodeMap[destNode]--
			if ng.nodeMap[destNode] == 0 {
				removeNodeMsg := MakeRemoveNode(destNode)
				deletions = append(deletions, removeNodeMsg)
			}
			removeEdgeMsg := MakeRemoveEdge(sourceNode, destNode)
			deletions = append(deletions, removeEdgeMsg)
		}
	case PeerConnectionsMessage:
		peerConnectionsDetails, ok := queryHit.Source.ParsedData.(PeerConnectionDetails)
		if !ok {
			log.Error("QueryHit type conversion error: ", queryHit.Source.Message)
			return additions, deletions
		}
		sourceNode := eventHost

		latestIncomingMap := make(map[string]bool)
		for _, peer := range peerConnectionsDetails.Details.IncomingPeers {
			latestIncomingMap[peer.HostName] = true
		}

		// calculate additions for incoming peers
		for peerHostname := range latestIncomingMap {
			if _, exists := ng.invNetworkMap[sourceNode][peerHostname]; !exists {
				addEdgeMsg := MakeAddEdge(peerHostname, sourceNode)
				additions = append(additions, addEdgeMsg)
			}
		}

		// calculate deletions for incoming peers
		for peerHostname := range ng.invNetworkMap[sourceNode] {
			if _, exists := latestIncomingMap[peerHostname]; !exists {
				removeEdgeMsg := MakeRemoveEdge(peerHostname, sourceNode)
				deletions = append(deletions, removeEdgeMsg)
			}
		}

		ng.invNetworkMap[sourceNode] = latestIncomingMap

		latestOutgoingMap := make(map[string]bool)
		for _, peer := range peerConnectionsDetails.Details.OutgoingPeers {
			latestOutgoingMap[peer.HostName] = true
		}

		// calculate additions for outgoing peers
		for peerHostname := range latestOutgoingMap {
			if _, exists := ng.networkMap[sourceNode][peerHostname]; !exists {
				addEdgeMsg := MakeAddEdge(sourceNode, peerHostname)
				additions = append(additions, addEdgeMsg)
			}
		}

		// calculate deletions for outgoing peers
		for peerHostname := range ng.networkMap[sourceNode] {
			if _, exists := latestOutgoingMap[peerHostname]; !exists {
				removeEdgeMsg := MakeRemoveEdge(sourceNode, peerHostname)
				deletions = append(deletions, removeEdgeMsg)
			}
		}

		ng.networkMap[sourceNode] = latestOutgoingMap

	}
	return additions, deletions
}

func (ng *NetworkGraph) makeNetworkGraphMessage() interface{} {
	// ng.ngMutex.Lock()
	// defer ng.ngMutex.Unlock()

	nwGraphMsg := NetworkGraphMessage{
		MessageType: "NetworkGraph",
	}

	for node := range ng.nodeMap {
		nwGraphMsg.Nodes = append(nwGraphMsg.Nodes, node)
	}

	for srcNode, destMap := range ng.networkMap {
		for destNode := range destMap {
			nwGraphMsg.Edges = append(nwGraphMsg.Edges, nwGraphMsgEdge{SourceNode: srcNode, DestNode: destNode})
		}
	}

	for srcNode, destMap := range ng.invNetworkMap {
		for destNode := range destMap {
			nwGraphMsg.Edges = append(nwGraphMsg.Edges, nwGraphMsgEdge{SourceNode: srcNode, DestNode: destNode})
		}
	}

	return nwGraphMsg
}

func (ng *NetworkGraph) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	go ng.dequeueTelemetryEvents(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Info("Terminating network graph refresh service")
			return
		case hit := <-ng.dequeChan:
			additions, deletions := ng.calculateMapDelta(hit)
			ng.esClient.metrics.metricsMutex.RLock()
			// log.Debug("TelemetryMessageQueue processing latency: ", ng.messageLatency)
			if ng.messageLatency > float64(CurrentConfig.MessageQueueThresholdLatency) {
				ng.catchup = true
				ng.esClient.metrics.metricsMutex.RUnlock()
				continue
			}
			ng.esClient.metrics.metricsMutex.RUnlock()
			if ng.catchup {
				ng.catchup = false
				message := ng.makeNetworkGraphMessage()
				ng.handler.broadcastMessage(message)
				continue
			}
			for _, message := range additions {
				ng.handler.broadcastMessage(message)
			}
			for _, message := range deletions {
				ng.handler.broadcastMessage(message)
			}
		case client := <-ng.handler.bootstrapChan:
			if !ng.catchup {
				message := ng.makeNetworkGraphMessage()
				ng.handler.unicastMessage(message, client)
			}
		}
	}
}
