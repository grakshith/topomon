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
	outgoingMap    map[string]map[string]bool
	incomingMap    map[string]map[string]bool
	nodeMap        map[string]int
	handler        *ConnectionHandler
	esClient       *ESClient
	dequeChan      chan Hit
	messageLatency float64
	catchup        bool
}

func MakeNetworkGraph(handler *ConnectionHandler, esClient *ESClient) *NetworkGraph {
	ng := &NetworkGraph{
		ngMutex:     &deadlock.RWMutex{},
		networkMap:  make(map[string]map[string]bool),
		outgoingMap: make(map[string]map[string]bool),
		incomingMap: make(map[string]map[string]bool),
		nodeMap:     make(map[string]int),
		handler:     handler,
		esClient:    esClient,
		dequeChan:   make(chan Hit),
		catchup:     true,
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
		networkMap := &ng.outgoingMap
		invNetworkMap := &ng.incomingMap

		if connectPeerDetails.Details.Incoming {
			networkMap = &ng.incomingMap
			invNetworkMap = &ng.outgoingMap
		}

		_, exists := (*networkMap)[sourceNode]
		if !exists {
			(*networkMap)[sourceNode] = make(map[string]bool)
		}

		_, exists = (*networkMap)[sourceNode][destNode]
		if !exists {
			(*networkMap)[sourceNode][destNode] = true
			if !checkInverseMappingExists(invNetworkMap, destNode, sourceNode) {
				nodeMsg := checkNodeMapForNodeChanges(&ng.nodeMap, sourceNode, 1)
				if nodeMsg != nil {
					additions = append(additions, nodeMsg)
				}
				nodeMsg = checkNodeMapForNodeChanges(&ng.nodeMap, destNode, 1)
				if nodeMsg != nil {
					additions = append(additions, nodeMsg)
				}

				var addEdgeMsg AddEdge
				if connectPeerDetails.Details.Incoming {
					addEdgeMsg = MakeAddEdge(destNode, sourceNode)
					updateNetworkMap(ng.networkMap, destNode, sourceNode, true)
				} else {
					addEdgeMsg = MakeAddEdge(sourceNode, destNode)
					updateNetworkMap(ng.networkMap, sourceNode, destNode, true)
				}
				additions = append(additions, addEdgeMsg)
			}
		}
	case DisconnectPeerMessage:
		disconnectPeerDetails, ok := queryHit.Source.ParsedData.(DisconnectPeerDetails)
		if !ok {
			log.Error("QueryHit type conversion error: ", queryHit.Source.Message)
			return additions, deletions
		}
		sourceNode := eventHost
		destNode := disconnectPeerDetails.Details.HostName
		networkMap := &ng.outgoingMap
		invNetworkMap := &ng.incomingMap

		if disconnectPeerDetails.Details.Incoming {
			networkMap = &ng.incomingMap
			invNetworkMap = &ng.outgoingMap
		}
		_, exists := (*networkMap)[sourceNode]
		// source not present, return
		if !exists {
			break
		}
		_, exists = (*networkMap)[sourceNode][destNode]
		if exists {
			delete((*networkMap)[sourceNode], destNode)
			if checkInverseMappingExists(invNetworkMap, destNode, sourceNode) {

				nodeMsg := checkNodeMapForNodeChanges(&ng.nodeMap, sourceNode, -1)
				if nodeMsg != nil {
					deletions = append(deletions, nodeMsg)
				}

				nodeMsg = checkNodeMapForNodeChanges(&ng.nodeMap, destNode, -1)
				if nodeMsg != nil {
					deletions = append(deletions, nodeMsg)
				}

				var removeEdgeMsg RemoveEdge
				if disconnectPeerDetails.Details.Incoming {
					removeEdgeMsg = MakeRemoveEdge(destNode, sourceNode)
					updateNetworkMap(ng.networkMap, destNode, sourceNode, false)
				} else {
					removeEdgeMsg = MakeRemoveEdge(sourceNode, destNode)
					updateNetworkMap(ng.networkMap, sourceNode, destNode, false)
				}
				deletions = append(deletions, removeEdgeMsg)
			}
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
			if _, exists := ng.incomingMap[sourceNode][peerHostname]; !exists {
				// check if mapping exists in inverse map
				if !checkInverseMappingExists(&ng.outgoingMap, peerHostname, sourceNode) {
					nodeMsg := checkNodeMapForNodeChanges(&ng.nodeMap, sourceNode, 1)
					if nodeMsg != nil {
						additions = append(additions, nodeMsg)
					}
					nodeMsg = checkNodeMapForNodeChanges(&ng.nodeMap, peerHostname, 1)
					if nodeMsg != nil {
						additions = append(additions, nodeMsg)
					}

					addEdgeMsg := MakeAddEdge(peerHostname, sourceNode)
					additions = append(additions, addEdgeMsg)
					updateNetworkMap(ng.networkMap, peerHostname, sourceNode, true)
				}
			}
		}

		// calculate deletions for incoming peers
		for peerHostname := range ng.incomingMap[sourceNode] {
			if _, exists := latestIncomingMap[peerHostname]; !exists {
				// check if mapping exists in inverse map
				if checkInverseMappingExists(&ng.outgoingMap, peerHostname, sourceNode) {
					nodeMsg := checkNodeMapForNodeChanges(&ng.nodeMap, sourceNode, -1)
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
					}
					nodeMsg = checkNodeMapForNodeChanges(&ng.nodeMap, peerHostname, -1)
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
					}
					removeEdgeMsg := MakeRemoveEdge(peerHostname, sourceNode)
					deletions = append(deletions, removeEdgeMsg)
					updateNetworkMap(ng.networkMap, peerHostname, sourceNode, false)
				}
			}
		}

		ng.incomingMap[sourceNode] = latestIncomingMap

		latestOutgoingMap := make(map[string]bool)
		for _, peer := range peerConnectionsDetails.Details.OutgoingPeers {
			latestOutgoingMap[peer.HostName] = true
		}

		// calculate additions for outgoing peers
		for peerHostname := range latestOutgoingMap {
			if _, exists := ng.outgoingMap[sourceNode][peerHostname]; !exists {
				if !checkInverseMappingExists(&ng.incomingMap, peerHostname, sourceNode) {
					nodeMsg := checkNodeMapForNodeChanges(&ng.nodeMap, sourceNode, 1)
					if nodeMsg != nil {
						additions = append(additions, nodeMsg)
					}
					nodeMsg = checkNodeMapForNodeChanges(&ng.nodeMap, peerHostname, 1)
					if nodeMsg != nil {
						additions = append(additions, nodeMsg)
					}

					addEdgeMsg := MakeAddEdge(sourceNode, peerHostname)
					additions = append(additions, addEdgeMsg)
					updateNetworkMap(ng.networkMap, sourceNode, peerHostname, true)
				}
			}
		}

		// calculate deletions for outgoing peers
		for peerHostname := range ng.outgoingMap[sourceNode] {
			if _, exists := latestOutgoingMap[peerHostname]; !exists {
				if checkInverseMappingExists(&ng.incomingMap, peerHostname, sourceNode) {
					nodeMsg := checkNodeMapForNodeChanges(&ng.nodeMap, sourceNode, -1)
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
					}
					nodeMsg = checkNodeMapForNodeChanges(&ng.nodeMap, peerHostname, -1)
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
					}

					removeEdgeMsg := MakeRemoveEdge(sourceNode, peerHostname)
					deletions = append(deletions, removeEdgeMsg)
					updateNetworkMap(ng.networkMap, sourceNode, peerHostname, false)
				}
			}
		}

		ng.outgoingMap[sourceNode] = latestOutgoingMap

	}
	return additions, deletions
}

func checkNodeMapForNodeChanges(nodeMap *map[string]int, node string, count int) interface{} {
	if _, exists := (*nodeMap)[node]; !exists {
		(*nodeMap)[node] = 1
		addNodeMsg := MakeAddNode(node)
		return addNodeMsg
	}
	(*nodeMap)[node] += count
	if degree := (*nodeMap)[node]; degree == 0 {
		delete(*nodeMap, node)
		removeNodeMsg := MakeRemoveNode(node)
		return removeNodeMsg
	}
	return nil
}

func updateNetworkMap(networkMap map[string]map[string]bool, sourceNode string, targetNode string, value bool) {
	_, exists := networkMap[sourceNode]
	switch value {
	case true:
		if !exists {
			networkMap[sourceNode] = make(map[string]bool)
		}
		networkMap[sourceNode][targetNode] = value
	case false:
		if exists {
			delete(networkMap[sourceNode], targetNode)
		}
	}
}

func checkInverseMappingExists(invMap *map[string]map[string]bool, srcNode string, destNode string) bool {
	if _, exists := (*invMap)[srcNode]; !exists {
		return false
	}
	if _, exists := (*invMap)[srcNode][destNode]; !exists {
		return false
	}
	return true
}

func (ng *NetworkGraph) makeNetworkGraphMessage() interface{} {
	// ng.ngMutex.Lock()
	// defer ng.ngMutex.Unlock()

	nwGraphMsg := NetworkGraphMessage{
		MessageType: "NetworkGraph",
	}

	// log.Debug("Node map: ", ng.nodeMap)
	// log.Debug("Out map: ", ng.networkMap)
	// log.Debug("In map: ", ng.invNetworkMap)

	for node := range ng.nodeMap {
		nwGraphMsg.Nodes = append(nwGraphMsg.Nodes, node)
		log.Debug("Global NW node map: ", node)
	}

	for srcNode, destMap := range ng.networkMap {
		for destNode := range destMap {
			nwGraphMsg.Edges = append(nwGraphMsg.Edges, nwGraphMsgEdge{SourceNode: srcNode, DestNode: destNode})
			log.Debug("Global NW map: ", srcNode, "->", destNode)
		}
	}

	// for srcNode, destMap := range ng.outgoingMap {
	// 	for destNode := range destMap {
	// 		nwGraphMsg.Edges = append(nwGraphMsg.Edges, nwGraphMsgEdge{SourceNode: srcNode, DestNode: destNode})
	// 	}
	// }

	// for srcNode, destMap := range ng.incomingMap {
	// 	for destNode := range destMap {
	// 		if _, exists := globalNwGraph[destNode][srcNode]; !exists {
	// 			nwGraphMsg.Edges = append(nwGraphMsg.Edges, nwGraphMsgEdge{SourceNode: destNode, DestNode: srcNode})
	// 		}
	// 	}
	// }

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
