package server

import (
	"context"
	"strings"
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
	SourceNode Node `json:"source"`
	DestNode   Node `json:"target"`
}

type nodeStats struct {
	degree           int
	telemetrySession string
}
type NetworkGraph struct {
	ngMutex        *deadlock.RWMutex
	networkMap     map[string]map[string]bool
	outgoingMap    map[string]map[string]bool
	incomingMap    map[string]map[string]bool
	nodeMap        map[string]*nodeStats
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
		nodeMap:     make(map[string]*nodeStats),
		handler:     handler,
		esClient:    esClient,
		dequeChan:   make(chan Hit),
		catchup:     true,
	}
	return ng
}

func logEdgeWithNodeDegree(srcDegree int, destDegree int) *log.Entry {
	return log.WithFields(log.Fields{
		"src degree":  srcDegree,
		"dest degree": destDegree,
	})
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

		eventHostInstanceName := connectPeerDetails.InstanceName
		sourceNode := formatNodeName(eventHost, eventHostInstanceName)
		sourceSession := connectPeerDetails.Session
		destNode := formatNodeName(connectPeerDetails.Details.HostName, connectPeerDetails.Details.InstanceName)
		destSession := ""

		networkMap := ng.outgoingMap
		invNetworkMap := ng.incomingMap

		if connectPeerDetails.Details.Incoming {
			networkMap = ng.incomingMap
			invNetworkMap = ng.outgoingMap
		}

		_, exists := networkMap[sourceNode]
		if !exists {
			networkMap[sourceNode] = make(map[string]bool)
		}

		_, exists = invNetworkMap[destNode]
		if !exists {
			invNetworkMap[destNode] = make(map[string]bool)
		}

		_, exists = networkMap[sourceNode][destNode]
		if !exists {
			networkMap[sourceNode][destNode] = true
			invNetworkMap[destNode][sourceNode] = true
			nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, 1, sourceSession)
			if nodeMsg != nil {
				additions = append(additions, nodeMsg)
				log.WithField("degree", ng.nodeMap[sourceNode].degree).Debug(nodeMsg)
			}
			nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, destNode, 1, destSession)
			if nodeMsg != nil {
				additions = append(additions, nodeMsg)
				log.WithField("degree", ng.nodeMap[destNode].degree).Debug(nodeMsg)
			}

			var addEdgeMsg AddEdge
			if connectPeerDetails.Details.Incoming {
				addEdgeMsg = MakeAddEdge(destNode, destSession, sourceNode, sourceSession)
				updateNetworkMap(ng.networkMap, destNode, sourceNode, true)
				logEdgeWithNodeDegree(ng.nodeMap[destNode].degree, ng.nodeMap[sourceNode].degree).Debug(addEdgeMsg)
			} else {
				addEdgeMsg = MakeAddEdge(sourceNode, sourceSession, destNode, destSession)
				updateNetworkMap(ng.networkMap, sourceNode, destNode, true)
				logEdgeWithNodeDegree(ng.nodeMap[sourceNode].degree, ng.nodeMap[destNode].degree).Debug(addEdgeMsg)
			}
			additions = append(additions, addEdgeMsg)
		}
	case DisconnectPeerMessage:
		disconnectPeerDetails, ok := queryHit.Source.ParsedData.(DisconnectPeerDetails)
		if !ok {
			log.Error("QueryHit type conversion error: ", queryHit.Source.Message)
			return additions, deletions
		}
		eventHostInstanceName := disconnectPeerDetails.InstanceName
		sourceNode := formatNodeName(eventHost, eventHostInstanceName)
		sourceSession := disconnectPeerDetails.Session
		destNode := formatNodeName(disconnectPeerDetails.Details.HostName, disconnectPeerDetails.Details.InstanceName)
		destSession := ""

		networkMap := ng.outgoingMap
		invNetworkMap := ng.incomingMap

		if disconnectPeerDetails.Details.Incoming {
			networkMap = ng.incomingMap
			invNetworkMap = ng.outgoingMap
		}
		_, exists := networkMap[sourceNode]
		// source not present, return
		if !exists {
			break
		}
		_, exists = networkMap[sourceNode][destNode]
		if exists {
			delete(networkMap[sourceNode], destNode)
			delete(invNetworkMap[destNode], sourceNode)
			nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, -1, sourceSession)
			if nodeMsg != nil {
				deletions = append(deletions, nodeMsg)
				log.WithField("degree", 0).Debug(nodeMsg)
			}

			nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, destNode, -1, destSession)
			if nodeMsg != nil {
				deletions = append(deletions, nodeMsg)
				log.WithField("degree", 0).Debug(nodeMsg)
			}

			var removeEdgeMsg RemoveEdge
			if disconnectPeerDetails.Details.Incoming {
				removeEdgeMsg = MakeRemoveEdge(destNode, destSession, sourceNode, sourceSession)
				updateNetworkMap(ng.networkMap, destNode, sourceNode, false)
				logEdgeWithNodeDegree(ng.nodeMap[destNode].degree, ng.nodeMap[sourceNode].degree).Debug(removeEdgeMsg)
			} else {
				removeEdgeMsg = MakeRemoveEdge(sourceNode, sourceSession, destNode, destSession)
				updateNetworkMap(ng.networkMap, sourceNode, destNode, false)
				logEdgeWithNodeDegree(ng.nodeMap[sourceNode].degree, ng.nodeMap[destNode].degree).Debug(removeEdgeMsg)
			}
			deletions = append(deletions, removeEdgeMsg)

		}
	case PeerConnectionsMessage:
		peerConnectionsDetails, ok := queryHit.Source.ParsedData.(PeerConnectionDetails)
		if !ok {
			log.Error("QueryHit type conversion error: ", queryHit.Source.Message)
			return additions, deletions
		}
		eventHostInstanceName := peerConnectionsDetails.InstanceName
		sourceNode := formatNodeName(eventHost, eventHostInstanceName)
		sourceSession := peerConnectionsDetails.Session

		latestIncomingMap := make(map[string]bool)
		for _, peer := range peerConnectionsDetails.Details.IncomingPeers {
			peerNode := formatNodeName(peer.HostName, peer.InstanceName)
			latestIncomingMap[peerNode] = true
		}

		// calculate additions for incoming peers
		for peerHostname := range latestIncomingMap {
			if _, exists := ng.incomingMap[sourceNode][peerHostname]; !exists {
				// check if mapping exists in inverse map
				createOrUpdateDirectionMap(ng.incomingMap, sourceNode, peerHostname, true)
				createOrUpdateDirectionMap(ng.outgoingMap, peerHostname, sourceNode, true)

				nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, 1, sourceSession)
				if nodeMsg != nil {
					additions = append(additions, nodeMsg)
					log.WithField("degree", ng.nodeMap[sourceNode].degree).Debug(nodeMsg)
				}
				nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, peerHostname, 1, "")
				if nodeMsg != nil {
					additions = append(additions, nodeMsg)
					log.WithField("degree", ng.nodeMap[peerHostname]).Debug(nodeMsg)
				}

				addEdgeMsg := MakeAddEdge(peerHostname, "", sourceNode, sourceSession)
				additions = append(additions, addEdgeMsg)
				logEdgeWithNodeDegree(ng.nodeMap[peerHostname].degree, ng.nodeMap[sourceNode].degree).Debug(addEdgeMsg)
				updateNetworkMap(ng.networkMap, peerHostname, sourceNode, true)

			}
		}

		// calculate deletions for incoming peers
		// for peerHostname := range ng.incomingMap[sourceNode] {
		// 	if _, exists := latestIncomingMap[peerHostname]; !exists {
		// 		createOrUpdateDirectionMap(ng.incomingMap, sourceNode, peerHostname, false)
		// 		createOrUpdateDirectionMap(ng.outgoingMap, peerHostname, sourceNode, false)

		// 		nodeMsg := checkNodeMapForNodeChanges(&ng.nodeMap, sourceNode, -1)
		// 		if nodeMsg != nil {
		// 			deletions = append(deletions, nodeMsg)
		// 			log.WithField("degree", ng.nodeMap[sourceNode]).Debug(nodeMsg)
		// 		}
		// 		nodeMsg = checkNodeMapForNodeChanges(&ng.nodeMap, peerHostname, -1)
		// 		if nodeMsg != nil {
		// 			deletions = append(deletions, nodeMsg)
		// 			log.WithField("degree", ng.nodeMap[peerHostname]).Debug(nodeMsg)
		// 		}
		// 		removeEdgeMsg := MakeRemoveEdge(peerHostname, sourceNode)
		// 		deletions = append(deletions, removeEdgeMsg)
		// 		logEdgeWithNodeDegree(ng.nodeMap[peerHostname], ng.nodeMap[sourceNode]).Debug(removeEdgeMsg)
		// 		updateNetworkMap(ng.networkMap, peerHostname, sourceNode, false)
		// 	}
		// }

		for peerHostname := range ng.outgoingMap {
			if _, exists := ng.outgoingMap[peerHostname][sourceNode]; exists {
				if _, existsInLatestMap := latestIncomingMap[peerHostname]; !existsInLatestMap {
					createOrUpdateDirectionMap(ng.incomingMap, sourceNode, peerHostname, false)
					createOrUpdateDirectionMap(ng.outgoingMap, peerHostname, sourceNode, false)

					nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, -1, sourceSession)
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
						log.WithField("degree", ng.nodeMap[sourceNode].degree).Debug(nodeMsg)
					}
					nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, peerHostname, -1, "")
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
						log.WithField("degree", ng.nodeMap[peerHostname].degree).Debug(nodeMsg)
					}
					removeEdgeMsg := MakeRemoveEdge(peerHostname, "", sourceNode, sourceSession)
					deletions = append(deletions, removeEdgeMsg)
					logEdgeWithNodeDegree(ng.nodeMap[peerHostname].degree, ng.nodeMap[sourceNode].degree).Debug(removeEdgeMsg)
					updateNetworkMap(ng.networkMap, peerHostname, sourceNode, false)
				}
			}
		}

		latestOutgoingMap := make(map[string]bool)
		for _, peer := range peerConnectionsDetails.Details.OutgoingPeers {
			peerNode := formatNodeName(peer.HostName, peer.InstanceName)
			latestOutgoingMap[peerNode] = true
		}

		// calculate additions for outgoing peers
		for peerHostname := range latestOutgoingMap {
			if _, exists := ng.outgoingMap[sourceNode][peerHostname]; !exists {
				createOrUpdateDirectionMap(ng.outgoingMap, sourceNode, peerHostname, true)
				createOrUpdateDirectionMap(ng.incomingMap, peerHostname, sourceNode, true)
				nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, 1, sourceSession)
				if nodeMsg != nil {
					additions = append(additions, nodeMsg)
					log.WithField("degree", ng.nodeMap[sourceNode].degree).Debug(nodeMsg)
				}
				nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, peerHostname, 1, "")
				if nodeMsg != nil {
					additions = append(additions, nodeMsg)
					log.WithField("degree", ng.nodeMap[peerHostname].degree).Debug(nodeMsg)
				}

				addEdgeMsg := MakeAddEdge(sourceNode, sourceSession, peerHostname, "")
				additions = append(additions, addEdgeMsg)
				logEdgeWithNodeDegree(ng.nodeMap[sourceNode].degree, ng.nodeMap[peerHostname].degree).Debug(addEdgeMsg)
				updateNetworkMap(ng.networkMap, sourceNode, peerHostname, true)
			}
		}

		// calculate deletions for outgoing peers
		// for peerHostname := range ng.outgoingMap[sourceNode] {
		// 	if _, exists := latestOutgoingMap[peerHostname]; !exists {
		// 		createOrUpdateDirectionMap(ng.outgoingMap, sourceNode, peerHostname, false)
		// 		createOrUpdateDirectionMap(ng.incomingMap, peerHostname, sourceNode, false)

		// 		nodeMsg := checkNodeMapForNodeChanges(&ng.nodeMap, sourceNode, -1)
		// 		if nodeMsg != nil {
		// 			deletions = append(deletions, nodeMsg)
		// 			log.WithField("degree", ng.nodeMap[sourceNode]).Debug(nodeMsg)
		// 		}
		// 		nodeMsg = checkNodeMapForNodeChanges(&ng.nodeMap, peerHostname, -1)
		// 		if nodeMsg != nil {
		// 			deletions = append(deletions, nodeMsg)
		// 			log.WithField("degree", ng.nodeMap[peerHostname]).Debug(nodeMsg)
		// 		}

		// 		removeEdgeMsg := MakeRemoveEdge(sourceNode, peerHostname)
		// 		deletions = append(deletions, removeEdgeMsg)
		// 		logEdgeWithNodeDegree(ng.nodeMap[sourceNode], ng.nodeMap[peerHostname]).Debug(removeEdgeMsg)
		// 		updateNetworkMap(ng.networkMap, sourceNode, peerHostname, false)
		// 	}
		// }

		for peerHostname := range ng.incomingMap {
			if _, exists := ng.incomingMap[peerHostname][sourceNode]; exists {
				if _, existsInLatestMap := latestOutgoingMap[peerHostname]; !existsInLatestMap {
					createOrUpdateDirectionMap(ng.outgoingMap, sourceNode, peerHostname, false)
					createOrUpdateDirectionMap(ng.incomingMap, peerHostname, sourceNode, false)

					nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, -1, sourceSession)
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
						log.WithField("degree", ng.nodeMap[sourceNode].degree).Debug(nodeMsg)
					}
					nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, peerHostname, -1, "")
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
						log.WithField("degree", ng.nodeMap[peerHostname].degree).Debug(nodeMsg)
					}
					removeEdgeMsg := MakeRemoveEdge(sourceNode, sourceSession, peerHostname, "")
					deletions = append(deletions, removeEdgeMsg)
					logEdgeWithNodeDegree(ng.nodeMap[sourceNode].degree, ng.nodeMap[peerHostname].degree).Debug(removeEdgeMsg)
					updateNetworkMap(ng.networkMap, sourceNode, peerHostname, false)
				}
			}
		}

	}
	return additions, deletions
}

func formatNodeName(telemetryGUID string, telemetryInstance string) string {
	var nodeNameBuilder strings.Builder
	split := strings.Split(telemetryGUID, ":")
	for _, str := range split {
		nodeNameBuilder.WriteString(str)
		nodeNameBuilder.WriteString(":")
	}
	// nodeNameBuilder.WriteString(":")

	// check if split contains any relay names
	// if len(split) > 1 && strings.HasPrefix(split[1], "R") {
	// 	if CurrentConfig.SkipTelemetryInstanceForRelays {
	// 		telemetryInstance = ""
	// 	}
	// }

	nodeNameBuilder.WriteString(telemetryInstance)
	return nodeNameBuilder.String()
}

func createOrUpdateDirectionMap(directionMap map[string]map[string]bool, srcNode string, destNode string, value bool) {
	// check if srcNode exists
	if _, exists := directionMap[srcNode]; !exists {
		directionMap[srcNode] = make(map[string]bool)
	}
	if !value {
		delete(directionMap[srcNode], destNode)
		return
	}
	directionMap[srcNode][destNode] = value
}

func getNodeTelemetrySession(nodeMap map[string]*nodeStats, node string) string {
	return nodeMap[node].telemetrySession
}

func checkNodeMapForNodeChanges(nodeMap map[string]*nodeStats, node string, count int, session string) interface{} {
	if _, exists := nodeMap[node]; !exists {
		nodeMap[node] = &nodeStats{
			degree:           1,
			telemetrySession: session,
		}
		addNodeMsg := MakeAddNode(node, session)
		return addNodeMsg
	}
	nodeMap[node].degree += count
	if session != "" {
		nodeMap[node].telemetrySession = session
	}
	if degree := nodeMap[node].degree; degree == 0 {
		delete(nodeMap, node)
		removeNodeMsg := MakeRemoveNode(node, session)
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
			nwGraphMsg.Edges = append(nwGraphMsg.Edges, nwGraphMsgEdge{SourceNode: Node{
				Name:             srcNode,
				TelemetrySession: getNodeTelemetrySession(ng.nodeMap, srcNode),
			}, DestNode: Node{
				Name:             destNode,
				TelemetrySession: getNodeTelemetrySession(ng.nodeMap, destNode),
			}})
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
