package server

import (
	"context"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// NetworkGraphMessage consists of all the nodes and edges
// in the server network map. This is sent to bootstrap the
// client when the client connects with the server for
// the first time, or when the server performs a catchup with
// the telemetry server.
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
	// networkMap     map[string]map[string]bool
	outgoingMap    map[string]map[string]bool
	incomingMap    map[string]map[string]bool
	nodeMap        map[string]*nodeStats
	handler        *ConnectionHandler
	esClient       *ESClient
	dequeChan      chan Hit // the channel used to dequeue telemetry events
	messageLatency float64  // the time difference between the timestamps of telemetry events in both the ends of the queue
	catchup        bool     // indicates whether the server is in catchup state
}

func MakeNetworkGraph(handler *ConnectionHandler, esClient *ESClient) *NetworkGraph {
	ng := &NetworkGraph{
		// networkMap:  make(map[string]map[string]bool),
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

// dequeueTelemetryEvents dequeues telemetry events and updates the
// messageLatency
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
		ng.esClient.metrics.metricsMutex.Unlock()
		ng.dequeChan <- hit
	}
}

// calculateMapDelta calculates additions and deletions to the map based on the elasticsearch query response.
func (ng *NetworkGraph) calculateMapDelta(queryHit Hit) ([]interface{}, []interface{}) {
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
		// only one of source and destination has the session information
		destSession := ""

		// networkMap and invNetworkMap are references to outgoing and incoming maps
		// based on the direction of the edge
		networkMap := ng.outgoingMap
		invNetworkMap := ng.incomingMap

		if connectPeerDetails.Details.Incoming {
			networkMap = ng.incomingMap
			invNetworkMap = ng.outgoingMap
		}

		// create new entry if not present
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
			// add an edge between sourceNode and destNode based on the connection direction
			networkMap[sourceNode][destNode] = true
			invNetworkMap[destNode][sourceNode] = true
			// update source node degree
			nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, 1, sourceSession)
			if nodeMsg != nil {
				additions = append(additions, nodeMsg)
				log.WithField("degree", ng.nodeMap[sourceNode].degree).Debug(nodeMsg)
			}
			// update dest node degree
			nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, destNode, 1, destSession)
			if nodeMsg != nil {
				additions = append(additions, nodeMsg)
				log.WithField("degree", ng.nodeMap[destNode].degree).Debug(nodeMsg)
			}

			// send AddEdge message
			var addEdgeMsg AddEdge
			if connectPeerDetails.Details.Incoming {
				addEdgeMsg = MakeAddEdge(destNode, destSession, sourceNode, sourceSession)
				// updateNetworkMap(ng.networkMap, destNode, sourceNode, true)
				logEdgeWithNodeDegree(ng.nodeMap[destNode].degree, ng.nodeMap[sourceNode].degree).Debug(addEdgeMsg)
			} else {
				addEdgeMsg = MakeAddEdge(sourceNode, sourceSession, destNode, destSession)
				// updateNetworkMap(ng.networkMap, sourceNode, destNode, true)
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
		// only one of source and destination has the session information
		destSession := ""

		// networkMap and invNetworkMap are references to outgoing and incoming maps
		// based on the direction of the edge
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
			// delete the edge between sourceNode and destNode
			delete(networkMap[sourceNode], destNode)
			delete(invNetworkMap[destNode], sourceNode)
			// update the sourceNode degree
			nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, -1, sourceSession)
			if nodeMsg != nil {
				deletions = append(deletions, nodeMsg)
				log.WithField("degree", 0).Debug(nodeMsg)
			}

			// update the destNode degree
			nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, destNode, -1, destSession)
			if nodeMsg != nil {
				deletions = append(deletions, nodeMsg)
				log.WithField("degree", 0).Debug(nodeMsg)
			}

			// send RemoveEdge message
			var removeEdgeMsg RemoveEdge
			if disconnectPeerDetails.Details.Incoming {
				removeEdgeMsg = MakeRemoveEdge(destNode, destSession, sourceNode, sourceSession)
				// updateNetworkMap(ng.networkMap, destNode, sourceNode, false)
				logEdgeWithNodeDegree(ng.nodeMap[destNode].degree, ng.nodeMap[sourceNode].degree).Debug(removeEdgeMsg)
			} else {
				removeEdgeMsg = MakeRemoveEdge(sourceNode, sourceSession, destNode, destSession)
				// updateNetworkMap(ng.networkMap, sourceNode, destNode, false)
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

		// latestIncomingMap stores the incoming connections of the sourceNode
		latestIncomingMap := make(map[string]bool)
		for _, peer := range peerConnectionsDetails.Details.IncomingPeers {
			peerNode := formatNodeName(peer.HostName, peer.InstanceName)
			latestIncomingMap[peerNode] = true
		}

		// calculate additions for incoming peers
		for peerHostname := range latestIncomingMap {
			if _, exists := ng.incomingMap[sourceNode][peerHostname]; !exists {
				// create or update the incoming and outgoing mappings
				createOrUpdateDirectionMap(ng.incomingMap, sourceNode, peerHostname, true)
				createOrUpdateDirectionMap(ng.outgoingMap, peerHostname, sourceNode, true)

				// update the sourceNode degree
				nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, 1, sourceSession)
				if nodeMsg != nil {
					additions = append(additions, nodeMsg)
					log.WithField("degree", ng.nodeMap[sourceNode].degree).Debug(nodeMsg)
				}
				// update the peer node degree
				nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, peerHostname, 1, "")
				if nodeMsg != nil {
					additions = append(additions, nodeMsg)
					log.WithField("degree", ng.nodeMap[peerHostname]).Debug(nodeMsg)
				}

				// add edge from peer node to sourceNode
				addEdgeMsg := MakeAddEdge(peerHostname, "", sourceNode, sourceSession)
				additions = append(additions, addEdgeMsg)
				logEdgeWithNodeDegree(ng.nodeMap[peerHostname].degree, ng.nodeMap[sourceNode].degree).Debug(addEdgeMsg)
				// updateNetworkMap(ng.networkMap, peerHostname, sourceNode, true)

			}
		}

		// calculate deletions for incoming peers
		for peerHostname := range ng.outgoingMap {
			if _, exists := ng.outgoingMap[peerHostname][sourceNode]; exists {
				if _, existsInLatestMap := latestIncomingMap[peerHostname]; !existsInLatestMap {
					// delete the mappings from sourceNode to peer node in the maps
					createOrUpdateDirectionMap(ng.incomingMap, sourceNode, peerHostname, false)
					createOrUpdateDirectionMap(ng.outgoingMap, peerHostname, sourceNode, false)

					// update the sourceNode degree
					nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, -1, sourceSession)
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
						log.WithField("degree", ng.nodeMap[sourceNode].degree).Debug(nodeMsg)
					}
					// update peer node degree
					nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, peerHostname, -1, "")
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
						log.WithField("degree", ng.nodeMap[peerHostname].degree).Debug(nodeMsg)
					}
					// remove the edge from peer node to sourceNode
					removeEdgeMsg := MakeRemoveEdge(peerHostname, "", sourceNode, sourceSession)
					deletions = append(deletions, removeEdgeMsg)
					logEdgeWithNodeDegree(ng.nodeMap[peerHostname].degree, ng.nodeMap[sourceNode].degree).Debug(removeEdgeMsg)
					// updateNetworkMap(ng.networkMap, peerHostname, sourceNode, false)
				}
			}
		}

		// latestOutgoingMap stores the outgoing connections of sourceNode
		latestOutgoingMap := make(map[string]bool)
		for _, peer := range peerConnectionsDetails.Details.OutgoingPeers {
			peerNode := formatNodeName(peer.HostName, peer.InstanceName)
			latestOutgoingMap[peerNode] = true
		}

		// calculate additions for outgoing peers
		for peerHostname := range latestOutgoingMap {
			if _, exists := ng.outgoingMap[sourceNode][peerHostname]; !exists {
				// create or update incoming and outgoing maps
				createOrUpdateDirectionMap(ng.outgoingMap, sourceNode, peerHostname, true)
				createOrUpdateDirectionMap(ng.incomingMap, peerHostname, sourceNode, true)

				// update sourceNode degree
				nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, 1, sourceSession)
				if nodeMsg != nil {
					additions = append(additions, nodeMsg)
					log.WithField("degree", ng.nodeMap[sourceNode].degree).Debug(nodeMsg)
				}

				// update peer node degree
				nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, peerHostname, 1, "")
				if nodeMsg != nil {
					additions = append(additions, nodeMsg)
					log.WithField("degree", ng.nodeMap[peerHostname].degree).Debug(nodeMsg)
				}

				// add edge from sourceNode to peer node
				addEdgeMsg := MakeAddEdge(sourceNode, sourceSession, peerHostname, "")
				additions = append(additions, addEdgeMsg)
				logEdgeWithNodeDegree(ng.nodeMap[sourceNode].degree, ng.nodeMap[peerHostname].degree).Debug(addEdgeMsg)
				// updateNetworkMap(ng.networkMap, sourceNode, peerHostname, true)
			}
		}

		// calculate deletions for outgoing peers
		for peerHostname := range ng.incomingMap {
			if _, exists := ng.incomingMap[peerHostname][sourceNode]; exists {
				if _, existsInLatestMap := latestOutgoingMap[peerHostname]; !existsInLatestMap {
					// delete mappings from sourceNode to peer node
					createOrUpdateDirectionMap(ng.outgoingMap, sourceNode, peerHostname, false)
					createOrUpdateDirectionMap(ng.incomingMap, peerHostname, sourceNode, false)

					// update sourceNode degree
					nodeMsg := checkNodeMapForNodeChanges(ng.nodeMap, sourceNode, -1, sourceSession)
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
						log.WithField("degree", ng.nodeMap[sourceNode].degree).Debug(nodeMsg)
					}

					// update peer node degree
					nodeMsg = checkNodeMapForNodeChanges(ng.nodeMap, peerHostname, -1, "")
					if nodeMsg != nil {
						deletions = append(deletions, nodeMsg)
						log.WithField("degree", ng.nodeMap[peerHostname].degree).Debug(nodeMsg)
					}

					// remove the edge from sourceNode to peer node
					removeEdgeMsg := MakeRemoveEdge(sourceNode, sourceSession, peerHostname, "")
					deletions = append(deletions, removeEdgeMsg)
					logEdgeWithNodeDegree(ng.nodeMap[sourceNode].degree, ng.nodeMap[peerHostname].degree).Debug(removeEdgeMsg)
					// updateNetworkMap(ng.networkMap, sourceNode, peerHostname, false)
				}
			}
		}

	}
	return additions, deletions
}

// Returns the formatted name of the node by combining the node name in
// telemetry GUID and telemetry instance
func formatNodeName(telemetryGUID string, telemetryInstance string) string {
	var nodeNameBuilder strings.Builder
	split := strings.Split(telemetryGUID, ":")
	if len(split) > 1 {
		nodeNameBuilder.WriteString(split[1])
		nodeNameBuilder.WriteString(":")
	}

	nodeNameBuilder.WriteString(telemetryInstance)
	return nodeNameBuilder.String()
}

// Checks if an edge from srcNode to destNode exists and delete or set the mapping based on value
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

// Updates the degree of the node and return AddNode/RemoveNode messages if necessary
func checkNodeMapForNodeChanges(nodeMap map[string]*nodeStats, node string, count int, session string) interface{} {
	if ns, exists := nodeMap[node]; !exists || ns.degree == 0 {
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
		// delete(nodeMap, node)
		removeNodeMsg := MakeRemoveNode(node, session)
		return removeNodeMsg
	}
	return nil
}

// func updateNetworkMap(networkMap map[string]map[string]bool, sourceNode string, targetNode string, value bool) {
// 	_, exists := networkMap[sourceNode]
// 	switch value {
// 	case true:
// 		if !exists {
// 			networkMap[sourceNode] = make(map[string]bool)
// 		}
// 		networkMap[sourceNode][targetNode] = value
// 	case false:
// 		if exists {
// 			delete(networkMap[sourceNode], targetNode)
// 		}
// 	}
// }

func (ng *NetworkGraph) makeNetworkGraphMessage() interface{} {
	nwGraphMsg := NetworkGraphMessage{
		MessageType: "NetworkGraph",
	}

	for node := range ng.nodeMap {
		nwGraphMsg.Nodes = append(nwGraphMsg.Nodes, node)
		log.Debug("Global NW node map: ", node)
	}

	for srcNode, destMap := range ng.outgoingMap {
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

	return nwGraphMsg
}

// Network graph refresh service main goroutine
func (ng *NetworkGraph) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	// dequeue telemetry events in a separate go routine
	go ng.dequeueTelemetryEvents(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Info("Terminating network graph refresh service")
			return
		case hit := <-ng.dequeChan:
			// calculate additions and deletions for new telemetry events dequeued
			additions, deletions := ng.calculateMapDelta(hit)
			ng.esClient.metrics.metricsMutex.RLock()
			// check if we need to enter catchup state
			if ng.messageLatency > float64(CurrentConfig.MessageQueueThresholdLatency) {
				ng.catchup = true
				ng.esClient.metrics.metricsMutex.RUnlock()
				continue
			}
			ng.esClient.metrics.metricsMutex.RUnlock()

			// switch to normal state because the catchup is over
			// send the current network graph to all clients
			if ng.catchup {
				ng.catchup = false
				message := ng.makeNetworkGraphMessage()
				ng.handler.Send <- message
				continue
			}

			// stream only the additions and deletions to the clients
			for _, message := range additions {
				ng.handler.Send <- message
			}
			for _, message := range deletions {
				ng.handler.Send <- message
			}

		case client := <-ng.handler.bootstrapChan:
			// send the network graph to the clients who need to be bootstrapped
			if !ng.catchup {
				message := ng.makeNetworkGraphMessage()
				ng.handler.unicastMessage(message, client)
			}
		}
	}
}
