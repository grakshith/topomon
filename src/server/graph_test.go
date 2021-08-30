package server

import (
	"fmt"
	"testing"

	"github.com/algorand/go-algorand/logging/telemetryspec"
	"github.com/stretchr/testify/require"
)

func TestCalculateMapDelta(t *testing.T) {

	queryHit1 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "B",
					Incoming: false,
				},
			},
		},
	}

	queryResult1 := []interface{}{MakeAddNode("A:", ""), MakeAddNode("B:", ""), MakeAddEdge("A:", "", "B:", "")}

	queryHit2 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "A",
					Incoming: true,
				},
			},
		},
	}
	ng := NetworkGraph{
		outgoingMap: make(map[string]map[string]bool),
		incomingMap: make(map[string]map[string]bool),
		networkMap:  make(map[string]map[string]bool),
		nodeMap:     make(map[string]*nodeStats),
	}

	additions, deletions := ng.calculateMapDelta(queryHit1)
	require.ElementsMatch(t, additions, queryResult1)
	require.Empty(t, deletions)

	fmt.Println("Query1")
	fmt.Println(ng.outgoingMap)
	fmt.Println(ng.incomingMap)

	additions, deletions = ng.calculateMapDelta(queryHit1)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	fmt.Println("Query1")
	fmt.Println(ng.outgoingMap)
	fmt.Println(ng.incomingMap)

	additions, deletions = ng.calculateMapDelta(queryHit2)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	fmt.Println("Query2")
	fmt.Println(ng.outgoingMap)
	fmt.Println(ng.incomingMap)

	queryHit5 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: PeerConnectionsMessage,
			ParsedData: PeerConnectionDetails{
				Details: telemetryspec.PeersConnectionDetails{
					IncomingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName: "E",
						},
					},
					OutgoingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName: "B",
						},
						{
							HostName: "C",
						},
						{
							HostName: "D",
						},
					},
				},
			},
		},
	}

	query5Additions := []interface{}{MakeAddNode("E:", ""), MakeAddNode("C:", ""), MakeAddNode("D:", ""), MakeAddEdge("E:", "", "A:", ""), MakeAddEdge("A:", "", "C:", ""), MakeAddEdge("A:", "", "D:", "")}
	// query5Deletions := []interface{}{RemoveNode{MessageType: "RemoveNode", Node: "B"}, RemoveEdge{MessageType: "RemoveEdge", SourceNode: "A", TargetNode: "B"}}

	additions, deletions = ng.calculateMapDelta(queryHit5)
	require.ElementsMatch(t, query5Additions, additions)
	require.Empty(t, deletions)

	fmt.Println("Query5")
	fmt.Println(ng.outgoingMap)
	fmt.Println(ng.incomingMap)

	fmt.Println("Query5.5")
	queryHit5_5 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "E",
					Incoming: true,
				},
			},
		},
	}
	additions, deletions = ng.calculateMapDelta(queryHit5_5)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	queryHit8 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "C",
					Incoming: false,
				},
			},
		},
	}

	queryHit9 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "F",
					Incoming: false,
				},
			},
		},
	}

	queryHit10 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "G",
					Incoming: false,
				},
			},
		},
	}

	queryHit11 := Hit{
		Source: HitSource{
			Host:    "F",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "B",
					Incoming: true,
				},
			},
		},
	}

	queryHit12 := Hit{
		Source: HitSource{
			Host:    "C",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "B",
					Incoming: true,
				},
			},
		},
	}

	queryHit13 := Hit{
		Source: HitSource{
			Host:    "G",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "B",
					Incoming: true,
				},
			},
		},
	}

	additions, deletions = ng.calculateMapDelta(queryHit8)
	fmt.Println("Query 8")

	query8Additions := []interface{}{MakeAddEdge("B:", "", "C:", "")}
	require.ElementsMatch(t, query8Additions, additions)
	require.Empty(t, deletions)

	additions, deletions = ng.calculateMapDelta(queryHit9)
	fmt.Println("Query 9")

	query9Addtions := []interface{}{MakeAddNode("F:", ""), MakeAddEdge("B:", "", "F:", "")}
	require.ElementsMatch(t, query9Addtions, additions)
	require.Empty(t, deletions)

	additions, deletions = ng.calculateMapDelta(queryHit10)
	fmt.Println("Query 10")
	query10Additions := []interface{}{MakeAddNode("G:", ""), MakeAddEdge("B:", "", "G:", "")}
	require.ElementsMatch(t, query10Additions, additions)
	require.Empty(t, deletions)

	fmt.Println("Query 11")
	additions, deletions = ng.calculateMapDelta(queryHit11)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	fmt.Println("Query 12")
	additions, deletions = ng.calculateMapDelta(queryHit12)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	fmt.Println("Query 13")
	additions, deletions = ng.calculateMapDelta(queryHit13)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	queryHit6 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: PeerConnectionsMessage,
			ParsedData: PeerConnectionDetails{
				Details: telemetryspec.PeersConnectionDetails{
					IncomingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName: "A",
						},
					},
					OutgoingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName: "C",
						},
						{
							HostName: "F",
						},
						{
							HostName: "G",
						},
					},
				},
			},
		},
	}

	additions, deletions = ng.calculateMapDelta(queryHit6)

	fmt.Println("Query6")
	fmt.Println(ng.outgoingMap)
	fmt.Println(ng.incomingMap)

	require.Empty(t, additions)
	require.Empty(t, deletions)

	queryHit3 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: DisconnectPeerMessage,
			ParsedData: DisconnectPeerDetails{
				Details: telemetryspec.DisconnectPeerEventDetails{
					PeerEventDetails: telemetryspec.PeerEventDetails{
						HostName: "A",
						Incoming: true,
					},
				},
			},
		},
	}

	additions, deletions = ng.calculateMapDelta(queryHit3)
	query3deletions := []interface{}{MakeRemoveEdge("A:", "", "B:", "")}
	require.ElementsMatch(t, query3deletions, deletions)
	require.Empty(t, additions)

	fmt.Println("Query3")
	fmt.Println(ng.outgoingMap)
	fmt.Println(ng.incomingMap)

	queryHit4 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: DisconnectPeerMessage,
			ParsedData: DisconnectPeerDetails{
				Details: telemetryspec.DisconnectPeerEventDetails{
					PeerEventDetails: telemetryspec.PeerEventDetails{
						HostName: "B",
						Incoming: false,
					},
				},
			},
		},
	}

	additions, deletions = ng.calculateMapDelta(queryHit4)

	fmt.Println("Query4")
	fmt.Println(ng.outgoingMap)
	fmt.Println(ng.incomingMap)

	require.Empty(t, additions)
	require.Empty(t, deletions)

	queryHit13_5 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "C",
					Incoming: false,
				},
			},
		},
	}

	fmt.Println("Query 13.5")
	additions, deletions = ng.calculateMapDelta(queryHit13_5)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	queryHit14 := Hit{
		Source: HitSource{
			Host:    "C",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				Details: telemetryspec.PeerEventDetails{
					HostName: "A",
					Incoming: true,
				},
			},
		},
	}

	fmt.Println("Query 14")
	additions, deletions = ng.calculateMapDelta(queryHit14)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	queryHit15 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: DisconnectPeerMessage,
			ParsedData: DisconnectPeerDetails{
				Details: telemetryspec.DisconnectPeerEventDetails{
					PeerEventDetails: telemetryspec.PeerEventDetails{
						HostName: "C",
						Incoming: false,
					},
				},
			},
		},
	}

	fmt.Println("Query 15")
	additions, deletions = ng.calculateMapDelta(queryHit15)
	query15deletions := []interface{}{MakeRemoveEdge("A:", "", "C:", "")}
	require.ElementsMatch(t, query15deletions, deletions)
	require.Empty(t, additions)

	queryHit7 := Hit{
		Source: HitSource{
			Host:    "C",
			Message: DisconnectPeerMessage,
			ParsedData: DisconnectPeerDetails{
				Details: telemetryspec.DisconnectPeerEventDetails{
					PeerEventDetails: telemetryspec.PeerEventDetails{
						HostName: "A",
						Incoming: true,
					},
				},
			},
		},
	}

	fmt.Println("Query 7")
	additions, deletions = ng.calculateMapDelta(queryHit7)
	require.Empty(t, deletions)
	require.Empty(t, additions)

	queryHit16 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: DisconnectPeerMessage,
			ParsedData: DisconnectPeerDetails{
				Details: telemetryspec.DisconnectPeerEventDetails{
					PeerEventDetails: telemetryspec.PeerEventDetails{
						HostName: "C",
						Incoming: false,
					},
				},
			},
		},
	}

	fmt.Println("Query16")
	additions, deletions = ng.calculateMapDelta(queryHit16)
	query16Deletions := []interface{}{MakeRemoveEdge("B:", "", "C:", ""), MakeRemoveNode("C:", "")}
	require.Empty(t, additions)
	require.ElementsMatch(t, query16Deletions, deletions)

	fmt.Println(ng.outgoingMap)
	fmt.Println(ng.incomingMap)
	fmt.Println(ng.nodeMap)

	queryHit17 := Hit{
		Source: HitSource{
			Host:    "C",
			Message: PeerConnectionsMessage,
			ParsedData: PeerConnectionDetails{
				Details: telemetryspec.PeersConnectionDetails{
					IncomingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName: "B",
						},
					},
					OutgoingPeers: []telemetryspec.PeerConnectionDetails{},
				},
			},
		},
	}

	fmt.Println("Query 17")
	additions, deletions = ng.calculateMapDelta(queryHit17)
	query17Additions := []interface{}{MakeAddEdge("B:", "", "C:", ""), MakeAddNode("C:", "")}
	require.ElementsMatch(t, additions, query17Additions)
	require.Empty(t, deletions)

	fmt.Println(ng.outgoingMap)
	fmt.Println(ng.incomingMap)
	fmt.Println(ng.networkMap)

}
