package server

import (
	"fmt"
	"testing"

	"github.com/algorand/go-algorand/logging/telemetryspec"
	"github.com/stretchr/testify/require"
)

func TestCalculateMapDelta(t *testing.T) {

	ng := NetworkGraph{
		outgoingMap: make(map[string]map[string]bool),
		incomingMap: make(map[string]map[string]bool),
		nodeMap:     make(map[string]*nodeStats),
	}

	query1 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "A",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "B",
					Incoming:     false,
					InstanceName: "B",
				},
			},
		},
	}

	queryResult1 := []interface{}{MakeAddNode("A", ""), MakeAddNode("B", ""), MakeAddEdge("A", "", "B", "")}

	fmt.Println("Query 1")
	additions, deletions := ng.calculateMapDelta(query1)
	require.ElementsMatch(t, additions, queryResult1)
	require.Empty(t, deletions)

	// same query again should produce zero delta
	additions, deletions = ng.calculateMapDelta(query1)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	// other node sending the telemetry event in the other direction
	query2 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "B",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "A",
					InstanceName: "A",
					Incoming:     true,
				},
			},
		},
	}

	fmt.Println("Query 2")
	additions, deletions = ng.calculateMapDelta(query2)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	// peer connections additions
	query3 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: PeerConnectionsMessage,
			ParsedData: PeerConnectionDetails{
				InstanceName: "A",
				Details: telemetryspec.PeersConnectionDetails{
					IncomingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName:     "E",
							InstanceName: "E",
						},
					},
					OutgoingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName:     "B",
							InstanceName: "B",
						},
						{
							HostName:     "C",
							InstanceName: "C",
						},
						{
							HostName:     "D",
							InstanceName: "D",
						},
					},
				},
			},
		},
	}

	query3Additions := []interface{}{MakeAddNode("E", ""), MakeAddNode("C", ""), MakeAddNode("D", ""), MakeAddEdge("E", "", "A", ""), MakeAddEdge("A", "", "C", ""), MakeAddEdge("A", "", "D", "")}

	fmt.Println("Query 3")
	additions, deletions = ng.calculateMapDelta(query3)
	require.ElementsMatch(t, query3Additions, additions)
	require.Empty(t, deletions)

	// already added from previous peerconnections message
	query4 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "A",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "E",
					InstanceName: "E",
					Incoming:     true,
				},
			},
		},
	}

	fmt.Println("Query 4")
	additions, deletions = ng.calculateMapDelta(query4)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	query5 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "B",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "C",
					InstanceName: "C",
					Incoming:     false,
				},
			},
		},
	}

	fmt.Println("Query 5")
	additions, deletions = ng.calculateMapDelta(query5)
	query5Additions := []interface{}{MakeAddEdge("B", "", "C", "")}
	require.ElementsMatch(t, query5Additions, additions)
	require.Empty(t, deletions)

	query6 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "B",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "F",
					InstanceName: "F",
					Incoming:     false,
				},
			},
		},
	}

	fmt.Println("Query 6")
	additions, deletions = ng.calculateMapDelta(query6)
	query6Addtions := []interface{}{MakeAddNode("F", ""), MakeAddEdge("B", "", "F", "")}
	require.ElementsMatch(t, query6Addtions, additions)
	require.Empty(t, deletions)

	query7 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "B",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "G",
					InstanceName: "G",
					Incoming:     false,
				},
			},
		},
	}

	fmt.Println("Query 7")
	additions, deletions = ng.calculateMapDelta(query7)
	query7Additions := []interface{}{MakeAddNode("G", ""), MakeAddEdge("B", "", "G", "")}
	require.ElementsMatch(t, query7Additions, additions)
	require.Empty(t, deletions)

	query8 := Hit{
		Source: HitSource{
			Host:    "F",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "F",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "B",
					InstanceName: "B",
					Incoming:     true,
				},
			},
		},
	}

	fmt.Println("Query 8")
	additions, deletions = ng.calculateMapDelta(query8)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	query9 := Hit{
		Source: HitSource{
			Host:    "C",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "C",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "B",
					InstanceName: "B",
					Incoming:     true,
				},
			},
		},
	}

	fmt.Println("Query 9")
	additions, deletions = ng.calculateMapDelta(query9)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	query10 := Hit{
		Source: HitSource{
			Host:    "G",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "G",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "B",
					InstanceName: "B",
					Incoming:     true,
				},
			},
		},
	}

	fmt.Println("Query 10")
	additions, deletions = ng.calculateMapDelta(query10)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	query11 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: PeerConnectionsMessage,
			ParsedData: PeerConnectionDetails{
				InstanceName: "B",
				Details: telemetryspec.PeersConnectionDetails{
					IncomingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName:     "A",
							InstanceName: "A",
						},
					},
					OutgoingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName:     "C",
							InstanceName: "C",
						},
						{
							HostName:     "F",
							InstanceName: "F",
						},
						{
							HostName:     "G",
							InstanceName: "G",
						},
					},
				},
			},
		},
	}

	fmt.Println("Query 11")
	additions, deletions = ng.calculateMapDelta(query11)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	query12 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: DisconnectPeerMessage,
			ParsedData: DisconnectPeerDetails{
				InstanceName: "B",
				Details: telemetryspec.DisconnectPeerEventDetails{
					PeerEventDetails: telemetryspec.PeerEventDetails{
						HostName:     "A",
						InstanceName: "A",
						Incoming:     true,
					},
				},
			},
		},
	}

	fmt.Println("Query 12")
	additions, deletions = ng.calculateMapDelta(query12)
	query12deletions := []interface{}{MakeRemoveEdge("A", "", "B", "")}
	require.ElementsMatch(t, query12deletions, deletions)
	require.Empty(t, additions)

	query13 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: DisconnectPeerMessage,
			ParsedData: DisconnectPeerDetails{
				InstanceName: "A",
				Details: telemetryspec.DisconnectPeerEventDetails{
					PeerEventDetails: telemetryspec.PeerEventDetails{
						HostName:     "B",
						InstanceName: "B",
						Incoming:     false,
					},
				},
			},
		},
	}

	fmt.Println("Query 13")
	additions, deletions = ng.calculateMapDelta(query13)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	query14 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "A",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "C",
					InstanceName: "C",
					Incoming:     false,
				},
			},
		},
	}

	fmt.Println("Query 14")
	additions, deletions = ng.calculateMapDelta(query14)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	query15 := Hit{
		Source: HitSource{
			Host:    "C",
			Message: ConnectPeerMessage,
			ParsedData: ConnectPeerDetails{
				InstanceName: "C",
				Details: telemetryspec.PeerEventDetails{
					HostName:     "A",
					InstanceName: "A",
					Incoming:     true,
				},
			},
		},
	}

	fmt.Println("Query 15")
	additions, deletions = ng.calculateMapDelta(query15)
	require.Empty(t, additions)
	require.Empty(t, deletions)

	query16 := Hit{
		Source: HitSource{
			Host:    "A",
			Message: DisconnectPeerMessage,
			ParsedData: DisconnectPeerDetails{
				InstanceName: "A",
				Details: telemetryspec.DisconnectPeerEventDetails{
					PeerEventDetails: telemetryspec.PeerEventDetails{
						HostName:     "C",
						InstanceName: "C",
						Incoming:     false,
					},
				},
			},
		},
	}

	fmt.Println("Query 16")
	additions, deletions = ng.calculateMapDelta(query16)
	query16deletions := []interface{}{MakeRemoveEdge("A", "", "C", "")}
	require.ElementsMatch(t, query16deletions, deletions)
	require.Empty(t, additions)

	query17 := Hit{
		Source: HitSource{
			Host:    "C",
			Message: DisconnectPeerMessage,
			ParsedData: DisconnectPeerDetails{
				InstanceName: "C",
				Details: telemetryspec.DisconnectPeerEventDetails{
					PeerEventDetails: telemetryspec.PeerEventDetails{
						HostName:     "A",
						InstanceName: "A",
						Incoming:     true,
					},
				},
			},
		},
	}

	fmt.Println("Query 17")
	additions, deletions = ng.calculateMapDelta(query17)
	require.Empty(t, deletions)
	require.Empty(t, additions)

	query18 := Hit{
		Source: HitSource{
			Host:    "B",
			Message: DisconnectPeerMessage,
			ParsedData: DisconnectPeerDetails{
				InstanceName: "B",
				Details: telemetryspec.DisconnectPeerEventDetails{
					PeerEventDetails: telemetryspec.PeerEventDetails{
						HostName:     "C",
						InstanceName: "C",
						Incoming:     false,
					},
				},
			},
		},
	}

	fmt.Println("Query 18")
	additions, deletions = ng.calculateMapDelta(query18)
	query18Deletions := []interface{}{MakeRemoveEdge("B", "", "C", ""), MakeRemoveNode("C", "")}
	require.Empty(t, additions)
	require.ElementsMatch(t, query18Deletions, deletions)

	query19 := Hit{
		Source: HitSource{
			Host:    "C",
			Message: PeerConnectionsMessage,
			ParsedData: PeerConnectionDetails{
				InstanceName: "C",
				Details: telemetryspec.PeersConnectionDetails{
					IncomingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName:     "B",
							InstanceName: "B",
						},
					},
					OutgoingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName:     "A",
							InstanceName: "A",
						},
					},
				},
			},
		},
	}

	fmt.Println("Query 19")
	additions, deletions = ng.calculateMapDelta(query19)
	query19Additions := []interface{}{MakeAddEdge("B", "", "C", ""), MakeAddNode("C", ""), MakeAddEdge("C", "", "A", "")}

	require.ElementsMatch(t, additions, query19Additions)
	require.Empty(t, deletions)

	query20 := Hit{
		Source: HitSource{
			Host:    "C",
			Message: PeerConnectionsMessage,
			ParsedData: PeerConnectionDetails{
				InstanceName: "C",
				Details: telemetryspec.PeersConnectionDetails{
					IncomingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName:     "D",
							InstanceName: "D",
						},
					},
					OutgoingPeers: []telemetryspec.PeerConnectionDetails{
						{
							HostName:     "F",
							InstanceName: "F",
						},
					},
				},
			},
		},
	}

	fmt.Println("Query 20")
	additions, deletions = ng.calculateMapDelta(query20)
	query20Additions := []interface{}{MakeAddEdge("D", "", "C", ""), MakeAddEdge("C", "", "F", "")}
	query20Deletions := []interface{}{MakeRemoveEdge("B", "", "C", ""), MakeRemoveEdge("C", "", "A", "")}

	require.ElementsMatch(t, additions, query20Additions)
	require.ElementsMatch(t, deletions, query20Deletions)

}
