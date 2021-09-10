package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/algorand/websocket"
)

// ws message definitions

type Node struct {
	Name             string `json:"name"`
	TelemetrySession string `json:"session"`
}

// AddNode stores the node to be added
type AddNode struct {
	MessageType string `json:"message"`
	Node        Node   `json:"node"`
}

// AddEdge stores the source and target nodes between which an edge has to be added
type AddEdge struct {
	MessageType string `json:"message"`
	SourceNode  Node   `json:"source"`
	TargetNode  Node   `json:"target"`
}

// RemoveNode stores the node to be removed
type RemoveNode struct {
	MessageType string `json:"message"`
	Node        Node   `json:"node"`
}

// RemoveEdge stores the source and target nodes between which an edges has to be removed
type RemoveEdge struct {
	MessageType string `json:"message"`
	SourceNode  Node   `json:"source"`
	TargetNode  Node   `json:"target"`
}

// functions to construct messages

// MakeAddNode returns a message of type AddNode
func MakeAddNode(node string, session string) AddNode {
	return AddNode{
		MessageType: "AddNode",
		Node: Node{
			Name:             node,
			TelemetrySession: session,
		},
	}
}

// MakeAddEdge returns a message of type AddEdge
func MakeAddEdge(sourceNode string, sourceSession string, targetNode string, targetSession string) AddEdge {
	return AddEdge{
		MessageType: "AddEdge",
		SourceNode: Node{
			Name:             sourceNode,
			TelemetrySession: sourceSession,
		},
		TargetNode: Node{
			Name:             targetNode,
			TelemetrySession: targetSession,
		},
	}
}

// MakeRemoveNode returns a message of type RemoveEdge
func MakeRemoveNode(node string, session string) RemoveNode {
	return RemoveNode{
		MessageType: "RemoveNode",
		Node: Node{
			Name:             node,
			TelemetrySession: session,
		},
	}
}

// MakeRemoveEdge returns a message of type RemoveEdge
func MakeRemoveEdge(sourceNode string, sourceSession string, targetNode string, targetSession string) RemoveEdge {
	return RemoveEdge{
		MessageType: "RemoveEdge",
		SourceNode: Node{
			Name:             sourceNode,
			TelemetrySession: sourceSession,
		},
		TargetNode: Node{
			Name:             targetNode,
			TelemetrySession: targetSession,
		},
	}
}

// ConnectionHandler handles the active websocket connections to the server
type ConnectionHandler struct {
	// Context to handle main exit
	ctx context.Context

	// map of the active clients
	clients map[*Client]bool

	// client join channel
	join chan *Client

	// client leave channel
	leave chan *Client

	// channel for outgoing messages from the ConnectionHandler
	Send chan interface{}

	// channel to bootstrap the client with the network map
	bootstrapChan chan *Client
}

// Client represents the client's websocket connection
type Client struct {
	// the connection handler for this client
	handler *ConnectionHandler
	// websocket conn
	conn *websocket.Conn

	// buffered channel for outgoing messages
	send chan interface{}
}

// Max wait time for a pong message from the client
const pongWait = 20 * time.Second

// ping interval
const pingPeriod = pongWait / 2

func MakeConnectionHandler(ctx context.Context) *ConnectionHandler {
	handler := &ConnectionHandler{
		ctx:           ctx,
		clients:       make(map[*Client]bool),
		join:          make(chan *Client),
		leave:         make(chan *Client),
		Send:          make(chan interface{}),
		bootstrapChan: make(chan *Client),
	}
	return handler
}

func (handler *ConnectionHandler) unicastMessage(message interface{}, client *Client) {
	client.send <- message
}

func (handler *ConnectionHandler) broadcastMessage(message interface{}) {
	for client := range handler.clients {
		handler.unicastMessage(message, client)
	}
}

func (handler *ConnectionHandler) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Info("Terminating connection handler")
			return
		case client := <-handler.join:
			log.Info("Client join: ", client.conn.RemoteAddr().String())
			// bootstrap the client with the network graph
			handler.bootstrapChan <- client
			handler.clients[client] = true
		case client := <-handler.leave:
			log.Info("Client leave: ", client.conn.RemoteAddr().String())
			delete(handler.clients, client)
		case message := <-handler.Send:
			handler.broadcastMessage(message)
		}
	}
}

func (client *Client) readComms(ctx context.Context) {
	client.conn.SetReadLimit(256)
	client.conn.SetReadDeadline(time.Now().Add(pongWait))
	client.conn.SetPongHandler(func(string) error { client.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })

	defer func() {
		log.Info("Leaving readComms: ", client.conn.RemoteAddr().String())
		if err := ctx.Err(); err == nil {
			client.handler.leave <- client
		}
		close(client.send)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_, _, err := client.conn.ReadMessage()
		if err != nil {
			log.Error("Error when reading pong messages: ", err)
			return
		}
	}

}

func (client *Client) writeComms(ctx context.Context) {
	// ticker to send ping messages
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		log.Info("Leaving writeComms: ", client.conn.RemoteAddr().String())
		ticker.Stop()
		client.conn.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			client.conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		case <-ticker.C:
			if err := client.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Info("Error writing to client: ", err)
				return
			}
			log.Debug("Sent ping to client: ", client.conn.RemoteAddr().String())
		case message, ok := <-client.send:
			if !ok {
				return
			}

			jsonMessage, err := json.Marshal(message)
			if err != nil {
				log.Error("Error while marshalling message: ", err)
			}
			w, err := client.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}

			w.Write(jsonMessage)

			if err := w.Close(); err != nil {
				return
			}
			log.Debug("Sending message to client: ", client.conn.RemoteAddr().String())

		}

	}

}

var upgrader = websocket.Upgrader{}

func (handler *ConnectionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("Upgrader error: ", err)
		return
	}
	client := &Client{
		handler: handler,
		conn:    c,
		send:    make(chan interface{}, 256),
	}
	handler.join <- client
	go client.writeComms(handler.ctx)
	go client.readComms(handler.ctx)
}
