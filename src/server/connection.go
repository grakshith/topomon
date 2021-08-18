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

// AddNode stores the node to be added
type AddNode struct {
	MessageType string `json:"message"`
	Node        string `json:"node"`
}

// AddEdge stores the source and target nodes between which an edge has to be added
type AddEdge struct {
	MessageType string `json:"message"`
	SourceNode  string `json:"source"`
	TargetNode  string `json:"target"`
}

// RemoveNode stores the node to be removed
type RemoveNode struct {
	MessageType string `json:"message"`
	Node        string `json:"node"`
}

// RemoveEdge stores the source and target nodes between which an edges has to be removed
type RemoveEdge struct {
	MessageType string `json:"message"`
	SourceNode  string `json:"source"`
	TargetNode  string `json:"target"`
}

// functions to construct messages

// MakeAddNode returns a message of type AddNode
func MakeAddNode(node string) AddNode {
	return AddNode{
		MessageType: "AddNode",
		Node:        node,
	}
}

// MakeAddEdge returns a message of type AddEdge
func MakeAddEdge(sourceNode string, targetNode string) AddEdge {
	return AddEdge{
		MessageType: "AddEdge",
		SourceNode:  sourceNode,
		TargetNode:  targetNode,
	}
}

// MakeRemoveNode returns a message of type RemoveEdge
func MakeRemoveNode(node string) RemoveNode {
	return RemoveNode{
		MessageType: "RemoveNode",
		Node:        node,
	}
}

// MakeRemoveEdge returns a message of type RemoveEdge
func MakeRemoveEdge(sourceNode string, targetNode string) RemoveEdge {
	return RemoveEdge{
		MessageType: "RemoveEdge",
		SourceNode:  sourceNode,
		TargetNode:  targetNode,
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

// client write deadline
const writeDeadline = 10 * time.Second

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
	// err := client.conn.WriteJSON(message)
	client.send <- message
	// if err != nil {
	// 	log.Print("Error sending to client: ", err)
	// 	handler.leave <- client
	// }
}

func (handler *ConnectionHandler) broadcastMessage(message interface{}) {
	log.Info("Send: ", message)
	for client := range handler.clients {
		handler.unicastMessage(message, client)
	}
}

func (handler *ConnectionHandler) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			handler.shutDown()
			log.Info("Terminating connection handler")
			return
		case client := <-handler.join:
			log.Info("Client join: ", client.conn.RemoteAddr().String())
			handler.bootstrapChan <- client
			handler.clients[client] = true
		case client := <-handler.leave:
			log.Info("Client leave: ", client.conn.RemoteAddr().String())
			// client.conn.Close()
			delete(handler.clients, client)
		case message := <-handler.Send:
			log.Info("Broadcasting message: ", message)
			handler.broadcastMessage(message)
		}
	}
}

func (handler *ConnectionHandler) shutDown() {
	// for client := range handler.clients {
	// 	client.conn.Close()
	// }
	close(handler.join)
	close(handler.leave)
	close(handler.Send)
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
		// client.conn.Close()
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
		// close(client.send)
		client.conn.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			client.conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		case <-ticker.C:
			// client.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			if err := client.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Info("Error writing to client: ", err)
				return
			}
			log.Debug("Sent ping to client: ", client.conn.RemoteAddr().String())
		case message, ok := <-client.send:
			// client.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			if !ok {
				// client.conn.WriteMessage(websocket.CloseMessage, []byte{})
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
			// w.Write([]byte("\n"))

			// check if there are more messages already queued
			// queuedSize := len(client.send)
			// for i := 0; i < queuedSize; i++ {
			// 	message := <-client.send
			// 	jsonMessage, err := json.Marshal(message)
			// 	if err != nil {
			// 		log.Error("Error while marshalling message: ", err)
			// 	}
			// 	w.Write(jsonMessage)
			// 	w.Write([]byte("\n"))
			// }

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
