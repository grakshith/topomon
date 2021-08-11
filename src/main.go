package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/grakshith/topomon/src/server"
	log "github.com/sirupsen/logrus"
)

func repl(handler *server.ConnectionHandler) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	for {
		select {
		case <-signalChan:
			log.Info("Received SIGINT, shutting down")
			return
		default:
		}
		var input string
		fmt.Print("\nInput: ")
		fmt.Scanln(&input)
		switch input {
		case "1":
			var node string
			fmt.Print("\nNode: ")
			fmt.Scanln(&node)
			addNodeMsg := server.MakeAddNode(node)
			handler.Send <- addNodeMsg
		case "2":
			var srcNode, targetNode string
			fmt.Print("\nSource: ")
			fmt.Scanln(&srcNode)
			fmt.Print("\nTarget: ")
			fmt.Scanln(&targetNode)
			addEdgeMsg := server.MakeAddEdge(srcNode, targetNode)
			handler.Send <- addEdgeMsg
		case "3":
			var node string
			fmt.Print("\nNode: ")
			fmt.Scanln(&node)
			removeNodeMsg := server.MakeRemoveNode(node)
			handler.Send <- removeNodeMsg
		case "4":
			var srcNode, targetNode string
			fmt.Print("\nSource: ")
			fmt.Scanln(&srcNode)
			fmt.Print("\nTarget: ")
			fmt.Scanln(&targetNode)
			removeEdgeMsg := server.MakeRemoveEdge(srcNode, targetNode)
			handler.Send <- removeEdgeMsg
		}
	}
}

func main() {
	ctx, stopServer := context.WithCancel(context.Background())
	httpServerDone := &sync.WaitGroup{}

	handler := server.MakeConnectionHandler(ctx)

	router := server.ConfigureURLS(handler)
	httpServer := &http.Server{Addr: server.DefaultLocalConfig.BuildBindAddr(), Handler: router}

	httpServerDone.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if err := httpServer.ListenAndServe(); err != nil {
			return
		}
	}(httpServerDone)

	go handler.Run(ctx)
	repl(handler)
	httpServer.Shutdown(ctx)
	httpServerDone.Wait()
	stopServer()
}
