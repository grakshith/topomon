package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"

	"github.com/grakshith/topomon/src/server"
	log "github.com/sirupsen/logrus"
)

/*
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
		case "5":
			fmt.Println(time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339Nano))

			var ts string = "2021-08-12T23:00:36.826439Z"

			esClient, _ := server.MakeESClient()
			hits, _ := esClient.QueryTelemetryEvents("", ts, 0, 0)
			respSize := len(hits)
			for respSize == server.DefaultLocalConfig.ESQuerySize {
				searchAfter := hits[len(hits)-1].Sort[0]
				log.Info(searchAfter)
				log.Info(hits[len(hits)-1].Source.Timestamp)
				log.Info(hits[len(hits)-1].Source.Message)
				log.Info(hits[len(hits)-1].Source.ParsedData)
				hits, _ = esClient.QueryTelemetryEvents("", ts, 0, searchAfter)
				respSize = len(hits)
			}
		}
	}
}
*/

func formatFilePath(filePath string) string {
	slash := strings.LastIndex(filePath, "/")
	return filePath[slash+1:]
}

func main() {
	// set log level
	log.SetLevel(log.DebugLevel)
	log.SetReportCaller(true)
	jsonFormatter := &log.TextFormatter{
		TimestampFormat: "2006-01-02T15:04:05.000000Z07:00",
		FullTimestamp:   true,
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			return "", fmt.Sprintf("%s:%d", formatFilePath(f.File), f.Line)
		},
	}
	log.SetFormatter(jsonFormatter)
	f, err := os.OpenFile("server.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Error("Cannot open log file: ", err)
	}
	log.SetOutput(io.MultiWriter(f, os.Stdout))

	ctx, stopServer := context.WithCancel(context.Background())
	servicesWG := &sync.WaitGroup{}

	handler := server.MakeConnectionHandler(ctx)
	esClient, err := server.MakeESClient()
	if err != nil {
		return
	}

	ngRefresher := server.MakeNetworkGraph(handler, esClient)

	router := server.ConfigureURLS(handler)
	httpServer := &http.Server{Addr: server.DefaultLocalConfig.BuildBindAddr(), Handler: router}

	servicesWG.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Error("http.ListenAndServe(): ", err)
		}
		log.Info("Terminating HTTP server")
	}(servicesWG)

	// spin up services
	servicesWG.Add(3)
	go handler.Run(ctx, servicesWG)
	go esClient.Run(ctx, servicesWG)
	go ngRefresher.Run(ctx, servicesWG)
	promClient := server.MakePromClient(handler)

	promClient.StartMetricsService(ctx)

	// register SIGINT signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		log.Info("Received SIGINT, shutting down")
		stopServer()
	}()

	<-ctx.Done()
	httpServer.Shutdown(ctx)
	servicesWG.Wait()
}
