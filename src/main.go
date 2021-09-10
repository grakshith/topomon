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

func formatFilePath(filePath string) string {
	slash := strings.LastIndex(filePath, "/")
	return filePath[slash+1:]
}

func main() {
	// set log level
	log.SetLevel(log.InfoLevel)
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

	// load configuration from file
	server.LoadConfig()

	// main context for the services
	ctx, stopServer := context.WithCancel(context.Background())
	servicesWG := &sync.WaitGroup{}

	handler := server.MakeConnectionHandler(ctx)
	esClient, err := server.MakeESClient()
	if err != nil {
		return
	}

	ngRefresher := server.MakeNetworkGraph(handler, esClient)

	promClient := server.MakePromClient(handler)

	// URL router
	router := server.ConfigureURLS(handler, promClient)
	httpServer := &http.Server{Addr: server.CurrentConfig.BuildBindAddr(), Handler: router}

	servicesWG.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Error("http.ListenAndServe(): ", err)
		}
		log.Info("Terminating HTTP server")
	}(servicesWG)

	// spin up services
	servicesWG.Add(4)
	go handler.Run(ctx, servicesWG)
	go esClient.Run(ctx, servicesWG)
	go ngRefresher.Run(ctx, servicesWG)
	go promClient.Run(ctx, servicesWG)

	log.Info("Started services")

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
