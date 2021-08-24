package server

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

type PromClient struct {
	client api.Client
}

func MakePromClient() (*PromClient, error) {
	client, err := api.NewClient(api.Config{
		Address: CurrentConfig.MetricsEndpoint,
	})
	if err != nil {
		log.Error("MakePromClient(): ", err)
	}
	return &PromClient{
		client: client,
	}, err
}

func (pClient *PromClient) QueryMetrics(queryString string) model.Value {
	v1api := v1.NewAPI(pClient.client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, warnings, err := v1api.Query(ctx, queryString, time.Now())
	if err != nil {
		log.Error("queryPrometheus():", err)
	}
	for _, warning := range warnings {
		log.Warn("queryPrometheus(): ", warning)
	}
	return result
}
