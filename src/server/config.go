package server

import (
	"encoding/json"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Config struct {
	Host                         string  `json:"host"`
	Port                         string  `json:"port"`
	TelemetryEndpoint            string  `json:"telemetryEndpoint"`
	MetricsEndpoint              string  `json:"metricsEndpoint"`
	Channel                      string  `json:"channel"`
	Network                      string  `json:"network"`
	ESQuerySize                  int     `json:"esQuerySize"`
	ESQueryRefreshPeriod         int     `json:"eqQueryRefreshPeriod"`         // in seconds
	MessageQueueThresholdLatency int     `json:"messageQueueThresholdLatency"` // in seconds
	MetricsRefreshPeriod         float32 `json:"metricsRefreshPeriod"`         // in milliseconds
}

var DefaultLocalConfig = Config{
	Host:                         "",
	Port:                         "8080",
	TelemetryEndpoint:            "http://localhost:9105",
	MetricsEndpoint:              "http://localhost:9090",
	ESQuerySize:                  1000,
	ESQueryRefreshPeriod:         2,
	MessageQueueThresholdLatency: 1,
	MetricsRefreshPeriod:         1000,
}

var CurrentConfig = DefaultLocalConfig

func LoadConfig() {
	fileBytes, err := os.ReadFile("config.json")
	if err != nil {
		log.Error("Error reading config file: ", err)
		os.Exit(1)
	}

	json.Unmarshal(fileBytes, &CurrentConfig)
	invalidConfig := false
	if CurrentConfig.TelemetryEndpoint == "" {
		log.Error("Config: TelemetryEndpoint is missing")
		invalidConfig = true
	}

	if CurrentConfig.MetricsEndpoint == "" {
		log.Error("Config: MetricsEndpoint is missing")
		invalidConfig = true
	}

	if CurrentConfig.Channel == "" {
		log.Error("Config: Channel name is missing")
		invalidConfig = true
	}
	if CurrentConfig.Network == "" {
		log.Error("Config: Network name is missing")
		invalidConfig = true
	}

	if invalidConfig {
		log.Error("Errors found in config file. Exiting.")
		os.Exit(1)
	}

}

func (config Config) BuildBindAddr() string {
	var bindAddr strings.Builder
	bindAddr.WriteString(config.Host)
	bindAddr.WriteString(":")
	bindAddr.WriteString(config.Port)
	return bindAddr.String()
}

func (config Config) BuildESIndexName() string {
	var indexName strings.Builder
	indexName.WriteString(config.Channel)
	indexName.WriteString("-")
	indexName.WriteString(config.Network)
	indexName.WriteString("-")
	indexName.WriteString("v1")
	return indexName.String()
}
