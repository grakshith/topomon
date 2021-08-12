package server

import (
	"strings"
)

type Config struct {
	Host              string `json:"host"`
	Port              string `json:"port"`
	TelemetryEndpoint string `json:"telemetryEndpoint"`
	MetricsEndpoint   string `json:"metricsEndpoint"`
	Channel           string `json:"channel"`
	Network           string `json:"network"`
}

var DefaultLocalConfig = Config{
	Host:              "",
	Port:              "8080",
	TelemetryEndpoint: "http://telemetry.rakshith-s1.algodev.network:9105",
	MetricsEndpoint:   "http://telemetry.rakshith-s1.algodev.network:9090",
	Channel:           "rakshith-s1",
	Network:           "rakshith-s1",
}

var CurrentConfig = DefaultLocalConfig

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
