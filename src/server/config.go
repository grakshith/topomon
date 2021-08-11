package server

import (
	"strings"
)

type Config struct {
	Host string `json:"host"`
	Port string `json:"port"`
}

var DefaultLocalConfig = Config{
	Host: "",
	Port: "8080",
}

func (config Config) BuildBindAddr() string {
	var bindAddr strings.Builder
	bindAddr.WriteString(config.Host)
	bindAddr.WriteString(":")
	bindAddr.WriteString(config.Port)
	return bindAddr.String()
}
