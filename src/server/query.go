package server

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/elastic/go-elasticsearch/v6"
	log "github.com/sirupsen/logrus"
)

const query = `
	"query":{
		"bool":{
			"must":
				{
					"terms":{
						"Message.keyword":["/Network/PeerConnections", "/Network/ConnectPeer", "/Network/DisconnectPeer"]
					}
				}
			,
			"filter":{
				"range":{
					"@timestamp":{
						"gte": %s
					}
				}
			}
		}
	},
	"size": 100,
	"sort":{
		"@timestamp":"asc"
	}`

type ESResponse struct {
	Took int
	Hits struct {
		Total int
		Hits  []struct {
			ID     string `json:"_id"`
			Source struct {
				Host      string          `json:"Host"`
				Timestamp string          `json:"@timestamp"`
				Message   string          `json:"Message"`
				Data      json.RawMessage `json:"Data"`
			} `json:"_source"`
		}
	}
}

type ESClient struct {
	es    *elasticsearch.Client
	index string
}

func MakeESClient() (*ESClient, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			CurrentConfig.TelemetryEndpoint,
		},
		Username:          "elastic",
		Password:          "elastic",
		EnableDebugLogger: true,
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Error("Error while creating Elasticsearch client: ", err)
		return nil, err
	}
	log.Info("Initialized Elasticsearch client")
	log.Info(es.Info())

	esClient := &ESClient{
		es:    es,
		index: CurrentConfig.BuildESIndexName(),
	}

	return esClient, nil
}

func (esClient *ESClient) QueryTelemetryEvents(queryString, timestamp string, after ...string) {
	builtQuery := buildQueryString(queryString, timestamp, after...)
	es := esClient.es
	res, err := es.Search(es.Search.WithIndex(esClient.index), es.Search.WithBody(builtQuery))
	if err != nil {
		log.Error("Error while querying elasticsearch: ", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Error("Error: ", err)
			return
		}
		log.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
		return
	}
	log.Info("Result: ", res.String())

	var esResponse ESResponse
	if err := json.NewDecoder(res.Body).Decode(&esResponse); err != nil {
		log.Error("Error: ", err)
		return
	}

	var id string
	for _, hit := range esResponse.Hits.Hits {
		id = hit.ID
	}

	log.Info(id)

}

func buildQueryString(queryString, timestamp string, after ...string) io.Reader {
	var builder strings.Builder
	builder.WriteString("{\n")
	if queryString == "" {
		builder.WriteString(query)
	} else {
		builder.WriteString(fmt.Sprintf(queryString, timestamp))
	}

	if len(after) > 0 && after[0] != "" && after[0] != "null" {
		builder.WriteString(",\n")
		builder.WriteString(fmt.Sprintf(`	"search_after": [%s]`, after[0]))
	}
	builder.WriteString("\n}")

	// log.Info("Query: ", builder.String())

	return strings.NewReader(builder.String())
}
