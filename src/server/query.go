package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	telemetryspec "github.com/algorand/go-algorand/logging/telemetryspec"
	"github.com/algorand/go-deadlock"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/enriquebris/goconcurrentqueue"
	log "github.com/sirupsen/logrus"
)

// this is the query to fetch the three message types we are interested in
// this looks for relevant messages having a timestamp greater than some value
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
						"gt": "%s"
					}
				}
			}
		}
	},
	"sort":{
		"@timestamp":"asc"
	}`

const PeerConnectionsMessage = "/Network/PeerConnections"
const ConnectPeerMessage = "/Network/ConnectPeer"
const DisconnectPeerMessage = "/Network/DisconnectPeer"

// type to parse the response of elasticsearch
type ESResponse struct {
	Took int
	Hits struct {
		Total      int
		HitDetails []Hit `json:"hits"`
	}
}

// Hit stores each elasticsearch query hit
type Hit struct {
	ID     string `json:"_id"`
	Source struct {
		Host      string
		Timestamp string `json:"@timestamp"`
		Message   string
		RawData   json.RawMessage `json:"Data"`
		// parsed data stores the concrete type corresponding to the message type
		ParsedData interface{}
	} `json:"_source"`
	Sort []int
}

// Types for each of the message types
// These are wrappers to telemetryspec types
type ConnectPeerDetails struct {
	Details telemetryspec.PeerEventDetails `json:"details"`
}

type PeerConnectionDetails struct {
	Details telemetryspec.PeersConnectionDetails `json:"details"`
}

type DisconnectPeerDetails struct {
	Details telemetryspec.DisconnectPeerEventDetails `json:"details"`
}

// ESClient is the wrapper around the elasticsearch client
// with some useful fields used in the queries
type ESClient struct {
	es                    *elasticsearch.Client
	index                 string
	querySize             int
	telemetryMessageQueue *goconcurrentqueue.FIFO
	metrics               queueMetrics
}

type queueMetrics struct {
	metricsMutex            *deadlock.RWMutex
	latestEnqueuedTimestamp string
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
	log.Debug(es.Info())

	esClient := &ESClient{
		es:                    es,
		index:                 CurrentConfig.BuildESIndexName(),
		querySize:             CurrentConfig.ESQuerySize,
		telemetryMessageQueue: goconcurrentqueue.NewFIFO(),
		metrics: queueMetrics{
			metricsMutex:            &deadlock.RWMutex{},
			latestEnqueuedTimestamp: time.Time{}.UTC().Format(time.RFC3339Nano),
		},
	}

	return esClient, nil
}

// QueryTelemetryEvents queries elasticsearch for the telemetry messages defined
// in queryString whose timestamps are greater than timestamp returning a page
// of the matching hits of length defined by the size parameter. In case, there is
// more than one page, the after parameter can be used to fetch the additional pages.
// The after parameter is a sort value returned by elasticsearch with each hit.
func (esClient *ESClient) QueryTelemetryEvents(queryString, timestamp string, size int, after int) ([]Hit, error) {
	builtQuery := esClient.buildQueryString(queryString, timestamp, size, after)
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
			return nil, err
		}
		log.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
		return nil, err
	}

	var esResponse ESResponse
	if err := json.NewDecoder(res.Body).Decode(&esResponse); err != nil {
		log.Error("Error: ", err)
		return nil, err
	}

	var parsed interface{}

	for i := range esResponse.Hits.HitDetails {
		// parse each message into appropriate types
		hit := &esResponse.Hits.HitDetails[i]
		message := hit.Source.Message
		switch message {
		case "/Network/PeerConnections":
			var eventDetails PeerConnectionDetails
			if err := json.Unmarshal(hit.Source.RawData, &eventDetails); err != nil {
				log.Error("Error while unmarshalling event: ", err)
			}
			parsed = eventDetails
		case "/Network/ConnectPeer":
			var eventDetails ConnectPeerDetails
			if err := json.Unmarshal(hit.Source.RawData, &eventDetails); err != nil {
				log.Error("Error while unmarshalling event: ", err)
			}
			parsed = eventDetails
		case "/Network/DisconnectPeer":
			var eventDetails DisconnectPeerDetails
			if err := json.Unmarshal(hit.Source.RawData, &eventDetails); err != nil {
				log.Error("Error while unmarshalling event: ", err)
			}
			parsed = eventDetails
		}
		hit.Source.ParsedData = parsed
	}
	return esResponse.Hits.HitDetails, nil
}

// buildQueryString builds a query string based on the parameters timestamp, size and after.
// Look at the description for QueryTelemetryEvents for more information.
func (esClient *ESClient) buildQueryString(queryString, timestamp string, size int, after int) io.Reader {
	var builder strings.Builder
	builder.WriteString("{\n")
	if queryString == "" {
		builder.WriteString(fmt.Sprintf(query, timestamp))
	} else {
		builder.WriteString(queryString)
	}

	builder.WriteString(",\n")
	if size == 0 {
		builder.WriteString(fmt.Sprintf(`	"size": %d`, esClient.querySize))
	} else {
		builder.WriteString(fmt.Sprintf(`	"size": %d`, size))
	}

	if after != 0 {
		builder.WriteString(",\n")
		builder.WriteString(fmt.Sprintf(`	"search_after": [%d]`, after))
	}
	builder.WriteString("\n}")

	return strings.NewReader(builder.String())
}

func (esClient *ESClient) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(time.Duration(DefaultLocalConfig.ESQueryRefreshPeriod) * time.Second)

	// initially query for messages from the past hour
	queryTimestamp := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339Nano)

	for {
		select {
		case <-ctx.Done():
			log.Info("Terminating query service")
			return
		case <-ticker.C:
			searchAfter := 0
			var lastTimestamp string

			for {
				hits, err := esClient.QueryTelemetryEvents("", queryTimestamp, 0, searchAfter)
				if err != nil {
					break
				}

				for _, hit := range hits {
					esClient.telemetryMessageQueue.Enqueue(hit)
					esClient.metrics.metricsMutex.Lock()
					esClient.metrics.latestEnqueuedTimestamp = hit.Source.Timestamp
					esClient.metrics.metricsMutex.Unlock()
				}

				respSize := len(hits)
				if respSize == 0 {
					break
				}

				searchAfter = hits[len(hits)-1].Sort[0]
				lastTimestamp = hits[len(hits)-1].Source.Timestamp
				log.Debug("Query last timestamp: ", lastTimestamp)

			}
			if lastTimestamp != "" {
				queryTimestamp = lastTimestamp
				log.Debug("Query timestamp updated: ", queryTimestamp)
			}
		}
	}
}
