package server

import (
	"context"
	"encoding/json"
	"html/template"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/algorand/go-deadlock"
	log "github.com/sirupsen/logrus"
)

type PromClient struct {
	queryStringsMu deadlock.RWMutex
	prefixURL      *url.URL
	httpClient     *http.Client
	handler        *ConnectionHandler
	queryStrings   []string
	restartChan    chan bool
}

// PromQueryResp stores the response from the metrics server
type PromQueryResp struct {
	Status string
	Data   QueryRespData
}

type QueryRespData struct {
	ResultType    string
	RawResults    []json.RawMessage `json:"result"`
	ParsedResults []interface{}
}

// Different types of results
type VectorResult struct {
	Metric struct {
		Instance         string
		TelemetrySession string `json:"telemetry_session"`
	}
	Value [2]interface{}
}

type MetricsMessage struct {
	MessageType string            `json:"message"`
	MetricName  string            `json:"name"`
	Metrics     map[string]string `json:"metrics"`
}

func MakeMetricsMessage(metricName string, queryResp QueryRespData) MetricsMessage {
	resultType := queryResp.ResultType
	metricsMessage := MetricsMessage{
		MessageType: "Metrics",
		MetricName:  metricName,
		Metrics:     make(map[string]string),
	}
	switch resultType {
	case "vector":
		for _, vector := range queryResp.ParsedResults {
			parsedVector, ok := vector.(VectorResult)
			if !ok {
				log.Error("Cannot parse into VectorResult")

			}
			value, ok := parsedVector.Value[1].(string)
			if !ok {
				log.Error("Cannot parse value to string")
			}
			metricsMessage.Metrics[parsedVector.Metric.TelemetrySession] = value
		}
	}
	return metricsMessage
}

var defaultMetrics = []string{"algod_tx_pool_count"}

func makePromPrefix() *url.URL {
	baseURL, err := url.Parse(CurrentConfig.MetricsEndpoint)
	if err != nil {
		log.Error("makePromPrefix(): ", err)
	}

	queryURL, err := url.Parse("api/v1/query")
	if err != nil {
		log.Error("makePromPrefix(): ", err)
	}

	return baseURL.ResolveReference(queryURL)
}

func MakePromClient(handler *ConnectionHandler) *PromClient {
	return &PromClient{
		queryStringsMu: deadlock.RWMutex{},
		prefixURL:      makePromPrefix(),
		httpClient:     &http.Client{Timeout: 10 * time.Second},
		handler:        handler,
		queryStrings:   defaultMetrics,
		restartChan:    make(chan bool),
	}
}

func (pClient *PromClient) QueryMetrics(queryString string) (*QueryRespData, error) {
	queryURL, err := url.Parse("")
	if err != nil {
		log.Error("QueryMetrics(): Unable to create URL: ", err)
	}
	queryValues := queryURL.Query()
	queryValues.Set("query", queryString)
	queryValues.Set("time", time.Now().UTC().Format(time.RFC3339Nano))
	queryURL.RawQuery = queryValues.Encode()

	formattedQueryURL := pClient.prefixURL.ResolveReference(queryURL)

	// log.Debug(formattedQueryURL.String())

	resp, err := pClient.httpClient.Get(formattedQueryURL.String())
	if err != nil {
		log.Error("QueryMetrics(): ", err)
		return nil, err
	}
	// log.Debug(resp)

	defer resp.Body.Close()

	var queryResp PromQueryResp
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		log.Error("QueryMetrics(): ", err)
		return nil, err
	}

	switch queryResp.Data.ResultType {
	case "vector":
		for _, rawMessage := range queryResp.Data.RawResults {
			var vector VectorResult
			if err := json.Unmarshal(rawMessage, &vector); err != nil {
				log.Error("Error while unmarshalling VectorResult: ", err)
			}
			queryResp.Data.ParsedResults = append(queryResp.Data.ParsedResults, vector)
		}
	}

	return &queryResp.Data, nil

}

func (pClient *PromClient) sendMetrics(ctx context.Context, metricQueryString string) {
	ticker := time.NewTicker(time.Duration(CurrentConfig.MetricsRefreshPeriod) * time.Millisecond)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, err := pClient.QueryMetrics(metricQueryString)
			if err != nil {
				break
			}
			metricsMessage := MakeMetricsMessage(metricQueryString, *result)
			pClient.handler.Send <- metricsMessage
		}
	}
}

func (pClient *PromClient) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	metricsCtx, cancel := context.WithCancel(ctx)
	pClient.startMetrics(metricsCtx)

	for {
		select {
		case <-pClient.restartChan:
			cancel()
			metricsCtx, cancel = context.WithCancel(ctx)
			pClient.startMetrics(metricsCtx)
		case <-ctx.Done():
			cancel()
			log.Info("Terminating metrics service")
			return
		}
	}
}

func (pClient *PromClient) startMetrics(ctx context.Context) {
	pClient.queryStringsMu.RLock()
	defer pClient.queryStringsMu.RUnlock()
	for _, metricQueryString := range pClient.queryStrings {
		if metricQueryString == "" {
			continue
		}
		go pClient.sendMetrics(ctx, metricQueryString)
	}
}

func (pClient *PromClient) metricsAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		pClient.queryStringsMu.RLock()
		defer pClient.queryStringsMu.RUnlock()
		var metricsBuilder strings.Builder
		for _, metric := range pClient.queryStrings {
			metricsBuilder.WriteString(metric)
			metricsBuilder.WriteString("\n")
		}
		displayTemplate(w, "metrics", metricsBuilder.String(), nil)
		return
	} else {
		pClient.queryStringsMu.Lock()
		defer pClient.queryStringsMu.Unlock()

		metricStrings := strings.Split(strings.TrimSpace(r.FormValue("data")), "\n")
		pClient.queryStrings = metricStrings
		pClient.restartChan <- true
		displayTemplate(w, "metrics", r.FormValue("data"), nil)
		return
	}
}

func displayTemplate(w http.ResponseWriter, name string, data string, err error) {
	template := template.Must(template.ParseFiles("templates/form.html"))
	template.Execute(w, struct {
		Name  string
		Data  string
		Error error
	}{name, data, err})
}
