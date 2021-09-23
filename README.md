# topomon
Algorand topology mapper

The topology mapper visualizes the nodes in an Algorand network by plotting the nnetwork topology and displaying the metrics for each node that reports them.
The server constructs the network graph by parsing the network telemetry events sent to Elasticsearch, and then each update to the network graph is streamed to the client which connects to the server. The metrics for all the nodes is queries from Prometheus.

## Building

Dependencies: nodejs>12, npm, Go>1.16

```bash
$ npm install -- Installs the dependencies for SigmaJS and TypeScript
$ npm run build -- Builds the client core js bundle
$ cd dist
$ go build ../src -- Builds the server
```

