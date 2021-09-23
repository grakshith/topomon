# topomon
Algorand topology mapper

The topology mapper visualizes the nodes in an Algorand network by plotting the nnetwork topology and displaying the metrics for each node that reports them.
The server constructs the network graph by parsing the network telemetry events sent to Elasticsearch, and then each update to the network graph is streamed to the client which connects to the server. The metrics for all the nodes is queries from Prometheus.

