import { UndirectedGraph } from "graphology";
import erdosRenyi from "graphology-generators/random/erdos-renyi";
import randomLayout from "graphology-layout/random";
import { Sigma } from "sigma";

// Retrieve sigma contaienr
const container = document.getElementById("sigma-container");

// Create the graph structure
const graph = erdosRenyi(UndirectedGraph, {
  order: 100,
  probability: 0.2
});

// Run the layout
randomLayout.assign(graph);

// SIgma settings
const settings = {};

// Sigma init & render
const renderer = new Sigma(graph, container, settings);
