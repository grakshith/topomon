import  Graph from "graphology";
import randomLayout from "graphology-layout/random";
import noverlap, { NoverlapNodeReducer } from "graphology-layout-noverlap";
import { animateNodes } from "sigma/utils/animate";
import { Sigma } from "sigma";
import { globalize } from "./utils";

// Retrieve sigma contaienr
const container = document.getElementById("sigma-container");

// Create the graph structure
const graph = new Graph();

// randomize the layout
// randomLayout.assign(graph);
// SIgma settings
const settings = {};

// Sigma init & render
const renderer = new Sigma(graph, container, settings);

globalize({graph, renderer});

// node and edge dicts
let nodeArray:string[] = [];
let edgeMap = new Map();

// layout settings
const NOVERLAP_SETTINGS = {
  margin: 2,
  ratio: 1,
  speed: 3,
};

const inputReducer: NoverlapNodeReducer = (key, attr) => {
  return { ...attr, ...renderer.graphToViewport(attr) };
};

const outputReducer: NoverlapNodeReducer = (key, attr) => {
  return { ...attr, ...renderer.graphToViewport(attr) };
};

// setup websocket connection
const ws = new WebSocket('ws://localhost:8080');

ws.addEventListener('open', function(event){
    console.log("ws: Connection opened - ", event);
});

ws.addEventListener('message', function(event){
    console.log("ws: ", event);
    var wsEvent = JSON.parse(event.data)
    switch(wsEvent.message){
      case "AddNode":
        var node = graph.addNode(wsEvent.node, {
          x: Math.random(),
          y: Math.random(),
          size: 4,
          label: wsEvent.node,
        });
        nodeArray.push(node);
        break;
      case "AddEdge":
        var edge = graph.addEdge(wsEvent.source, wsEvent.target);
        edgeMap.set(edge, {"source": wsEvent.source, "target": wsEvent.target});
        break;
      case "RemoveNode":
        var node:string = wsEvent.node;
        var index = nodeArray.indexOf(node);
        nodeArray.splice(index, 1);
        graph.dropNode(node);
        break;
      case "RemoveEdge":
        var edge = graph.edge(wsEvent.source, wsEvent.target);
        graph.dropEdge(edge);
        edgeMap.delete(edge);
        break;
    }
    renderer.refresh();
    console.log("Refreshed");
    // const layout = noverlap(graph, { inputReducer, outputReducer, maxIterations: 500, settings: NOVERLAP_SETTINGS });
    // animateNodes(graph, layout, { duration: 100, easing: "linear" }, null);
});
