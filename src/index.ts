import  Graph from "graphology";
import randomLayout from "graphology-layout/random";
import noverlap, { NoverlapNodeReducer } from "graphology-layout-noverlap";
import { Attributes, EdgeKey, NodeKey } from "graphology-types";
import { animateNodes } from "sigma/utils/animate";
import FA2Layout from "graphology-layout-forceatlas2/worker";
import NoverlapLayout from 'graphology-layout-forceatlas2/worker';
import { Sigma } from "sigma";
import { globalize } from "./utils";

// Retrieve sigma contaienr
const container = document.getElementById("sigma-container");

// Create the graph structure
const graph = new Graph();

let highlightedNodes = new Set();
let highlightedEdges = new Set();

const nodeReducer = (node: NodeKey, data: Attributes) => {
  if(highlightedNodes.has(node)){
    return { ...data, color: "#f00", zIndex: 1};
  }

  return data;
}

const edgeReducer = (edge: EdgeKey, data: Attributes) => {
  if(highlightedEdges.has(edge)){
    return { ...data, color: "#f00", zIndex: 1};
  }

  return data;
}

// SIgma settings
const settings = {
  nodeReducer: nodeReducer,
  edgeReducer: edgeReducer,
  zIndex: true,
};

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

const layout = new FA2Layout(graph, { settings: { slowDown: 1000000 } });
layout.start();

const inputReducer: NoverlapNodeReducer = (key, attr) => {
  return { ...attr, ...renderer.graphToViewport(attr) };
};

const outputReducer: NoverlapNodeReducer = (key, attr) => {
  return { ...attr, ...renderer.graphToViewport(attr) };
};

// setup websocket connection
const ws = new WebSocket('ws://localhost:8080');

// Event listeners

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

renderer.on("clickNode", ({node, captor, event}) => {
  console.log("Clicking: ", node, captor, event);
  var neighbors = graph.neighbors(node);
  neighbors.forEach(neighbor => {
    highlightedNodes.add(neighbor);    
  });
  highlightedNodes.add(node);
  var edges = graph.edges(node);
  edges.forEach(edge => {
    highlightedEdges.add(edge);
  });

  renderer.refresh();

  // pan camera to node
  renderer.getCamera().animate(renderer.getNodeDisplayData(node) as { x: number; y: number }, {
    easing: "linear",
    duration: 500,
  });

});

renderer.on("clickStage", ({node, captor, event})=>{
  console.log("Clicking stage: ", node, captor, event);
  highlightedNodes.clear();
  highlightedEdges.clear();
  renderer.getCamera().animatedReset({duration: 500});
  renderer.refresh();
})