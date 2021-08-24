import  DirectedGraph from "graphology";
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
const graph = new DirectedGraph();

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
  defaultEdgeType: "arrow",
  renderEdgeLabels: true,
  nodeReducer: nodeReducer,
  edgeReducer: edgeReducer,
  zIndex: true,
};

// Sigma init & render
const renderer = new Sigma(graph, container, settings);

globalize({graph, renderer});

// node and edge dicts
let nodeArray:string[] = [];
let sessionMap = new Map();

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
const ws = new WebSocket('ws://localhost:8080/ws');

// Event listeners

ws.addEventListener('open', function(event){
    console.log("ws: Connection opened - ", event);
});

ws.addEventListener('message', function(event){
    console.log("ws: ", event);
    var wsEvent = JSON.parse(event.data)
    switch(wsEvent.message){
      // case "AddNode":
      //     var node = graph.mergeNode(wsEvent.node, {
      //       x: Math.random(),
      //       y: Math.random(),
      //       size: 4,
      //       label: wsEvent.node,
      //     });
      //     nodeArray.push(node);
      //   break;
      case "AddEdge":
          if(graph.hasNode(wsEvent.source.name)==false){
            graph.mergeNode(wsEvent.source.name, {
              x: Math.random(),
              y: Math.random(),
              size: 4,
              label: wsEvent.source.name,
              });
              if(wsEvent.source.telemetrySession!=""){
                sessionMap.set(wsEvent.source.telemetrySession, wsEvent.source.name);
              }
          }
          if(graph.hasNode(wsEvent.target.name)==false){
            graph.mergeNode(wsEvent.target.name, {
              x: Math.random(),
              y: Math.random(),
              size: 4,
              label: wsEvent.target.name,
              });
              if(wsEvent.target.telemetrySession!=""){
                sessionMap.set(wsEvent.target.telemetrySession, wsEvent.target.name);
              }
          }
          var edge = graph.mergeEdge(wsEvent.source.name, wsEvent.target.name);
        break;
      // case "RemoveNode":
      //   if(graph.hasNode(wsEvent.node)){
      //     var node:string = wsEvent.node;
      //     graph.dropNode(node);
      //   }
      //   break;
      case "RemoveEdge":
        if(graph.hasEdge(wsEvent.source.name, wsEvent.target.name)){
          var edge = graph.edge(wsEvent.source.name, wsEvent.target.name);
          graph.dropEdge(edge);
        }
        break;
      case "NetworkGraph":
        graph.clear()
        // wsEvent.nodes.forEach((node: string) => {
        //   if(graph.hasNode(node)==false){
        //     graph.mergeNode(node, {
        //       x: Math.random(),
        //       y: Math.random(),
        //       size: 4,
        //       label: node,
        //     });  
        //   }
        // });
        wsEvent.edges.forEach((edge:any) => {
          if(graph.hasNode(edge.source.name)==false){
            graph.mergeNode(edge.source.name, {
              x: Math.random(),
              y: Math.random(),
              size: 4,
              label: edge.source.name,
            });
            if(edge.source.telemetrySession!=""){
              sessionMap.set(edge.source.telemetrySession, edge.source.name);
            }
          }
          if(graph.hasNode(edge.target.name)==false){
            graph.mergeNode(edge.target.name, {
              x: Math.random(),
              y: Math.random(),
              size: 4,
              label: edge.target.name,
            });
            if(edge.target.telemetrySession!=""){
              sessionMap.set(edge.target.telemetrySession, edge.target.name);
            }
          }
          var e = graph.mergeEdge(edge.source.name, edge.target.name);
        });
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