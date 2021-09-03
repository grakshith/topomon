import  DirectedGraph from "graphology";
import randomLayout from "graphology-layout/random";
import noverlap, { NoverlapNodeReducer } from "graphology-layout-noverlap";
import { Attributes, EdgeKey, NodeKey } from "graphology-types";
import { animateNodes } from "sigma/utils/animate";
import FA2Layout from "graphology-layout-forceatlas2/worker";
import NoverlapLayout from 'graphology-layout-forceatlas2/worker';
import { Sigma } from "sigma";
import { globalize } from "./utils";
import $ from "jquery";

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

  return {...data, zIndex: 0};
}

const edgeReducer = (edge: EdgeKey, data: Attributes) => {
  if(highlightedEdges.has(edge)){
    return { ...data, color: "#00f", zIndex: 1};
  }

  return {...data, zIndex:0};
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

// node and edge dicts
let nodeArray:string[] = [];
let sessionMap:Map<string, string> = new Map();
let metricsMap:Map<string, Map<string, string>> = new Map();

globalize({graph, renderer, sessionMap, metricsMap});

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
const ws = new WebSocket('ws://'+window.location.host+'/ws');

// metrics selector
var currentMetric:string;

// Functions

function updateMetricsDivPositions(){
  $(".metrics-tooltip").each(function(){
    var div = $(this)
    var nodeName = div.attr("id");
    var nodeDisplayData = renderer.getNodeDisplayData(nodeName);
    var coords = renderer.framedGraphToViewport({x: nodeDisplayData.x, y:nodeDisplayData.y});
    var size = nodeDisplayData.size;
    div.css({left:coords.x+size, top:coords.y+size});
  });
}

function createMetricsDiv(nodeName:string){
  var nodeTooltip = {
    id:nodeName,
    class:"metrics-tooltip",
    css:{
      "position": "absolute",
      "background": "white",
      "border": "1px solid blue"
    }
  };
  var $div = $("<div>", nodeTooltip);
  $div.hide();
  $("#sigma-container").append($div);
}

function createMetricButton(metricName: string){
  var metricButton = {
    type: "radio",
    id: "r-"+metricName,
    name: "metrics-radio-button",
    value: metricName,
    click: function() {currentMetric = metricName;}
  };
  var $but = $("<input/>", metricButton);
  var metricButtonLabel = {
    for: metricName,
    html: metricName
  };
  var $label = $("<label>", metricButtonLabel);
  $("#panel").append($but);
  $("#panel").append($label);
}

function displayMetricsDiv(nodeName: string){
  var div = $("#"+$.escapeSelector(nodeName));
  if(div.text()!=""){
    div.show();
  }
}

function nodeSize(nodeName: string): number {
  var splits = nodeName.split(":");
  if(splits.length==2){
    if(splits[0].startsWith("R")){
      return 7;
    }
  }
  return 4;
}

// Event listeners

ws.addEventListener('open', function(event){
    console.log("ws: Connection opened - ", event);
});

ws.addEventListener('message', function(event){
    // console.log("ws: ", event);
    var wsEvent = JSON.parse(event.data)
    // console.log("ws: ", wsEvent)
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
              size: nodeSize(wsEvent.source.name),
              label: wsEvent.source.name,
              });
            createMetricsDiv(wsEvent.source.name);
          }
          if(wsEvent.source.session!==""){
            sessionMap.set(wsEvent.source.name, wsEvent.source.session);
          }
          if(graph.hasNode(wsEvent.target.name)==false){
            graph.mergeNode(wsEvent.target.name, {
              x: Math.random(),
              y: Math.random(),
              size: nodeSize(wsEvent.target.name),
              label: wsEvent.target.name,
              });
            createMetricsDiv(wsEvent.target.name);  
          }
          if(wsEvent.target.session!==""){
            sessionMap.set(wsEvent.target.name, wsEvent.target.session);
          }
          var edge = graph.mergeEdge(wsEvent.source.name, wsEvent.target.name);
          graph.mergeEdgeAttributes(edge, {
            zIndex: 0,
          });
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
              size: nodeSize(edge.source.name),
              label: edge.source.name,
            });
            createMetricsDiv(edge.source.name);
          }
          if(edge.source.session!==""){
            sessionMap.set(edge.source.name, edge.source.session);
          }
          if(graph.hasNode(edge.target.name)==false){
            graph.mergeNode(edge.target.name, {
              x: Math.random(),
              y: Math.random(),
              size: nodeSize(edge.target.name),
              label: edge.target.name,
            });
            createMetricsDiv(edge.target.name);
          }
          if(edge.target.session!=""){
            sessionMap.set(edge.target.name, edge.target.session);
          }
          var e = graph.mergeEdge(edge.source.name, edge.target.name);
          graph.mergeEdgeAttributes(e, {
            zIndex: 0,
          });
        });
        break;
      case "Metrics":
        var metricName = wsEvent.name;
        if(!document.getElementById("r-"+metricName)){
          createMetricButton(metricName);
        }
        if(currentMetric!==metricName){
          break;
        }
        metricsMap.set(currentMetric, new Map(Object.entries(wsEvent.metrics)));
        $(".metrics-tooltip").each(function(){
          var div = $(this)
          var nodeName = div.attr("id");
          var sessionKey = sessionMap.get(nodeName);
          if(sessionKey!=undefined){
            var metricValue = metricsMap.get(currentMetric).get(sessionKey);
            if(metricValue!=undefined){
              var prevMetricValue = div.text();
              if(prevMetricValue!==""){
                var delta = parseInt(prevMetricValue);
              }
              var metricValueInt = parseInt(metricValue);
              delta = metricValueInt - delta;
              div.text(metricValue+": "+delta);
              if(metricValueInt<5000){
                div.css("border", "1px solid green");
              }
              else if(metricValueInt>=5000 && metricValueInt<10000){
                div.css("border", "1px solid blue");
              }
              else{
                div.css("border", "1px solid red");
              }
            }
          }
        });
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
    displayMetricsDiv(neighbor);
  });
  highlightedNodes.add(node);
  var edges = graph.edges(node);
  edges.forEach(edge => {
    highlightedEdges.add(edge);
  });

  renderer.refresh();
  displayMetricsDiv(node);
  // pan camera to node
  // renderer.getCamera().animate(renderer.getNodeDisplayData(node) as { x: number; y: number }, {
  //   easing: "linear",
  //   duration: 500,
  // });

});

renderer.on("clickStage", ({node, captor, event})=>{
  console.log("Clicking stage: ", node, captor, event);
  highlightedNodes.clear();
  highlightedEdges.clear();

  $(".metrics-tooltip").each(function(){
  var div = $(this)
  div.hide();
  });
  renderer.getCamera().animatedReset({duration: 500});
  renderer.refresh();
});

window.setInterval(updateMetricsDivPositions, 10);