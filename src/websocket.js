ws = new WebSocket('ws://localhost:8080');

ws.addEventListener('open', function(event){
    console.log("ws: Connection opened");
});

ws.addEventListener('message', function(event){
    console.log("ws: Message ", event.data);
});
