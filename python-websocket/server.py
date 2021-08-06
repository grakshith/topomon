from websocket_server import WebsocketServer
import threading
import json

def client_join(client, server):
    print("New client joined: ", client)

def client_leave(client, server):
    print("Client left: ", client)

def client_message(client, server, message):
    print("Client: ", client, " Message: ", message)

def repl(server):
    while True:
        try:
            opt = input(">> ")
            if(opt=="1"):
                node = input("Node: ")
                type_1 = {
                    'message': 'AddNode',
                    'node': node
                }
                server.send_message_to_all(json.dumps(type_1))
            elif(opt=="2"):
                src = input("Source: ")
                target = input("Target: ")
                type_2 = {
                    'message': 'AddEdge',
                    'source': src,
                    'target': target
                }
                server.send_message_to_all(json.dumps(type_2))
            elif(opt=="3"):
                node = input("Node: ")
                type_3 = {
                    'message': 'RemoveNode',
                    'node': node
                }
                server.send_message_to_all(json.dumps(type_3))
            elif(opt=="4"):
                src = input("Source: ")
                target = input("Target: ")
                type_4 = {
                    'message': 'RemoveEdge',
                    'source': src,
                    'target': target
                }
                server.send_message_to_all(json.dumps(type_4))
        except KeyboardInterrupt:
            return


if __name__ == '__main__':
    server = WebsocketServer(8080)
    server.set_fn_new_client(client_join)
    server.set_fn_client_left(client_leave)
    server.set_fn_message_received(client_message)
    t = threading.Thread(target=repl, args=(server,))
    t.start()
    server.run_forever()
    t.join()
    # server.run_forever()