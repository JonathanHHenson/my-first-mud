package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/JonathanHHenson/my-first-mud/server/internal/net"
)

var port = flag.Int("port", 8080, "Port to listen on")

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins
	},
}

func handle_ws_request_with_manager(cm *net.ConnectionManager) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		var clientId = uuid.New().String()
		cm.AddClient(clientId, c)
	}
}

func echo_messages(cm *net.ConnectionManager) {
	for {
		var msg = <-cm.Receive
		var client = cm.Client(msg.ClientId)
		client.Send <- msg.Data
	}
}

func main() {
	flag.Parse()
	log.SetFlags(0)

	var addr = fmt.Sprintf("localhost:%d", *port)
	var ws_endpoint = "/ws"
	log.Printf("Websocket is available at address ws://%s%s", addr, ws_endpoint)

	var cm = net.NewConnectionManager(
		net.DefaultConnectionConfig(),
		10,
	)
	go echo_messages(cm)

	var handle_ws_request = handle_ws_request_with_manager(cm)
	http.HandleFunc(ws_endpoint, handle_ws_request)
	log.Fatal(http.ListenAndServe(addr, nil))
}
