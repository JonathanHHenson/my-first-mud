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

func checkAuth(sessionToken string) (string, *net.UserInfo, error) {
	var clientID = uuid.New().String()
	var userInfo *net.UserInfo = &net.UserInfo{Username: sessionToken}

	return clientID, userInfo, nil
}

func handleWsRequestWithManager(cm *net.ConnectionManager) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if sessionToken := r.URL.Query().Get("token"); sessionToken == "" {
			log.Print("auth: missing token")
			return
		}

		clientId, userInfo, err := checkAuth(r.URL.Query().Get("token"))
		if err != nil {
			log.Print("auth:", err)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		cm.AddClient(clientId, conn, userInfo)
	}
}

func echoMessages(cm *net.ConnectionManager) {
	for {
		var msg = <-cm.Receive
		cm.SendToClient(msg.ClientId, msg.Data)
	}
}

func main() {
	flag.Parse()
	log.SetFlags(0)

	var addr = fmt.Sprintf("localhost:%d", *port)
	var wsEndpoint = "/ws"
	log.Printf("Websocket is available at address ws://%s%s", addr, wsEndpoint)

	var cm = net.NewConnectionManager(
		net.DefaultConnectionConfig(),
		10,
	)
	go echoMessages(cm)

	var handleWsRequest = handleWsRequestWithManager(cm)
	http.HandleFunc(wsEndpoint, handleWsRequest)
	log.Fatal(http.ListenAndServe(addr, nil))
}
