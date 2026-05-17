package net

import (
	"context"
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

type ConnectionManager struct {
	clients map[string]*Client
	mu      sync.RWMutex
	config  ConnectionConfig
	Receive chan IncomingMessage
}

func NewConnectionManager(config ConnectionConfig, receiveBufferSize int) *ConnectionManager {
	return &ConnectionManager{
		clients: make(map[string]*Client),
		config:  config,
		Receive: make(chan IncomingMessage, receiveBufferSize),
	}
}

func (cm *ConnectionManager) IterClients() []*Client {
	cm.mu.RLock()
	clients := make([]*Client, 0, len(cm.clients))
	for _, client := range cm.clients {
		clients = append(clients, client)
	}
	cm.mu.RUnlock()
	return clients
}

func (cm *ConnectionManager) SendToClient(clientId string, message OutgoingMessage) bool {
	client := cm.Client(clientId)
	if client == nil {
		return false
	}
	return client.Send(message)
}

func (cm *ConnectionManager) Client(clientId string) *Client {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.clients[clientId]
}

func (cm *ConnectionManager) AddClient(clientId string, conn *websocket.Conn, userInfo *UserInfo) {
	ioCtx, cancelIo := context.WithCancel(context.Background())
	client := &Client{
		clientId: clientId,
		UserInfo: userInfo,

		conn: conn,
		send: make(chan OutgoingMessage, cm.config.SendBufferSize),

		cancelIo: cancelIo,
		done:     make(chan struct{}),
	}

	cm.mu.Lock()
	oldClient := cm.clients[clientId]
	cm.clients[clientId] = client
	cm.mu.Unlock()

	if oldClient != nil {
		log.Printf("client %s [%s] is already connected, closing previous connection..", userInfo.Username, clientId)
		oldClient.close()
		<-oldClient.done
		log.Printf("previous connection has been closed for client %s [%s]", userInfo.Username, clientId)
	}

	go func() {
		client.run(ioCtx, cm.Receive, &cm.config)
		cm.removeClient(client)
	}()
}

func (cm *ConnectionManager) removeClient(client *Client) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.clients[client.clientId] == client {
		delete(cm.clients, client.clientId)
		log.Printf("removed client %s [%s]", client.UserInfo.Username, client.clientId)
	}
}
