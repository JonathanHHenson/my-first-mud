package net

import (
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type IncomingMessage struct {
	ClientId string
	Data     []byte
}

type OutgoingMessage []byte

type Client struct {
	conn     *websocket.Conn
	ClientId string
	Send     chan OutgoingMessage
}

type ConnectionConfig struct {
	SendBufferSize int
	PongWait       time.Duration
	PingPeriod     time.Duration
	MaxMessageSize int64
}

type ConnectionManager struct {
	clients map[string]*Client
	mu      sync.RWMutex
	config  ConnectionConfig
	Receive chan IncomingMessage
}

func (cm *ConnectionManager) IterClients() chan *Client {
	ch := make(chan *Client)
	go func() {
		defer close(ch)

		cm.mu.RLock()
		defer cm.mu.RUnlock()
		for _, client := range cm.clients {
			ch <- client
		}
	}()
	return ch
}

func (cm *ConnectionManager) Client(clientId string) *Client {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.clients[clientId]
}

func (cm *ConnectionManager) closeClient(clientId string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if client, ok := cm.clients[clientId]; ok {
		defer log.Printf("closed client %s", clientId)
		defer client.conn.Close()

		delete(cm.clients, clientId)
	}
}

func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		SendBufferSize: 0,
		PongWait:       60 * time.Second,
		PingPeriod:     50 * time.Second,
		MaxMessageSize: 1024,
	}
}

func NewConnectionManager(config ConnectionConfig, receiveBufferSize int) *ConnectionManager {
	return &ConnectionManager{
		clients: make(map[string]*Client),
		config:  config,
		Receive: make(chan IncomingMessage, receiveBufferSize),
	}
}

func (cm *ConnectionManager) AddClient(clientId string, conn *websocket.Conn) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.clients[clientId] = &Client{
		conn:     conn,
		ClientId: clientId,
		Send:     make(chan OutgoingMessage, cm.config.SendBufferSize),
	}

	go handleRead(cm, clientId)
	go handleWrite(cm, clientId)
}

func handleRead(cm *ConnectionManager, clientId string) {
	client := cm.Client(clientId)
	defer cm.closeClient(clientId)

	client.conn.SetReadLimit(cm.config.MaxMessageSize)
	client.conn.SetReadDeadline(time.Now().Add(cm.config.PongWait))
	client.conn.SetPongHandler(func(string) error {
		client.conn.SetReadDeadline(time.Now().Add(cm.config.PongWait))
		return nil
	})

	for {
		_, message, err := client.conn.ReadMessage()
		if err != nil {
			log.Printf("read error for client %s: %v", client.ClientId, err)
			return
		}
		cm.Receive <- IncomingMessage{ClientId: clientId, Data: message}
		log.Printf("received: %s", message)
	}
}

func handleWrite(cm *ConnectionManager, clientId string) {
	client := cm.Client(clientId)
	defer cm.closeClient(clientId)
	ticker := time.NewTicker(cm.config.PingPeriod)
	defer ticker.Stop()

	for {
		select {
		case msg := <-client.Send:
			err := client.conn.WriteMessage(websocket.BinaryMessage, msg)
			if err != nil {
				log.Printf("write error for client %s: %v", client.ClientId, err)
				return
			}
		case <-ticker.C:
			err := client.conn.WriteMessage(websocket.PingMessage, nil)
			if err != nil {
				log.Printf("ping error for client %s: %v", client.ClientId, err)
				return
			}
		}
	}
}
