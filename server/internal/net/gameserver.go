package net

import (
	"sync"
	"time"
)

type GameServer struct {
	clients   map[uint64]*Client
	gameInput chan GameInput

	mu sync.RWMutex
}

type GameInput struct {
	ClientId   uint64
	Data       []byte
	ReceivedAt time.Time
}

func NewGameServer(inputBufferSize int) *GameServer {
	return &GameServer{
		clients:   make(map[uint64]*Client),
		gameInput: make(chan GameInput, inputBufferSize),
	}
}

func (gs *GameServer) RegisterClient(client *Client) {
	gs.mu.Lock()
	oldClient := gs.clients[client.Id]
	gs.clients[client.Id] = client
	gs.mu.Unlock()

	if oldClient != nil && oldClient != client {
		oldClient.Close()
	}
}

func (gs *GameServer) unregisterClient(client *Client) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if gs.clients[client.Id] == client {
		delete(gs.clients, client.Id)
	}
}

func (gs *GameServer) Input() chan<- GameInput {
	return gs.gameInput
}

func (gs *GameServer) Receive() <-chan GameInput {
	return gs.gameInput
}

func (gs *GameServer) Client(id uint64) *Client {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	return gs.clients[id]
}

func (gs *GameServer) IsCurrentClient(client *Client) bool {
	if client == nil {
		return false
	}

	gs.mu.RLock()
	defer gs.mu.RUnlock()

	return gs.clients[client.Id] == client
}

func (gs *GameServer) AllClients() []*Client {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	clients := make([]*Client, 0, len(gs.clients))
	for _, client := range gs.clients {
		clients = append(clients, client)
	}
	return clients
}
