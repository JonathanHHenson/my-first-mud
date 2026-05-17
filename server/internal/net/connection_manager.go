package net

import (
	"context"
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
	clientId string
	UserInfo *UserInfo

	// Websocket
	conn *websocket.Conn
	send chan OutgoingMessage

	// Synchronization
	cancelIo  context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

func (c *Client) Send(message OutgoingMessage) bool {
	select {
	case c.send <- message:
		return true
	case <-c.done:
		return false
	default:
		log.Printf("disconnecting slow client %s [%s]: send buffer full", c.UserInfo.Username, c.clientId)
		c.close()
		return false
	}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		c.cancelIo()
		c.conn.Close()
	})
}

type ConnectionConfig struct {
	SendBufferSize int
	PongWait       time.Duration
	PingPeriod     time.Duration
	WriteWait      time.Duration
	MaxMessageSize int64
}

func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		SendBufferSize: 10,
		PongWait:       60 * time.Second,
		PingPeriod:     50 * time.Second,
		WriteWait:      10 * time.Second,
		MaxMessageSize: 1024,
	}
}

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

func (cm *ConnectionManager) IterClients() chan *Client {
	ch := make(chan *Client)
	go func() {
		defer close(ch)

		cm.mu.RLock()
		clients := make([]*Client, 0, len(cm.clients))
		for _, client := range cm.clients {
			clients = append(clients, client)
		}
		cm.mu.RUnlock()

		for _, client := range clients {
			ch <- client
		}
	}()
	return ch
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

	go cm.runClient(ioCtx, client)
}

func (cm *ConnectionManager) removeClient(client *Client) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.clients[client.clientId] == client {
		delete(cm.clients, client.clientId)
		log.Printf("removed client %s [%s]", client.UserInfo.Username, client.clientId)
	}
}

func (cm *ConnectionManager) runClient(ctx context.Context, client *Client) {
	defer close(client.done)
	defer cm.removeClient(client)

	client.conn.SetReadLimit(cm.config.MaxMessageSize)
	client.conn.SetReadDeadline(time.Now().Add(cm.config.PongWait))
	client.conn.SetPongHandler(func(string) error {
		client.conn.SetReadDeadline(time.Now().Add(cm.config.PongWait))
		return nil
	})

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		errCh <- cm.handleRead(ctx, client)
	}()
	go func() {
		defer wg.Done()
		errCh <- cm.handleWrite(ctx, client)
	}()

	<-errCh
	client.close()
	wg.Wait()
}

func (cm *ConnectionManager) handleRead(ctx context.Context, client *Client) error {
	for {
		_, message, err := client.conn.ReadMessage()
		if err != nil {
			ctxCancelled := ctx.Err() != nil
			websocketClosed := websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway)
			if ctxCancelled || websocketClosed {
				log.Printf("closed read for client %s [%s]", client.UserInfo.Username, client.clientId)
			} else {
				log.Printf("read error for client %s [%s]: %v", client.UserInfo.Username, client.clientId, err)
			}
			return err
		}
		select {
		case cm.Receive <- IncomingMessage{ClientId: client.clientId, Data: message}:
		case <-ctx.Done():
			return ctx.Err()
		}
		log.Printf("received from client %s [%s]: %s", client.UserInfo.Username, client.clientId, message)
	}
}

func (cm *ConnectionManager) handleWrite(ctx context.Context, client *Client) error {
	ticker := time.NewTicker(cm.config.PingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("closed write for client %s [%s]", client.UserInfo.Username, client.clientId)
			return ctx.Err()
		case message := <-client.send:
			client.conn.SetWriteDeadline(time.Now().Add(cm.config.WriteWait))
			err := client.conn.WriteMessage(websocket.BinaryMessage, message)
			if err != nil {
				log.Printf("write error for client %s [%s]: %v", client.UserInfo.Username, client.clientId, err)
				return err
			}
			log.Printf("sent to client %s [%s]: %s", client.UserInfo.Username, client.clientId, message)
		case <-ticker.C:
			client.conn.SetWriteDeadline(time.Now().Add(cm.config.WriteWait))
			err := client.conn.WriteMessage(websocket.PingMessage, nil)
			if err != nil {
				log.Printf("ping error for client %s [%s]: %v", client.UserInfo.Username, client.clientId, err)
				return err
			}
		}
	}
}
