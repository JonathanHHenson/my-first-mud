package net

import (
	"context"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	clientId string
	UserInfo *UserInfo

	// Websocket
	conn *websocket.Conn
	send chan OutgoingMessage

	// Synchronization
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

func NewClient(clientId string, conn *websocket.Conn, userInfo *UserInfo, sendBufferSize int) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		clientId: clientId,
		UserInfo: userInfo,

		conn: conn,
		send: make(chan OutgoingMessage, sendBufferSize),

		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
}

func (c *Client) Done() <-chan struct{} {
	return c.done
}

func (c *Client) Send(message OutgoingMessage) bool {
	// Check if the client is done before sending
	select {
	case <-c.ctx.Done():
		return false
	default:
	}

	select {
	case c.send <- message:
		return true
	case <-c.ctx.Done():
		return false
	default:
		log.Printf("disconnecting slow client %s [%s]: send buffer full", c.UserInfo.Username, c.clientId)
		c.Close()
		return false
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.conn.Close()
		log.Printf("closed connection for client %s [%s]", c.UserInfo.Username, c.clientId)
	})
}

func recoverAndLog(name string, c *Client) {
	if r := recover(); r != nil {
		log.Printf("panic in %s for client %s [%s]: %v\n%s", name, c.UserInfo.Username, c.clientId, r, debug.Stack())
	}
}

func (c *Client) run(recv chan<- IncomingMessage, connConfig *ConnectionConfig) {
	defer close(c.done)
	defer c.Close()
	defer recoverAndLog("run", c)

	c.conn.SetReadLimit(connConfig.MaxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(connConfig.PongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(connConfig.PongWait))
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.handleRead(recv, connConfig)
	}()
	go func() {
		defer wg.Done()
		c.handleWrite(connConfig)
	}()
	wg.Wait()
}

func (c *Client) handleRead(recv chan<- IncomingMessage, connConfig *ConnectionConfig) error {
	defer c.Close()
	defer recoverAndLog("handleRead", c)

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			ctxCancelled := c.ctx.Err() != nil
			websocketClosed := websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway)
			if ctxCancelled || websocketClosed {
				log.Printf("closed read for client %s [%s]", c.UserInfo.Username, c.clientId)
			} else {
				log.Printf("read error for client %s [%s]: %v", c.UserInfo.Username, c.clientId, err)
			}
			return err
		}
		select {
		case recv <- IncomingMessage{ClientId: c.clientId, Data: message}:
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
		log.Printf("received from client %s [%s]: %s", c.UserInfo.Username, c.clientId, message)
	}
}

func (c *Client) handleWrite(connConfig *ConnectionConfig) error {
	defer c.Close()
	defer recoverAndLog("handleWrite", c)

	ticker := time.NewTicker(connConfig.PingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Printf("closed write for client %s [%s]", c.UserInfo.Username, c.clientId)
			return c.ctx.Err()
		case message := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(connConfig.WriteWait))
			err := c.conn.WriteMessage(websocket.BinaryMessage, message)
			if err != nil {
				log.Printf("write error for client %s [%s]: %v", c.UserInfo.Username, c.clientId, err)
				return err
			}
			log.Printf("sent to client %s [%s]: %s", c.UserInfo.Username, c.clientId, message)
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(connConfig.WriteWait))
			err := c.conn.WriteMessage(websocket.PingMessage, nil)
			if err != nil {
				log.Printf("ping error for client %s [%s]: %v", c.UserInfo.Username, c.clientId, err)
				return err
			}
		}
	}
}
