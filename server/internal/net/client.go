package net

import (
	"context"
	"log"
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
		println("closed connection for client %s [%s]", c.UserInfo.Username, c.clientId)
	})
}

func (c *Client) run(ctx context.Context, recv chan<- IncomingMessage, connConfig *ConnectionConfig) {
	defer close(c.done)

	c.conn.SetReadLimit(connConfig.MaxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(connConfig.PongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(connConfig.PongWait))
		return nil
	})

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		errCh <- c.handleRead(ctx, recv, connConfig)
	}()
	go func() {
		defer wg.Done()
		errCh <- c.handleWrite(ctx, connConfig)
	}()

	<-errCh
	c.close()
	wg.Wait()
}

func (c *Client) handleRead(ctx context.Context, recv chan<- IncomingMessage, connConfig *ConnectionConfig) error {
	for {
		c.conn.SetReadDeadline(time.Now().Add(connConfig.PongWait))
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			ctxCancelled := ctx.Err() != nil
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
		case <-ctx.Done():
			return ctx.Err()
		}
		log.Printf("received from client %s [%s]: %s", c.UserInfo.Username, c.clientId, message)
	}
}

func (c *Client) handleWrite(ctx context.Context, connConfig *ConnectionConfig) error {
	ticker := time.NewTicker(connConfig.PingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("closed write for client %s [%s]", c.UserInfo.Username, c.clientId)
			return ctx.Err()
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
