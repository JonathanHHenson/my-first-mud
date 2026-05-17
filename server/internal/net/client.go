package net

import (
	"context"
	"log"
	"sync"

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
