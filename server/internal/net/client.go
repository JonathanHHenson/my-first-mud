package net

import (
	"log/slog"
	"sync"
	"time"

	"github.com/lxzan/gws"
)

type Client struct {
	ID       uint64
	UserInfo UserInfo

	conn        *gws.Conn
	config      *Config
	send        chan OutboundMessage
	done        chan struct{}
	closeOnce   sync.Once
	cleanupOnce sync.Once
}

type OutboundMessage []byte

func newClient(id uint64, conn *gws.Conn, userInfo UserInfo, config *Config) *Client {
	return &Client{
		ID:       id,
		UserInfo: userInfo,

		conn:   conn,
		config: config,
		send:   make(chan OutboundMessage, config.SendBufferSize),
		done:   make(chan struct{}),
	}
}

func (c *Client) Start() {
	go c.writeLoop()
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		_ = c.conn.WriteClose(1000, []byte("closing"))
	})
}

func (c *Client) Done() <-chan struct{} {
	return c.done
}

func (c *Client) TrySend(msg OutboundMessage) bool {
	if c == nil {
		return false
	}

	select {
	case <-c.done:
		return false
	default:
	}

	select {
	case <-c.done:
		return false
	case c.send <- msg:
		return true

	default:
		// Slow clients will be disconnected
		c.Close()
		return false
	}
}

func (c *Client) cleanup() {
	c.cleanupOnce.Do(func() {
		close(c.done)
	})
}

func (c *Client) writeLoop() {
	ticker := time.NewTicker(c.config.PingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return

		case msg, ok := <-c.send:
			if !ok {
				return
			}

			_ = c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteWait))
			if err := c.conn.WriteMessage(gws.OpcodeBinary, msg); err != nil {
				slog.Debug("failed to send message", "client_id", c.ID, "message", string(msg), "error", err)
				c.Close()
				return
			}
			slog.Debug("sent message", "client_id", c.ID, "message", string(msg))

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteWait))
			if err := c.conn.WritePing(nil); err != nil {
				slog.Debug("failed to send ping", "client_id", c.ID, "error", err)
				c.Close()
				return
			}
			slog.Debug("sent ping", "client_id", c.ID)
		}
	}
}
