package net

import "time"

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
