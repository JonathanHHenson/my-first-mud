package net

import "time"

type Config struct {
	SendBufferSize int
	PongWait       time.Duration
	PingPeriod     time.Duration
	WriteWait      time.Duration
	MaxMessageSize int
}

func DefaultConfig() Config {
	return Config{
		SendBufferSize: 10,
		PongWait:       60 * time.Second,
		PingPeriod:     50 * time.Second,
		WriteWait:      10 * time.Second,
		MaxMessageSize: 1024,
	}
}

func (c Config) Deadline() time.Duration {
	return c.PingPeriod + c.PongWait
}
