package net

type IncomingMessage struct {
	ClientId string
	Data     []byte
}

type OutgoingMessage []byte
