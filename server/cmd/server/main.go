package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/JonathanHHenson/my-first-mud/server/internal/auth"
	"github.com/JonathanHHenson/my-first-mud/server/internal/net"
)

var port = flag.Int("port", 8080, "Port to listen on")
var debug = flag.Bool("debug", false, "Enable debug mode")

func echoMessages(gs *net.GameServer) {
	for msg := range gs.Receive() {
		if ok := gs.Client(msg.ClientId).TrySend(msg.Data); !ok {
			slog.Debug("failed to send message", "client_id", msg.ClientId)
		}
	}
}

func main() {
	flag.Parse()
	logLevel := slog.LevelInfo
	if *debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	})))

	var addr = fmt.Sprintf("localhost:%d", *port)
	var wsEndpoint = "/ws"
	var address = fmt.Sprintf("ws://%s%s", addr, wsEndpoint)
	slog.Info("websocket is available", "address", address)

	var gs = net.NewGameServer(10)
	var ap = &auth.DummyAuth{}
	var h = net.NewHandler(
		gs,
		ap,
		net.DefaultConfig(),
	)
	go echoMessages(gs)

	http.HandleFunc(wsEndpoint, h.ServeHTTP)

	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	} else {
		slog.Info("server shutdown complete")
	}
}
