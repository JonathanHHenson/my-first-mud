package net

import (
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/lxzan/gws"
)

type Handler struct {
	server       *GameServer
	authProvider AuthProvider
	config       Config
	upgrader     *gws.Upgrader
}

func NewHandler(server *GameServer, auth AuthProvider, config Config) *Handler {
	handler := &Handler{
		server:       server,
		authProvider: auth,
		config:       config,
	}
	handler.upgrader = gws.NewUpgrader(handler, &gws.ServerOption{
		ParallelEnabled:    true,                                  // Parallel message processing
		Recovery:           gws.Recovery,                          // Exception recovery
		PermessageDeflate:  gws.PermessageDeflate{Enabled: false}, // Disable compression (small frequent packets)
		ReadMaxPayloadSize: config.MaxMessageSize,
	})
	return handler
}

func (h *Handler) OnOpen(conn *gws.Conn) {
	clientId, userInfo, ok := loginInfoFromConn(conn)
	if !ok {
		conn.WriteClose(1000, []byte("unexpected connection with no client id. Closing..."))
		return
	}

	client := newClient(clientId, conn, userInfo, &h.config)

	conn.Session().Store("client", client)
	_ = conn.SetReadDeadline(time.Now().Add(h.config.Deadline()))

	h.server.RegisterClient(client)

	go func() {
		<-client.done
		h.server.unregisterClient(client)
	}()

	client.Start()

	slog.Info("client connected", "client_id", client.Id)
}

func (h *Handler) OnMessage(conn *gws.Conn, message *gws.Message) {
	defer message.Close()

	client, ok := clientFromConn(conn)
	if !ok || !h.server.IsCurrentClient(client) {
		return
	}

	h.server.Input() <- GameInput{
		ClientId:   client.Id,
		Data:       slices.Clone(message.Bytes()),
		ReceivedAt: time.Now(),
	}
}

func (h *Handler) OnClose(conn *gws.Conn, err error) {
	client, ok := clientFromConn(conn)
	if !ok {
		return
	}

	client.cleanup()

	slog.Info("client disconnected", "client_id", client.Id, "error", err)
}

func (h *Handler) OnPing(conn *gws.Conn, payload []byte) {
	_ = conn.SetReadDeadline(time.Now().Add(h.config.Deadline()))
	_ = conn.WritePong(payload)
}

func (h *Handler) OnPong(conn *gws.Conn, payload []byte) {
	_ = conn.SetReadDeadline(time.Now().Add(h.config.Deadline()))
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if sessionToken := r.URL.Query().Get("token"); sessionToken == "" {
		slog.Error("auth: missing token")
		http.Error(w, "auth: missing token", http.StatusUnauthorized)
		return
	}

	clientId, userInfo, err := h.authProvider.Auth(r.URL.Query().Get("token"))
	if err != nil {
		slog.Error("auth failed", "error", err)
		http.Error(w, "auth: "+err.Error(), http.StatusUnauthorized)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r)
	if err != nil {
		slog.Error("upgrade failed", "error", err)
		return
	}
	conn.Session().Store("client_id", clientId)
	conn.Session().Store("user_info", userInfo)

	go conn.ReadLoop()
}

func loginInfoFromConn(conn *gws.Conn) (uint64, UserInfo, bool) {
	clientIdValue, ok := conn.Session().Load("client_id")
	if !ok {
		return 0, UserInfo{}, false
	}

	clientId, ok := clientIdValue.(uint64)
	if !ok {
		return 0, UserInfo{}, false
	}

	userInfoValue, ok := conn.Session().Load("user_info")
	if !ok {
		return 0, UserInfo{}, false
	}

	userInfo, ok := userInfoValue.(UserInfo)
	if !ok {
		return 0, UserInfo{}, false
	}

	return clientId, userInfo, true
}

func clientFromConn(conn *gws.Conn) (*Client, bool) {
	value, ok := conn.Session().Load("client")
	if !ok {
		return nil, false
	}

	client, ok := value.(*Client)
	return client, ok
}
