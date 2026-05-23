package auth

import (
	"sync/atomic"

	"github.com/JonathanHHenson/my-first-mud/server/internal/net"
)

type DummyAuth struct {
	nextId atomic.Uint64
}

func (d *DummyAuth) Auth(token string) (uint64, net.UserInfo, error) {
	id := d.nextId.Add(1)
	userInfo := net.UserInfo{Username: token}
	return id, userInfo, nil
}
