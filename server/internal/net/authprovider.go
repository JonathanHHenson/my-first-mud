package net

type AuthProvider interface {
	Auth(token string) (uint64, UserInfo, error)
}
