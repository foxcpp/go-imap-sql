package children

import (
	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/server"
)

type Backend interface {
	EnableChildrenExt() bool
}

type extension struct{}

func (ext *extension) Capabilities(c server.Conn) []string {
	b, ok := c.Server().Backend.(Backend)
	if !ok {
		return nil
	}

	if !b.EnableChildrenExt() {
		return nil
	}

	if c.Context().State&imap.AuthenticatedState != 0 {
		return []string{Capability}
	}
	return nil
}

func (ext *extension) Command(name string) server.HandlerFactory {
	return nil
}

func NewExtension() server.Extension {
	return &extension{}
}
