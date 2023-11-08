package nodepool

import (
	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
)

func (p *NodePool) Register(id string, host string) error {
	header, err := p.headMetadata(host, p.rootKey)
	if err == nil && header.StatusCode() == fiber.StatusOK {
		if p.root != nil {
			return errors.New("root node already registered")
		}
		p.root = &NodeInfo{Host: host, Id: id}
	} else if err != nil && err != fiber.ErrNotFound && header.StatusCode() != fiber.StatusNotFound {
		return err
	}

	p.locker.Lock()
	defer p.locker.Unlock()
	p.nodeInfo[id] = &NodeInfo{
		Id:   id,
		Host: host,
	}
	return nil
}
