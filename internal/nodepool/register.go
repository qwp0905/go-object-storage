package nodepool

import (
	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/pkg/logger"
)

func (p *NodePool) Register(id string, host string) error {
	header, err := p.headMetadata(host, p.rootKey)
	if err == nil && header.StatusCode() == fiber.StatusOK {
		if p.root != nil {
			return errors.New("root node already registered")
		}
		p.root = &NodeInfo{Host: host, Id: id}
	} else if err != nil && err != fiber.ErrNotFound {
		return errors.WithStack(err)
	}

	p.locker.Lock()
	defer p.locker.Unlock()
	p.nodeInfo[id] = &NodeInfo{
		Id:   id,
		Host: host,
	}
	logger.Infof("datanode id %s host %s registered", id, host)
	return nil
}
