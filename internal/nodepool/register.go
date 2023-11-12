package nodepool

import (
	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/pkg/logger"
)

func (p *NodePool) Register(id string, host string) error {
	_, err := p.GetMetadata(host, p.rootKey)
	if err == nil {
		if p.root != nil && p.root.Id != id {
			return errors.New("root node already registered")
		}
		p.root = &NodeInfo{Host: host, Id: id}
		logger.Infof("datanode id %s host %s registered as root", id, host)
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
