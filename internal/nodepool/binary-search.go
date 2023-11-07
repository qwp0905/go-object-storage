package nodepool

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func (p *NodePool) search(key string, metadata *datanode.Metadata) (*datanode.Metadata, error) {
	if key == metadata.Key {
		return metadata, nil
	}

	for _, next := range metadata.NextNodes {
		if strings.HasPrefix(next.Key, key) {
			nextMeta, err := p.getMetadata(p.getNode(next.NodeId).host, key)
			if err != nil {
				return nil, err
			}
			return p.search(key, nextMeta)
		}
	}
	return nil, fiber.ErrNotFound
}
