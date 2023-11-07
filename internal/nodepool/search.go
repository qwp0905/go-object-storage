package nodepool

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func (p *NodePool) search(key string, metadata *datanode.Metadata) (*datanode.Metadata, error) {
	if key == metadata.Key && metadata.FileExists() {
		return metadata, nil
	}

	for _, next := range metadata.NextNodes {
		if !strings.HasPrefix(next.Key, key) {
			continue
		}

		nextMeta, err := p.getMetadata(p.getNode(next.NodeId).Host, key)
		if err != nil {
			return nil, err
		}
		return p.search(key, nextMeta)
	}

	return nil, fiber.ErrNotFound
}

func (p *NodePool) listSearch(key string, metadata *datanode.Metadata) ([]*datanode.Metadata, error) {
	if key == metadata.Key && metadata.FileExists() {
		return []*datanode.Metadata{metadata}, nil
	}

	out := []*datanode.Metadata{}
	for _, next := range metadata.NextNodes {
		if !strings.HasPrefix(next.Key, key) {
			continue
		}

		nextMeta, err := p.getMetadata(p.getNode(next.NodeId).Host, key)
		if err != nil {
			return nil, err
		}
		r, err := p.listSearch(key, nextMeta)
		if err != nil {
			return nil, err
		}
		out = append(out, r...)
	}

	return out, nil
}
