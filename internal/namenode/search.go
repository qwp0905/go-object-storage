package namenode

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func (n *NameNode) get(id, key string, metadata *datanode.Metadata) (string, *datanode.Metadata, error) {
	if key == metadata.Key && metadata.FileExists() {
		return id, metadata, nil
	}
	for _, next := range metadata.NextNodes {
		if !strings.HasPrefix(key, next.Key) {
			continue
		}

		nextMeta, err := n.pool.GetMetadata(next.NodeId, key)
		if err != nil {
			return "", nil, err
		}
		return n.get(next.NodeId, key, nextMeta)
	}

	return "", nil, fiber.ErrNotFound
}

func (n *NameNode) scan(key string, limit int, metadata *datanode.Metadata) ([]*datanode.Metadata, error) {
	out := []*datanode.Metadata{}
	if metadata.FileExists() {
		out = append(out, metadata)
	}

	for _, next := range metadata.NextNodes {
		if !strings.HasPrefix(key, next.Key) {
			continue
		}

		nextMeta, err := n.pool.GetMetadata(next.NodeId, next.Key)
		if err != nil {
			return nil, err
		}

		r, err := n.scan(key, limit, nextMeta)
		if err != nil {
			return nil, err
		}

		for _, v := range r {
			if len(out) == limit {
				return out, nil
			}
			out = append(out, v)
		}
	}

	return out, nil
}
