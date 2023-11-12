package nodepool

import (
	"context"
	"io"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func (p *NodePool) search(id, key string, metadata *datanode.Metadata) (string, *datanode.Metadata, error) {
	if key == metadata.Key && metadata.FileExists() {
		return id, metadata, nil
	}

	for _, next := range metadata.NextNodes {
		if !strings.HasPrefix(next.Key, key) {
			continue
		}

		nextMeta, err := p.getMetadata(p.getNodeHost(next.NodeId), key)
		if err != nil {
			return "", nil, err
		}
		return p.search(next.NodeId, key, nextMeta)
	}

	return "", nil, fiber.ErrNotFound
}

func (p *NodePool) listSearch(key string, limit int, metadata *datanode.Metadata) ([]*datanode.Metadata, error) {
	out := []*datanode.Metadata{}
	if metadata.FileExists() {
		out = append(out, metadata)
	}

	for _, next := range metadata.NextNodes {
		if !strings.HasPrefix(next.Key, key) {
			continue
		}

		nextMeta, err := p.getMetadata(p.getNodeHost(next.NodeId), next.Key)
		if err != nil {
			return nil, err
		}
		r, err := p.listSearch(key, limit, nextMeta)
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

func (p *NodePool) GetMetadata(key string) (*datanode.Metadata, error) {
	if len(p.nodeInfo) == 0 {
		return nil, errors.New("no host registered...")
	}

	root, err := p.getRootMetadata()
	if err != nil {
		return nil, err
	}

	_, metadata, err := p.search(p.root.Id, key, root)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func (p *NodePool) GetObject(ctx context.Context, key string) (io.Reader, error) {
	metadata, err := p.GetMetadata(key)
	if err != nil {
		return nil, err
	}

	return p.getDirect(ctx, metadata)
}

func (p *NodePool) ListObject(prefix string, limit int) ([]*datanode.Metadata, error) {
	if len(p.nodeInfo) == 0 {
		return nil, errors.New("no host registered...")
	}

	root, err := p.getRootMetadata()
	if err != nil {
		return nil, err
	}

	return p.listSearch(prefix, limit, root)
}
