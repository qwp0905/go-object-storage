package nodepool

import (
	"io"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func compare(a, b string) string {
	min := len(b)
	if len(a) < len(b) {
		min = len(a)
	}

	out := ""
	for i := 0; i < min; i++ {
		if a[i:i+1] != b[i:i+1] {
			break
		}
		out += a[i : i+1]
	}
	return out
}

func (p *NodePool) reorderMetadata(id string, current *datanode.Metadata, saved *datanode.NextRoute) error {
	for _, next := range current.NextNodes {
		matched := compare(next.Key, saved.Key)
		if len(matched) <= len(saved.Key) {
			continue
		}

		nextMeta, err := p.getMetadata(p.getNodeHost(next.NodeId), next.Key)
		if err != nil {
			return err
		}
		if nextMeta.Key != saved.Key {
			newChild := &datanode.Metadata{
				Key:       matched,
				Source:    nextMeta.Source,
				Size:      nextMeta.Size,
				NodeId:    nextMeta.NodeId,
				NextNodes: nextMeta.NextNodes,
			}

			r := p.getNodeToSave()
			if err := p.putMetadata(r.Host, newChild); err != nil {
				return err
			}
			nextMeta.NextNodes = []*datanode.NextRoute{{
				NodeId: r.Id,
				Key:    matched,
			}}
			if err := p.putMetadata(p.getNodeHost(next.NodeId), nextMeta); err != nil {
				return err
			}
		}
		return p.reorderMetadata(next.NodeId, nextMeta, saved)
	}

	current.NextNodes = append(current.NextNodes, saved)
	return p.putMetadata(p.getNodeHost(id), current)
}

func (p *NodePool) PutObject(key string, size int, r io.Reader) error {
	if len(p.nodeInfo) == 0 {
		return errors.New("no host registered...")
	}

	root, err := p.getRootMetadata()
	if err != nil {
		return err
	}

	id, metadata, err := p.search(p.root.Id, key, root)
	if err != nil && err != fiber.ErrNotFound {
		return err
	} else if err == nil {
		metadata.Size = uint(size)
		metadata.LastModified = time.Now()

		if _, err := p.putDirect(metadata, r); err != nil {
			return err
		}

		return p.putMetadata(p.getNodeHost(id), metadata)
	} else {
		node := p.getNodeToSave()
		metadata = &datanode.Metadata{
			Key:          key,
			Source:       generateKey(),
			Size:         uint(size),
			NodeId:       node.Id,
			LastModified: time.Now(),
			NextNodes:    []*datanode.NextRoute{},
		}
		if _, err := p.putDirect(metadata, r); err != nil {
			return err
		}

		metadataNode := p.getNodeToSave()
		if err := p.putMetadata(p.getNodeHost(metadataNode.Id), metadata); err != nil {
			return err
		}
		return p.reorderMetadata(p.root.Id, root, &datanode.NextRoute{NodeId: metadataNode.Id, Key: key})
	}
}

func generateKey() string {
	id, _ := uuid.NewRandom()
	return id.String()
}
