package namenode

import (
	"context"
	"io"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
)

type NameNode struct {
	pool *nodepool.NodePool
}

func New(pool *nodepool.NodePool) *NameNode {
	return &NameNode{pool: pool}
}

func (n *NameNode) HeadObject(ctx context.Context, key string) (*datanode.Metadata, error) {
	root, err := n.pool.GetRootMetadata(ctx)
	if err != nil {
		return nil, err
	}

	_, metadata, err := n.get(ctx, n.pool.GetRootId(), key, root)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func (n *NameNode) GetObject(ctx context.Context, key string) (io.Reader, error) {
	metadata, err := n.HeadObject(ctx, key)
	if err != nil {
		return nil, err
	}

	return n.pool.GetDirect(ctx, metadata)
}

func (n *NameNode) ListObject(ctx context.Context, prefix string, limit int) ([]*datanode.Metadata, error) {
	root, err := n.pool.GetRootMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return n.scan(ctx, prefix, limit, root)
}

func (n *NameNode) PutObject(ctx context.Context, key string, size int, r io.Reader) error {
	root, err := n.pool.GetRootMetadata(ctx)
	if err != nil {
		return err
	}

	id, metadata, err := n.get(ctx, n.pool.GetRootId(), key, root)
	if err != nil && err != fiber.ErrNotFound {
		return err
	}

	if err == nil {
		metadata.Size = uint(size)
		metadata.LastModified = time.Now()

		if _, err := n.pool.PutDirect(ctx, metadata, r); err != nil {
			return err
		}

		return n.pool.PutMetadata(ctx, id, metadata)
	}

	nodeId, err := n.pool.AcquireNode(ctx)
	if err != nil {
		return err
	}
	newMeta := &datanode.Metadata{
		Key:          key,
		Source:       generateKey(),
		Size:         uint(size),
		NodeId:       nodeId,
		LastModified: time.Now(),
		NextNodes:    []*datanode.NextRoute{},
	}

	metaNode, err := n.pool.AcquireNode(ctx)
	if err != nil {
		return err
	}
	if _, err := n.pool.PutDirect(ctx, newMeta, r); err != nil {
		return err
	}
	if err := n.pool.PutMetadata(ctx, metaNode, newMeta); err != nil {
		return err
	}

	return n.reorderMetadata(ctx, n.pool.GetRootId(), root, &datanode.NextRoute{
		NodeId: metaNode,
		Key:    key,
	})
}

func (n *NameNode) DeleteObject(ctx context.Context, key string) error {
	root, err := n.pool.GetRootMetadata(ctx)
	if err != nil {
		return err
	}
	if _, err := n.delete(ctx, n.pool.GetRootId(), key, root); err != nil {
		return err
	}

	metadata, err := n.HeadObject(ctx, key)
	if err != nil {
		if err == fiber.ErrNotFound {
			return nil
		}
		return err
	}

	return n.pool.DeleteDirect(ctx, metadata)
}

func generateKey() string {
	id, _ := uuid.NewRandom()
	return id.String()
}
