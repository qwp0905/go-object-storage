package namenode

import (
	"context"
	"io"

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

	return n.put(ctx, n.pool.GetRootId(), key, root, size, r)
}

func (n *NameNode) DeleteObject(ctx context.Context, key string) error {
	root, err := n.pool.GetRootMetadata(ctx)
	if err != nil {
		return err
	}

	metadata, err := n.HeadObject(ctx, key)
	if err != nil {
		if err == fiber.ErrNotFound {
			return nil
		}
		return err
	}

	if _, err := n.delete(ctx, n.pool.GetRootId(), key, root); err != nil {
		return err
	}

	return n.pool.DeleteDirect(ctx, metadata)
}

func generateKey() string {
	id, _ := uuid.NewRandom()
	return id.String()
}
