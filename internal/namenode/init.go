package namenode

import (
	"context"

	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/metadata"
)

func (n *nameNodeImpl) getRootId(ctx context.Context) (string, error) {
	if n.rootId != "" {
		return n.rootId, nil
	}
	locker := n.lockerPool.Get(n.rootKey)
	if err := locker.Lock(ctx); err != nil {
		return "", err
	}
	defer locker.Unlock(ctx)

	id, err := n.findRoot(ctx)
	if err == nil {
		n.rootId = id
		return id, nil
	}

	rootId, err := n.pool.AcquireNode(ctx)
	if err != nil {
		return "", err
	}
	root := metadata.New(n.rootKey)
	if err := n.pool.PutMetadata(ctx, rootId, root); err != nil {
		return "", err
	}

	n.rootId = rootId
	return n.rootId, nil
}

func (n *nameNodeImpl) findRoot(ctx context.Context) (string, error) {
	ids, err := n.pool.GetNodeIds(ctx)
	if err != nil {
		return "", err
	}

	for _, id := range ids {
		if _, err := n.pool.GetMetadata(ctx, id, n.rootKey); err != nil {
			continue
		}
		return id, nil
	}

	return "", fiber.ErrNotFound
}
