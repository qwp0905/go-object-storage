package namenode

import (
	"context"

	"github.com/qwp0905/go-object-storage/internal/metadata"
)

func (n *nameNodeImpl) delete(ctx context.Context, key, id, current string) (*metadata.Metadata, error) {
	locker := n.lockerPool.Get(current)
	if err := locker.Lock(ctx); err != nil {
		return nil, err
	}
	defer locker.Unlock(ctx)

	currentMeta, err := n.pool.GetMetadata(ctx, id, current)
	if err != nil {
		return nil, err
	}

	if key == currentMeta.Key && currentMeta.FileExists() {
		if len(currentMeta.NextNodes) == 0 {
			if err := n.pool.DeleteMetadata(ctx, id, key); err != nil {
				return nil, err
			}
			return nil, nil
		}

		currentMeta.Clear()
		if err := n.pool.PutMetadata(ctx, id, currentMeta); err != nil {
			return nil, err
		}

		return currentMeta, nil
	}

	index := currentMeta.FindPrefix(key)
	if index == -1 {
		return currentMeta, nil
	}

	next := currentMeta.GetNext(index)
	deleted, err := n.delete(ctx, key, next.NodeId, next.Key)
	if err != nil {
		return nil, err
	}

	if deleted != nil {
		if deleted.Len() != 1 || deleted.FileExists() {
			return currentMeta, nil
		}

		currentMeta.NextNodes[index] = deleted.NextNodes[0]
		if err := n.pool.PutMetadata(ctx, id, currentMeta); err != nil {
			return nil, err
		}
		if err := n.pool.DeleteMetadata(ctx, next.NodeId, next.Key); err != nil {
			return nil, err
		}

		return currentMeta, nil
	}

	if currentMeta.Len() == 1 &&
		!currentMeta.FileExists() &&
		currentMeta.Key != n.pool.GetRootKey() {
		if err := n.pool.DeleteMetadata(ctx, id, currentMeta.Key); err != nil {
			return nil, err
		}

		return nil, nil
	}

	currentMeta.RemoveNext(index)
	if err := n.pool.PutMetadata(ctx, id, currentMeta); err != nil {
		return nil, err
	}

	return currentMeta, nil
}
