package namenode

import (
	"context"
	"strings"

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

		updated := &metadata.Metadata{
			Key:       currentMeta.Key,
			NextNodes: currentMeta.NextNodes,
		}
		if err := n.pool.PutMetadata(ctx, id, updated); err != nil {
			return nil, err
		}

		return updated, nil
	}

	for i, next := range currentMeta.NextNodes {
		if !strings.HasPrefix(key, next.Key) {
			continue
		}

		deleted, err := n.delete(ctx, key, next.NodeId, next.Key)
		if err != nil {
			return nil, err
		}

		if deleted == nil {
			if len(currentMeta.NextNodes) == 1 &&
				!currentMeta.FileExists() &&
				currentMeta.Key != n.pool.GetRootKey() {
				if err := n.pool.DeleteMetadata(ctx, id, currentMeta.Key); err != nil {
					return nil, err
				}

				return nil, nil
			}

			currentMeta.NextNodes = append(currentMeta.NextNodes[:i], currentMeta.NextNodes[i+1:]...)
			if err := n.pool.PutMetadata(ctx, id, currentMeta); err != nil {
				return nil, err
			}

			return currentMeta, nil
		}

		if len(deleted.NextNodes) == 1 && !deleted.FileExists() {
			currentMeta.NextNodes[i] = deleted.NextNodes[0]
			if err := n.pool.PutMetadata(ctx, id, currentMeta); err != nil {
				return nil, err
			}
			if err := n.pool.DeleteMetadata(ctx, next.NodeId, next.Key); err != nil {
				return nil, err
			}
			return currentMeta, nil
		}

		return currentMeta, nil
	}

	return currentMeta, nil
}
