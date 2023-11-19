package namenode

import (
	"context"
	"strings"

	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func (n *NameNode) delete(ctx context.Context, key, id, current string) (*datanode.Metadata, error) {
	locker := n.lockerPool.Get(current)
	if err := locker.Lock(ctx); err != nil {
		return nil, err
	}
	defer locker.Unlock(ctx)

	metadata, err := n.pool.GetMetadata(ctx, id, current)
	if err != nil {
		return nil, err
	}

	if key == metadata.Key && metadata.FileExists() {
		if len(metadata.NextNodes) == 0 {
			if err := n.pool.DeleteMetadata(ctx, id, key); err != nil {
				return nil, err
			}
			return nil, nil
		}

		updated := &datanode.Metadata{
			Key:       metadata.Key,
			NextNodes: metadata.NextNodes,
		}
		if err := n.pool.PutMetadata(ctx, id, updated); err != nil {
			return nil, err
		}

		return updated, nil
	}

	for i, next := range metadata.NextNodes {
		if !strings.HasPrefix(key, next.Key) {
			continue
		}

		deleted, err := n.delete(ctx, key, next.NodeId, next.Key)
		if err != nil {
			return nil, err
		}

		if deleted == nil {
			if len(metadata.NextNodes) == 1 &&
				!metadata.FileExists() &&
				metadata.Key != n.pool.GetRootKey() {
				if err := n.pool.DeleteMetadata(ctx, id, metadata.Key); err != nil {
					return nil, err
				}

				return nil, nil
			}

			metadata.NextNodes = append(metadata.NextNodes[:i], metadata.NextNodes[i+1:]...)
			if err := n.pool.PutMetadata(ctx, id, metadata); err != nil {
				return nil, err
			}

			return metadata, nil
		}

		if len(deleted.NextNodes) == 1 && !deleted.FileExists() {
			metadata.NextNodes[i] = deleted.NextNodes[0]
			if err := n.pool.PutMetadata(ctx, id, metadata); err != nil {
				return nil, err
			}
			if err := n.pool.DeleteMetadata(ctx, next.NodeId, next.Key); err != nil {
				return nil, err
			}
			return metadata, nil
		}

		return metadata, nil
	}

	return metadata, nil
}
