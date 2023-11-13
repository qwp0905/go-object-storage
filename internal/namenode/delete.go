package namenode

import (
	"strings"

	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func (n *NameNode) delete(id, key string, metadata *datanode.Metadata) (*datanode.Metadata, error) {
	if key == metadata.Key && metadata.FileExists() {
		if len(metadata.NextNodes) == 0 {
			if err := n.pool.DeleteMetadata(id, key); err != nil {
				return nil, err
			}
			return nil, nil
		}

		updated := &datanode.Metadata{
			Key:       metadata.Key,
			NextNodes: metadata.NextNodes,
		}
		if err := n.pool.PutMetadata(id, updated); err != nil {
			return nil, err
		}

		return updated, nil
	}

	for i, next := range metadata.NextNodes {
		if !strings.HasPrefix(key, next.Key) {
			continue
		}

		nextMeta, err := n.pool.GetMetadata(next.NodeId, key)
		if err != nil {
			return nil, err
		}

		deleted, err := n.delete(next.NodeId, key, nextMeta)
		if err != nil {
			return nil, err
		}

		if deleted == nil {
			if len(metadata.NextNodes) == 1 && !metadata.FileExists() {
				if err := n.pool.DeleteMetadata(id, metadata.Key); err != nil {
					return nil, err
				}

				return nil, nil
			}

			metadata.NextNodes = append(metadata.NextNodes[:i], metadata.NextNodes[i+1:]...)
			if err := n.pool.PutMetadata(id, metadata); err != nil {
				return nil, err
			}

			return metadata, nil
		}

		if len(deleted.NextNodes) == 1 && !deleted.FileExists() {
			metadata.NextNodes[i] = deleted.NextNodes[0]
			if err := n.pool.PutMetadata(id, metadata); err != nil {
				return nil, err
			}
			return metadata, nil
		}

		return metadata, nil
	}

	return metadata, nil
}
