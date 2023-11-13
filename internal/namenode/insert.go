package namenode

import (
	"context"

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

func (n *NameNode) reorderMetadata(ctx context.Context, id string, current *datanode.Metadata, saved *datanode.NextRoute) error {
	for _, next := range current.NextNodes {
		matched := compare(next.Key, saved.Key)
		if len(matched) <= len(current.Key) {
			continue
		}

		nextMeta, err := n.pool.GetMetadata(ctx, next.NodeId, next.Key)
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

			newId, err := n.pool.AcquireNode(ctx)
			if err != nil {
				return err
			}
			if err := n.pool.PutMetadata(ctx, newId, newChild); err != nil {
				return err
			}
			nextMeta.NextNodes = []*datanode.NextRoute{{
				NodeId: newId,
				Key:    matched,
			}}
			if err := n.pool.PutMetadata(ctx, next.NodeId, nextMeta); err != nil {
				return err
			}
		}
		// TODO 이부분 로직 수정 필요 문제 있음
		return n.reorderMetadata(ctx, next.NodeId, nextMeta, saved)
	}

	current.NextNodes = append(current.NextNodes, saved)
	return n.pool.PutMetadata(ctx, id, current)
}
