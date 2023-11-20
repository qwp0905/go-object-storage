package namenode

import (
	"context"
	"io"

	"github.com/qwp0905/go-object-storage/internal/metadata"
)

func (n *nameNodeImpl) put(
	ctx context.Context,
	key, id, current, contentType string,
	size int,
	r io.Reader,
) error {
	locker := n.lockerPool.Get(current)
	if err := locker.Lock(ctx); err != nil {
		return err
	}

	currentMeta, err := n.pool.GetMetadata(ctx, id, current)
	if err != nil {
		defer locker.Unlock(ctx)
		return err
	}

	if key == currentMeta.Key {
		defer locker.Unlock(ctx)

		currentMeta.UpdateAttr(size, contentType)
		if !currentMeta.FileExists() {
			nodeId, err := n.pool.AcquireNode(ctx)
			if err != nil {
				return err
			}
			currentMeta.SetNew(nodeId)
		}

		if err := n.pool.PutDirect(ctx, currentMeta, r); err != nil {
			return err
		}

		return n.pool.PutMetadata(ctx, id, currentMeta)
	}

	index, matched := currentMeta.FindMatched(key)
	if index == -1 {
		defer locker.Unlock(ctx)
		dataId, err := n.pool.AcquireNode(ctx)
		if err != nil {
			return err
		}

		newMeta := metadata.New(key)
		newMeta.SetNew(dataId)
		newMeta.UpdateAttr(size, contentType)
		if err := n.pool.PutDirect(ctx, newMeta, r); err != nil {
			return err
		}

		metadataId, err := n.pool.AcquireNode(ctx)
		if err != nil {
			return err
		}
		if err := n.pool.PutMetadata(ctx, metadataId, newMeta); err != nil {
			return err
		}

		currentMeta.InsertNext(metadataId, key)
		return n.pool.PutMetadata(ctx, id, currentMeta)
	}

	next := currentMeta.GetNext(index)
	if next.Key == matched {
		if err := locker.Unlock(ctx); err != nil {
			return err
		}
		return n.put(ctx, key, next.NodeId, next.Key, contentType, size, r)
	}

	nodeId, err := n.pool.AcquireNode(ctx)
	if err != nil {
		return err
	}

	newMeta := &metadata.Metadata{Key: matched, NextNodes: []*metadata.NextRoute{next}}
	if err := n.pool.PutMetadata(ctx, nodeId, newMeta); err != nil {
		return err
	}

	currentMeta.NextNodes[index] = &metadata.NextRoute{NodeId: nodeId, Key: matched}
	if err := n.pool.PutMetadata(ctx, id, currentMeta); err != nil {
		return err
	}

	if err := locker.Unlock(ctx); err != nil {
		return err
	}

	return n.put(ctx, key, nodeId, matched, contentType, size, r)
}
