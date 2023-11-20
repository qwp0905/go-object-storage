package namenode

import (
	"context"
	"io"
	"time"

	"github.com/qwp0905/go-object-storage/internal/metadata"
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
	defer locker.Unlock(ctx)

	currentMeta, err := n.pool.GetMetadata(ctx, id, current)
	if err != nil {
		return err
	}

	if key == currentMeta.Key {
		currentMeta.Size = uint(size)
		currentMeta.LastModified = time.Now()
		currentMeta.Type = contentType
		if !currentMeta.FileExists() {
			nodeId, err := n.pool.AcquireNode(ctx)
			if err != nil {
				return err
			}
			currentMeta.NodeId = nodeId
			currentMeta.Source = generateKey()
		}

		if _, err := n.pool.PutDirect(ctx, currentMeta, r); err != nil {
			return err
		}

		return n.pool.PutMetadata(ctx, id, currentMeta)
	}

	for i, next := range currentMeta.NextNodes {
		matched := compare(next.Key, key)
		if len(matched) <= len(currentMeta.Key) {
			continue
		}
		if matched == next.Key {
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

		currentMeta.NextNodes[i] = &metadata.NextRoute{NodeId: nodeId, Key: matched}
		if err := n.pool.PutMetadata(ctx, id, currentMeta); err != nil {
			return err
		}

		return n.put(ctx, key, nodeId, matched, contentType, size, r)
	}

	dataId, err := n.pool.AcquireNode(ctx)
	if err != nil {
		return err
	}
	newMeta := &metadata.Metadata{
		NodeId:       dataId,
		Key:          key,
		Source:       generateKey(),
		LastModified: time.Now(),
		Type:         contentType,
		Size:         uint(size),
		NextNodes:    make([]*metadata.NextRoute, 0),
	}
	if _, err := n.pool.PutDirect(ctx, newMeta, r); err != nil {
		return err
	}

	metadataId, err := n.pool.AcquireNode(ctx)
	if err != nil {
		return err
	}
	if err := n.pool.PutMetadata(ctx, metadataId, newMeta); err != nil {
		return err
	}

	index := 0
	for i := range currentMeta.NextNodes {
		if currentMeta.NextNodes[i].Key > key {
			break
		}
		index++
	}
	currentMeta.NextNodes = append(currentMeta.NextNodes, &metadata.NextRoute{})
	copy(currentMeta.NextNodes[index+1:], currentMeta.NextNodes[index:])
	currentMeta.NextNodes[index] = &metadata.NextRoute{Key: key, NodeId: metadataId}

	return n.pool.PutMetadata(ctx, id, currentMeta)
}
