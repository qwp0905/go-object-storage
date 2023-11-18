package namenode

import (
	"context"
	"io"
	"time"

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

func (n *NameNode) put(
	ctx context.Context,
	id string,
	key, contentType string,
	metadata *datanode.Metadata,
	size int,
	r io.Reader,
) error {
	if key == metadata.Key {
		metadata.Size = uint(size)
		metadata.LastModified = time.Now()
		metadata.Type = contentType
		if !metadata.FileExists() {
			nodeId, err := n.pool.AcquireNode(ctx)
			if err != nil {
				return err
			}
			metadata.NodeId = nodeId
			metadata.Source = generateKey()
		}

		if _, err := n.pool.PutDirect(ctx, metadata, r); err != nil {
			return err
		}

		return n.pool.PutMetadata(ctx, id, metadata)
	}

	for i, next := range metadata.NextNodes {
		matched := compare(next.Key, key)
		if len(matched) <= len(metadata.Key) {
			continue
		}

		nextMeta, err := n.pool.GetMetadata(ctx, next.NodeId, next.Key)
		if err != nil {
			return err
		}

		if matched == nextMeta.Key {
			return n.put(ctx, next.NodeId, key, contentType, nextMeta, size, r)
		}

		nodeId, err := n.pool.AcquireNode(ctx)
		if err != nil {
			return err
		}

		newMeta := &datanode.Metadata{Key: matched, NextNodes: []*datanode.NextRoute{next}}
		if err := n.pool.PutMetadata(ctx, nodeId, newMeta); err != nil {
			return err
		}

		metadata.NextNodes[i] = &datanode.NextRoute{NodeId: nodeId, Key: matched}
		if err := n.pool.PutMetadata(ctx, id, metadata); err != nil {
			return err
		}

		return n.put(ctx, nodeId, key, contentType, newMeta, size, r)
	}

	dataId, err := n.pool.AcquireNode(ctx)
	if err != nil {
		return err
	}
	newMeta := &datanode.Metadata{
		NodeId:       dataId,
		Key:          key,
		Source:       generateKey(),
		LastModified: time.Now(),
		Type:         contentType,
		Size:         uint(size),
		NextNodes:    make([]*datanode.NextRoute, 0),
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

	metadata.NextNodes = append(metadata.NextNodes, &datanode.NextRoute{
		Key:    key,
		NodeId: metadataId,
	})
	return n.pool.PutMetadata(ctx, id, metadata)
}
