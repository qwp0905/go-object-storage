package datanode

import (
	"context"
	"io"
)

func (d *DataNode) GetObject(ctx context.Context, key string) (io.Reader, error) {
	return d.bp.Get(d.getDataKey(key))
}

func (d *DataNode) PutObject(key string, size int, r io.Reader) (*Metadata, error) {
	if err := d.bp.Put(d.getDataKey(key), size, r); err != nil {
		return nil, err
	}

	return &Metadata{
		Source:    key,
		Size:      uint(size),
		NodeId:    d.config.Id,
		NextNodes: []*NextRoute{},
	}, nil
}

func (d *DataNode) DeleteObject(key string) error {
	return d.bp.Delete(key)
}
