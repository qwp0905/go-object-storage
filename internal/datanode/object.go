package datanode

import (
	"context"
	"io"
)

func (d *DataNode) GetObject(ctx context.Context, key string) (io.Reader, error) {
	return d.bp.Get(d.getDataKey(key))
}

func (d *DataNode) PutObject(size int, r io.Reader) (*Metadata, error) {
	source := generateKey()
	if err := d.bp.Put(d.getDataKey(source), size, r); err != nil {
		return nil, err
	}

	return &Metadata{
		Source:    source,
		Size:      uint(size),
		NodeId:    d.id,
		NextNodes: []*NextRoute{},
	}, nil
}

func (d *DataNode) DeleteObject(key string) error {
	return d.bp.Delete(key)
}
