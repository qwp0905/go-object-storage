package datanode

import (
	"context"
	"io"
)

func (d *DataNode) GetObject(ctx context.Context, key string) (io.Reader, error) {
	return d.fs.ReadFile(ctx, d.getDataKey(key))
}

func (d *DataNode) PutObject(r io.Reader) (*Metadata, error) {
	source := generateKey()
	size, err := d.fs.WriteFile(d.getDataKey(source), r)
	if err != nil {
		return nil, err
	}

	return &Metadata{
		Source:    source,
		Size:      size,
		NodeId:    d.id,
		NextNodes: []*NextRoute{},
	}, nil
}
