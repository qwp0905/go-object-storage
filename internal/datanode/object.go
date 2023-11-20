package datanode

import (
	"context"
	"io"
)

func (d *dataNodeImpl) GetObject(ctx context.Context, key string) (io.Reader, error) {
	return d.bp.Get(d.getDataKey(key))
}

func (d *dataNodeImpl) PutObject(key string, size int, r io.Reader) error {
	return d.bp.Put(d.getDataKey(key), size, r)
}

func (d *dataNodeImpl) DeleteObject(key string) error {
	return d.bp.Delete(key)
}
