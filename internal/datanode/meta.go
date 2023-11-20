package datanode

import (
	"bytes"

	"github.com/goccy/go-json"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/metadata"
)

func (d *dataNodeImpl) GetMetadata(key string) (*metadata.Metadata, error) {
	r, err := d.bp.Get(d.getMetaKey(key))
	if err != nil {
		return nil, err
	}
	metadata := new(metadata.Metadata)
	if err := json.NewDecoder(r).Decode(metadata); err != nil {
		return nil, errors.WithStack(err)
	}

	return metadata, nil
}

func (d *dataNodeImpl) PutMetadata(metadata *metadata.Metadata) error {
	b, err := json.Marshal(metadata)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := d.bp.Put(d.getMetaKey(metadata.Key), len(b), bytes.NewReader(b)); err != nil {
		return err
	}

	return nil
}

func (d *dataNodeImpl) DeleteMetadata(key string) error {
	return d.bp.Delete(d.getMetaKey(key))
}
