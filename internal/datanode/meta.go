package datanode

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

type Metadata struct {
	Key          string       `json:"key"`
	Source       string       `json:"source"`
	Size         uint         `json:"size"`
	NodeId       string       `json:"node_id"`
	LastModified time.Time    `json:"last_modified"`
	NextNodes    []*NextRoute `json:"next_nodes"`
}

type NextRoute struct {
	NodeId string `json:"node_id"`
	Key    string `json:"key"`
}

func (m *Metadata) FileExists() bool {
	return m.Source != ""
}

func (d *DataNode) GetMetadata(key string) (*Metadata, error) {
	r, err := d.bp.Get(d.getMetaKey(key))
	if err != nil {
		return nil, err
	}
	metadata := new(Metadata)
	if err := json.NewDecoder(r).Decode(metadata); err != nil {
		return nil, errors.WithStack(err)
	}

	return metadata, nil
}

func (d *DataNode) PutMetadata(metadata *Metadata) error {
	b, err := json.Marshal(metadata)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := d.bp.Put(d.getMetaKey(metadata.Key), len(b), bytes.NewReader(b)); err != nil {
		return err
	}

	return nil
}

func (d *DataNode) DeleteMetadata(key string) error {
	return d.bp.Delete(d.getMetaKey(key))
}
