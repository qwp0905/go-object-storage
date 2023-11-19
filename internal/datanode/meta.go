package datanode

import (
	"bytes"

	"time"

	"github.com/goccy/go-json"
	"github.com/pkg/errors"
)

type Metadata struct {
	Key          string       `json:"key"`
	Source       string       `json:"source,omitempty"`
	Size         uint         `json:"size,omitempty"`
	Type         string       `json:"type,omitempty"`
	NodeId       string       `json:"node_id,omitempty"`
	LastModified time.Time    `json:"last_modified,omitempty"`
	NextNodes    []*NextRoute `json:"next_nodes"`
}

type NextRoute struct {
	NodeId string `json:"node_id"`
	Key    string `json:"key"`
}

func (m *Metadata) FileExists() bool {
	return m.Source != "" && m.NodeId != ""
}

func (d *dataNodeImpl) GetMetadata(key string) (*Metadata, error) {
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

func (d *dataNodeImpl) PutMetadata(metadata *Metadata) error {
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
