package datanode

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
)

type Metadata struct {
	Key       string       `json:"key"`
	Source    string       `json:"source"`
	Size      uint         `json:"size"`
	NodeId    string       `json:"node_id"`
	NextNodes []*NextRoute `json:"next_nodes"`
}

type NextRoute struct {
	NodeId string `json:"node_id"`
	Key    string `json:"key"`
}

func (m *Metadata) FileExists() bool {
	return m.Source != ""
}

func (d *DataNode) GetMetadata(ctx context.Context, key string) (io.Reader, error) {
	return d.fs.ReadFile(ctx, d.getMetaKey(key))
}

func (d *DataNode) PutMetadata(key string, metadata *Metadata) error {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(metadata); err != nil {
		return err
	}

	_, err := d.fs.WriteFile(d.getMetaKey(key), buf)
	if err != nil {
		return err
	}

	return nil
}

func (d *DataNode) DeleteMetadata(key string) error {
	return d.fs.RemoveFile(d.getMetaKey(key))
}
