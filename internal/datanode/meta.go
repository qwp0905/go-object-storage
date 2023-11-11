package datanode

import (
	"bytes"
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

func (d *DataNode) GetMetadata(key string) (io.Reader, error) {
	return d.bp.Get(d.getMetaKey(key))
}

func (d *DataNode) PutMetadata(key string, metadata *Metadata) error {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(metadata); err != nil {
		return err
	}

	if err := d.bp.Put(d.getMetaKey(key), buf.Len(), buf); err != nil {
		return err
	}

	return nil
}

func (d *DataNode) DeleteMetadata(key string) error {
	return d.bp.Delete(d.getMetaKey(key))
}
