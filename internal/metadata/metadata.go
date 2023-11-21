package metadata

import (
	"strings"
	"time"

	"github.com/google/uuid"
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

func New(key string) *Metadata {
	return &Metadata{Key: key, NextNodes: make([]*NextRoute, 0)}
}

type NextRoute struct {
	NodeId string `json:"node_id"`
	Key    string `json:"key"`
}

func (m *Metadata) FileExists() bool {
	return m.Source != "" && m.NodeId != ""
}

func (m *Metadata) FindPrefix(key string) int {
	for i := range m.NextNodes {
		if strings.HasPrefix(key, m.NextNodes[i].Key) {
			return i
		}
	}
	return -1
}

func (m *Metadata) FindMatched(key string) (int, string) {
	for i := range m.NextNodes {
		if matched := compare(m.NextNodes[i].Key, key); len(matched) > len(m.Key) {
			return i, matched
		}
	}

	return -1, ""
}

func (m *Metadata) GetNext(index int) *NextRoute {
	return m.NextNodes[index]
}

func (m *Metadata) UpdateAttr(size int, contentType string) {
	m.Size = uint(size)
	m.Type = contentType
	m.LastModified = time.Now()
}

func (m *Metadata) SetNew(nodeId string) {
	m.Source = uuid.Must(uuid.NewRandom()).String()
	m.NodeId = nodeId
}

func (m *Metadata) Clear() {
	*m = Metadata{Key: m.Key, NextNodes: m.NextNodes}
}

func (m *Metadata) InsertNext(id, key string) {
	index := 0
	for i := range m.NextNodes {
		if m.NextNodes[i].Key > key {
			break
		}
		index++
	}
	m.NextNodes = append(m.NextNodes, &NextRoute{})
	copy(m.NextNodes[index+1:], m.NextNodes[index:])
	m.NextNodes[index] = &NextRoute{NodeId: id, Key: key}
}

func (m *Metadata) RemoveNext(index int) {
	m.NextNodes = append(m.NextNodes[:index], m.NextNodes[index+1:]...)
}

func (m *Metadata) Len() int {
	return len(m.NextNodes)
}

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
