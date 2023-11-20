package metadata

import (
	"strings"
	"time"
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

func (m *Metadata) FindKey(key string) *NextRoute {
	for _, next := range m.NextNodes {
		if strings.HasPrefix(key, next.Key) {
			return next
		}
	}
	return nil
}

func (m *Metadata) FindMatched(key string) (string, *NextRoute) {
	for _, next := range m.NextNodes {
		matched := compare(next.Key, key)
		if len(matched) > len(m.Key) {
			return matched, next
		}
	}

	return "", nil
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
