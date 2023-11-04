package repository

import (
	"errors"
	"strings"
	"sync"
)

const separator = "/"

type Metadata struct {
	source   string
	fullPath string
}

type node struct {
	metadata *Metadata
	children map[string]*node
}

func newNode(meta *Metadata) *node {
	return &node{
		metadata: meta,
		children: make(map[string]*node),
	}
}

func (n *node) isLeaf() bool {
	return len(n.children) == 0
}

func (n *node) add(keys []string, now int, source string) {
	if len(keys) == now {
		n.metadata.source = source
		return
	}

	if _, ok := n.children[keys[now]]; !ok {
		n.children[keys[now]] = newNode(&Metadata{
			fullPath: separator + strings.Join(keys[0:now+1], separator),
		})
	}

	n.children[keys[now]].add(keys, now+1, source)
}

func (n *node) search(keys []string) []*Metadata {
	if n.isLeaf() {
		return []*Metadata{n.metadata}
	}

	if len(keys) == 1 && keys[0] == "" {
		out := []*Metadata{}
		for _, c := range n.children {
			out = append(out, c.metadata)
		}
		return out
	}

	for key, c := range n.children {
		if key == keys[0] {
			return c.search(keys[1:])
		}
	}
	return []*Metadata{}
}

type Cache struct {
	nodes  *node
	locker *sync.Mutex
}

func NewCache() *Cache {
	return &Cache{
		nodes: newNode(&Metadata{
			fullPath: separator,
		}),
		locker: new(sync.Mutex),
	}
}

func (c *Cache) Set(key string, id string) error {
	if key[len(key)-1:] == separator {
		return errors.New("cannot add directory")
	}
	if key[0:1] == separator {
		key = key[1:]
	}
	c.nodes.add(strings.Split(key, separator), 0, id)
	return nil
}

func (c *Cache) Get(key string) []*Metadata {
	if key[0:1] == separator {
		key = key[1:]
	}

	return c.nodes.search(strings.Split(key, separator))
}
