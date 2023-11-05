package repository

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

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

const separator = "/"

type Metadata struct {
	Pointer   string
	CreatedAt time.Time
	Key       string
	IsDir     bool
}

type metadata struct {
	pointer   string
	createdAt time.Time
}

type cacheNode struct {
	metadata *metadata
	label    string
	children []*cacheNode
}

func newNode(label string, meta *metadata) *cacheNode {
	return &cacheNode{
		metadata: meta,
		label:    label,
		children: []*cacheNode{},
	}
}

func (n *cacheNode) reorder(match string) {
	if r := strings.TrimPrefix(n.label, match); r != "" {
		n.label = match
		nc := newNode(r, n.metadata)
		nc.children = n.children
		n.children = []*cacheNode{nc}
	}
}

func (n *cacheNode) add(key string, meta *metadata) {
	if key == "" || !strings.HasPrefix(key, n.label) {
		return
	}

	p := strings.TrimPrefix(key, n.label)
	for _, c := range n.children {
		if mat := compare(c.label, p); mat != "" {
			c.reorder(mat)
			c.add(p, meta)
			return
		}
	}

	n.children = append(n.children, newNode(p, meta))
}

func (n *cacheNode) isLeaf() bool {
	return len(n.children) == 0
}

func (n *cacheNode) get(key string) []*Metadata {
	if key == "" {
		return []*Metadata{}
	}
	if n.label == key {
		return []*Metadata{
			{Key: n.label, Pointer: n.metadata.pointer, CreatedAt: n.metadata.createdAt},
		}
	}
	if strings.HasPrefix(n.label, key) {
		out := []*Metadata{}
		for _, c := range n.children {
			out = append(out, &Metadata{
				Pointer:   c.metadata.pointer,
				CreatedAt: c.metadata.createdAt,
				Key:       strings.TrimPrefix(n.label, key) + c.label,
			})
		}
		return out
	}

	mat, ok := strings.CutPrefix(key, n.label)
	if !ok {
		return []*Metadata{}
	}

	out := []*Metadata{}
	for _, c := range n.children {
		r := c.get(mat)
		if len(r) == 0 {
			continue
		}
		out = append(out, r...)
	}

	return out
}

func (n *cacheNode) Print() {
	fmt.Println(n.label, len(n.children), n.metadata)
	for _, c := range n.children {
		c.Print()
	}
}

type Cache struct {
	nodes  *cacheNode
	locker *sync.Mutex
}

func NewCache() *Cache {
	return &Cache{
		nodes:  newNode(separator, &metadata{}),
		locker: new(sync.Mutex),
	}
}

func (c *Cache) Insert(key string, meta *Metadata) {
	if key[0:1] != separator {
		key = separator + key
	}
	c.locker.Lock()
	defer c.locker.Unlock()
	c.nodes.add(key, &metadata{pointer: meta.Pointer, createdAt: meta.CreatedAt})
}

func (c *Cache) Find(key string) []*Metadata {
	if key[0:1] != separator {
		key = separator + key
	}
	c.waitLock()
	r := c.nodes.get(key)
	out := []*Metadata{}
	if len(r) == 1 {
		r[0].Key = key
		out = append(out, r[0])
	} else {
		for _, m := range r {
			m.Key = key + m.Key
			out = append(out, m)
		}
	}

	return out
}

func (c *Cache) waitLock() {
	c.locker.Lock()
	defer c.locker.Unlock()
}
