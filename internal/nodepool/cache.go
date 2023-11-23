package nodepool

import (
	"sync"

	"github.com/qwp0905/go-object-storage/pkg/list"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

type Cache interface {
	Get(key string) string
	Set(key, value string)
	Del(key string)
}

type cacheImpl struct {
	noCopy   nocopy.NoCopy
	mu       *sync.Mutex
	accessed *list.DoubleLinked[string]
	maxSize  int
	table    map[string]*cacheItem
}

type cacheItem struct {
	lastAccess *list.DoubleLinkedElement[string]
	value      string
}

func newCacheItem(key, value string) *cacheItem {
	return &cacheItem{
		lastAccess: list.NewDoubleLinkedElement[string](key),
		value:      value,
	}
}

func NewCache(size int) Cache {
	return &cacheImpl{
		mu:       new(sync.Mutex),
		accessed: list.NewDoubleLinked[string](),
		maxSize:  size,
		table:    make(map[string]*cacheItem),
	}
}

func (c *cacheImpl) Get(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.table[key]
	if !ok {
		return ""
	}

	c.accessed.MoveBack(item.lastAccess)
	return item.value
}

func (c *cacheImpl) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if item, ok := c.table[key]; ok {
		c.accessed.Remove(item.lastAccess)
		delete(c.table, key)
	}

	for len(c.table) >= c.maxSize {
		l := c.accessed.First()
		c.accessed.Remove(l)
		delete(c.table, l.Value)
	}

	item := newCacheItem(key, value)
	c.accessed.PushBack(item.lastAccess)
	c.table[key] = item
}

func (c *cacheImpl) Del(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.table[key]; !ok {
		return
	}
	c.accessed.Remove(c.table[key].lastAccess)
	delete(c.table, key)
}
