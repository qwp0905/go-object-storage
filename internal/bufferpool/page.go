package bufferpool

import (
	"bytes"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/pkg/list"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

type page struct {
	noCopy     nocopy.NoCopy
	data       []byte
	key        string
	lastAccess *list.DoubleLinkedElement[string]
	dirty      bool
	mu         *sync.RWMutex
}

func emptyPage(key string) *page {
	return &page{
		key:        key,
		mu:         new(sync.RWMutex),
		dirty:      false,
		lastAccess: list.NewDoubleLinkedElement[string](key),
	}
}

func (bp *page) getData() *bytes.Reader {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bytes.NewReader(bp.data)
}

func (bp *page) getSize() int {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return len(bp.data)
}

func (bp *page) isDirty() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.dirty
}

func (bp *page) setDirty() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.dirty = true
}

func (bp *page) clearDirty() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.dirty = false
}

func (bp *page) clear() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.data = nil
	bp.lastAccess = nil
}

func (bp *page) putData(r io.Reader) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	b, err := io.ReadAll(r)
	if err != nil {
		return errors.WithStack(err)
	}

	if c, ok := r.(io.Closer); ok {
		defer c.Close()
	}

	bp.data = b
	return nil
}
