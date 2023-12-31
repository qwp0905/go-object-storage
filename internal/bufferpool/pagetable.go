package bufferpool

import (
	"sync"

	"github.com/qwp0905/go-object-storage/pkg/list"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

type pageTable struct {
	noCopy    nocopy.NoCopy
	accessed  *list.DoubleLinked[string]
	pages     map[string]*page
	mu        *sync.RWMutex
	allocated int
}

func newPageTable() *pageTable {
	return &pageTable{
		accessed:  list.NewDoubleLinked[string](),
		pages:     make(map[string]*page),
		mu:        new(sync.RWMutex),
		allocated: 0,
	}
}

func (t *pageTable) get(key string) (*page, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	page, ok := t.pages[key]
	if ok {
		t.accessed.MoveBack(page.lastAccess)
	}
	return page, ok
}

func (t *pageTable) allocate(p *page) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if page, ok := t.pages[p.key]; ok {
		t.allocated -= page.getSize()
		t.accessed.Remove(page.lastAccess)
		page.clear()
	}

	t.allocated += p.getSize()
	t.pages[p.key] = p
	t.accessed.PushBack(p.lastAccess)
}

func (t *pageTable) deallocate(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	page, ok := t.pages[key]
	if !ok {
		return
	}
	t.allocated -= page.getSize()
	t.accessed.Remove(page.lastAccess)
	delete(t.pages, key)
	page.clear()
}

func (t *pageTable) toList() []*page {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]*page, 0)
	e := t.accessed.Last()
	for e != nil {
		out = append(out, t.pages[e.Value])
		e = e.GetPrev()
	}
	return out
}

func (t *pageTable) oldest() *page {
	t.mu.RLock()
	defer t.mu.RUnlock()
	e := t.accessed.First()
	if e == nil {
		return nil
	}
	return t.pages[e.Value]
}
