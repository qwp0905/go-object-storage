package bufferpool

import (
	"sync"

	"github.com/qwp0905/go-object-storage/pkg/logger"
)

type pageTable struct {
	accessed  *queue
	pages     map[string]*page
	locker    *sync.Mutex
	allocated int
}

func newPageTable() *pageTable {
	return &pageTable{
		accessed:  newQueue(),
		pages:     make(map[string]*page),
		locker:    new(sync.Mutex),
		allocated: 0,
	}
}

func (t *pageTable) get(key string) (*page, bool) {
	t.locker.Lock()
	defer t.locker.Unlock()
	page, ok := t.pages[key]
	if ok {
		t.accessed.moveToBack(page.lastAccess)
	}
	return page, ok
}

func (t *pageTable) allocate(p *page) {
	t.locker.Lock()
	defer t.locker.Unlock()
	if page, ok := t.pages[p.key]; ok {
		t.allocated -= page.getSize()
		t.accessed.remove(page.lastAccess)
		page.clear()
	}

	t.allocated += p.getSize()
	t.pages[p.key] = p
	t.accessed.pushBack(p.lastAccess)
	logger.Infof("%s allocated", p.key)
}

func (t *pageTable) deAllocate(key string) {
	t.locker.Lock()
	defer t.locker.Unlock()
	page, ok := t.pages[key]
	if !ok {
		return
	}
	t.allocated -= page.getSize()
	t.accessed.remove(page.lastAccess)
	delete(t.pages, key)
	page.clear()
	logger.Infof("%s deallocated", key)
}

func (t *pageTable) toList() []*page {
	t.locker.Lock()
	defer t.locker.Unlock()
	out := make([]*page, 0)
	e := t.accessed.last()
	for e != nil {
		out = append(out, t.pages[e.value])
		e = e.getPrev()
	}
	return out
}

func (t *pageTable) oldest() *page {
	t.locker.Lock()
	defer t.locker.Unlock()
	e := t.accessed.first()
	if e == nil {
		return nil
	}
	return t.pages[e.value]
}

type queue struct {
	root element
	len  int
}

func newQueue() *queue {
	q := &queue{len: 0}
	(&q.root).next = &q.root
	(&q.root).prev = &q.root
	(&q.root).list = q
	return q
}

func (q *queue) first() *element {
	if q.len == 0 {
		return nil
	}
	return q.root.next
}

func (q *queue) last() *element {
	if q.len == 0 {
		return nil
	}
	return q.root.prev
}

func (q *queue) pushBack(e *element) {
	q.insert(e, q.root.prev)
}

func (q *queue) moveToBack(e *element) {
	q.move(e, q.root.prev)
}

func (q *queue) insert(e, at *element) {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = q
	q.len++
}

func (l *queue) move(e, at *element) {
	if e == at {
		return
	}
	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
}

func (q *queue) remove(e *element) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	q.len--
}

type element struct {
	list       *queue
	next, prev *element
	value      string
}

func (e *element) getPrev() *element {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}
