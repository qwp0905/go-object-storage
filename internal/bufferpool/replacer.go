package bufferpool

import (
	"sync"
	"time"

	"github.com/qwp0905/go-object-storage/pkg/logger"
)

func (p *BufferPool) available() int {
	total := 0
	for _, v := range p.pages {
		total += v.getSize()
	}

	return int(p.maxSize) - total
}

func (p *BufferPool) deAllocate(key string) {
	p.locker.Lock()
	defer p.locker.Unlock()
	logger.Infof("%s de allocated", key)
	delete(p.pages, key)
}

func (p *BufferPool) allocate(bp *page) {
	p.locker.Lock()
	defer p.locker.Unlock()
	logger.Infof("%s allocated", bp.key)
	p.pages[bp.key] = bp
}

func (p *BufferPool) victim(size int) error {
	if size <= 0 {
		return nil
	}
	t := time.Now()
	var key string
	for k, v := range p.pages {
		if v.isDirty() {
			s := v.getSize()
			if _, err := p.fs.WriteFile(k, v.getData()); err != nil {
				return err
			}
			p.deAllocate(k)
			return p.victim(size - s)
		}
		if t.Compare(v.lastAccess) == 1 {
			t = v.lastAccess
			key = k
		}
	}

	s := p.pages[key].getSize()
	p.deAllocate(key)
	return p.victim(size - s)
}

func (p *BufferPool) flush(size int) error {
	return p.victim(size - p.available())
}

type queue struct {
	list   []string
	locker *sync.Mutex
}

func newQueue() *queue {
	return &queue{
		list:   []string{},
		locker: new(sync.Mutex),
	}
}

func (q *queue) shift() string {
	q.locker.Lock()
	defer q.locker.Unlock()
	s := q.list[0]
	q.list = q.list[1:]
	return s
}

func (q *queue) push(s string) {
	q.locker.Lock()
	defer q.locker.Unlock()
	q.list = append(q.list, s)
}

func (q *queue) delete(s string) {
	q.locker.Lock()
	defer q.locker.Unlock()
	for i, v := range q.list {
		if v != s {
			continue
		}
		q.list = append(q.list[:i], q.list[i+1:]...)
		return
	}
}
