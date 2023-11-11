package bufferpool

import (
	"time"

	"github.com/qwp0905/go-object-storage/pkg/logger"
)

func (p *BufferPool) available() int {
	total := 0
	for _, v := range p.buffers {
		total += v.getSize()
	}

	return int(p.maxSize) - total
}

func (p *BufferPool) deAllocate(key string) {
	p.locker.Lock()
	defer p.locker.Unlock()
	logger.Infof("%s de allocated", key)
	delete(p.buffers, key)
}

func (p *BufferPool) allocate(bp *buffer) {
	p.locker.Lock()
	defer p.locker.Unlock()
	logger.Infof("%s allocated", bp.key)
	p.buffers[bp.key] = bp
}

func (p *BufferPool) victim(size int) error {
	if size <= 0 {
		return nil
	}
	t := time.Now()
	var key string
	for k, v := range p.buffers {
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

	s := p.buffers[key].getSize()
	p.deAllocate(key)
	return p.victim(size - s)
}

func (p *BufferPool) flush(size int) error {
	return p.victim(size - p.available())
}
