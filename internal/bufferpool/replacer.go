package bufferpool

import (
	"context"
	"io"
	"time"
)

func (p *BufferPool) available() int {
	total := 0
	for _, v := range p.objects {
		total += v.getSize()
	}

	return int(p.maxSize) - total
}

func (p *BufferPool) deleteKey(key string) {
	p.locker.Lock()
	defer p.locker.Unlock()
	delete(p.objects, key)
}

func (p *BufferPool) victim(size int) error {
	if size <= 0 {
		return nil
	}
	t := time.Now()
	var key string
	for k, v := range p.objects {
		if v.isDirty() {
			s := v.getSize()
			if _, err := p.fs.WriteFile(k, v.getData()); err != nil {
				return err
			}
			p.deleteKey(k)
			return p.victim(size - s)
		}
		if t.Compare(v.lastAccess) == 1 {
			t = v.lastAccess
			key = k
		}
	}

	s := p.objects[key].getSize()
	p.deleteKey(key)
	return p.victim(size - s)
}

func (p *BufferPool) newBuffer(ctx context.Context, key string) (*buffer, error) {
	f, err := p.fs.ReadFile(ctx, key)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := int(info.Size())
	available := p.available()
	if available < size {
		if err := p.victim(size - available); err != nil {
			return nil, err
		}
	}

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return &buffer{
		data:       b,
		lastAccess: time.Now(),
		pinCount:   0,
		key:        key,
		dirty:      false,
	}, nil
}
