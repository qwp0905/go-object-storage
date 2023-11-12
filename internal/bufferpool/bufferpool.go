package bufferpool

import (
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

const (
	B  = 1
	KB = B * (1 << 10)
	MB = KB * (1 << 10)
	GB = MB * (1 << 10)
	TB = GB * (1 << 10)
	PB = TB * (1 << 10)
)

type BufferPool struct {
	noCopy   nocopy.NoCopy
	fs       *filesystem.FileSystem
	locker   *sync.Mutex
	pages    map[string]*page
	maxSize  int
	accessed *queue
}

func NewBufferPool(maxSize int, fs *filesystem.FileSystem) *BufferPool {
	return &BufferPool{
		fs:       fs,
		locker:   new(sync.Mutex),
		maxSize:  maxSize,
		pages:    make(map[string]*page),
		accessed: newQueue(),
	}
}

func (p *BufferPool) Get(key string) (io.Reader, error) {
	bp, ok := p.pages[key]
	if ok {
		return bp.getData(), nil
	}

	f, err := p.fs.ReadFile(key)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := p.flush(int(info.Size())); err != nil {
		return nil, err
	}

	bp = emptyPage(key)
	if err := bp.putData(f); err != nil {
		return nil, err
	}
	p.allocate(bp)

	return bp.getData(), nil
}

func (p *BufferPool) Put(key string, size int, r io.Reader) error {
	bp, ok := p.pages[key]
	if ok {
		size = size - bp.getSize()
	}

	if err := p.flush(size); err != nil {
		return err
	}

	bp = emptyPage(key)
	bp.setDirty()
	if err := bp.putData(r); err != nil {
		return err
	}
	p.allocate(bp)

	return nil
}

func (p *BufferPool) Delete(key string) error {
	if _, ok := p.pages[key]; ok {
		p.deAllocate(key)
	}

	return p.fs.RemoveFile(key)
}

func (p *BufferPool) FlushAll() error {
	for k, v := range p.pages {
		if !v.isDirty() {
			continue
		}

		if _, err := p.fs.WriteFile(k, v.getData()); err != nil {
			return err
		}
		v.clear()
	}
	return nil
}
