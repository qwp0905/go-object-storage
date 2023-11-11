package bufferpool

import (
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/pkg/logger"
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
	buffers  map[string]*buffer
	maxSize  int
	interval time.Duration
}

func NewBufferPool(maxSize int, interval time.Duration, fs *filesystem.FileSystem) *BufferPool {
	bp := &BufferPool{
		fs:       fs,
		locker:   new(sync.Mutex),
		maxSize:  maxSize,
		buffers:  make(map[string]*buffer),
		interval: interval,
	}
	go func() {
		for {
			time.Sleep(bp.interval)
			logger.Info("flushing start")
			if err := bp.FlushAll(); err != nil {
				logger.Warnf("flushing at %+v", err)
				continue
			}
			logger.Info("flushing start")
		}
	}()

	return bp
}

func (p *BufferPool) Get(key string) (io.Reader, error) {
	bp, ok := p.buffers[key]
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

	bp = emptyBuffer(key)
	if err := bp.putData(f); err != nil {
		return nil, err
	}
	p.allocate(bp)

	return bp.getData(), nil
}

func (p *BufferPool) Put(key string, size int, r io.Reader) error {
	bp, ok := p.buffers[key]
	if ok {
		size = size - bp.getSize()
	}

	if err := p.flush(size); err != nil {
		return err
	}

	bp = emptyBuffer(key)
	bp.setDirty()
	if err := bp.putData(r); err != nil {
		return err
	}
	p.allocate(bp)

	return nil
}

func (p *BufferPool) Delete(key string) error {
	if _, ok := p.buffers[key]; ok {
		p.deAllocate(key)
	}

	return p.fs.RemoveFile(key)
}

func (p *BufferPool) FlushAll() error {
	for k, v := range p.buffers {
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
