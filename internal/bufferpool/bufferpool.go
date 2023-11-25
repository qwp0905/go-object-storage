package bufferpool

import (
	"io"
	"os"

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

type BufferPool interface {
	Get(key string) (io.Reader, error)
	Put(key string, size int, r io.Reader) error
	Delete(key string) error
	BeforeDestroy(sig <-chan os.Signal, done chan struct{})
}

type bufferPoolImpl struct {
	noCopy  nocopy.NoCopy
	fs      filesystem.FileSystem
	maxSize int
	table   *pageTable
	retry   int
}

func NewBufferPool(maxSize int, fs filesystem.FileSystem) BufferPool {
	return &bufferPoolImpl{
		fs:      fs,
		maxSize: maxSize,
		table:   newPageTable(),
		retry:   10,
	}
}

func (p *bufferPoolImpl) Get(key string) (io.Reader, error) {
	page, ok := p.table.get(key)
	if ok {
		return page.getData(), nil
	}

	f, size, err := p.fs.ReadFile(key)
	if err != nil {
		return nil, err
	}

	if !p.isAllowed(size) {
		return f, nil
	}
	defer f.Close()

	if err := p.acquire(size); err != nil {
		return nil, err
	}

	page = emptyPage(key)
	if err := page.putData(f); err != nil {
		return nil, err
	}

	p.table.allocate(page)
	return page.getData(), nil
}

func (p *bufferPoolImpl) Put(key string, size int, r io.Reader) error {
	if !p.isAllowed(size) {
		if _, err := p.fs.WriteFile(key, r); err != nil {
			return err
		}
		p.table.deallocate(key)
		return nil
	}

	if err := p.acquire(size); err != nil {
		return err
	}

	page := emptyPage(key)
	page.setDirty()
	if err := page.putData(r); err != nil {
		return err
	}
	p.table.allocate(page)
	go p.lazyWrite(page)
	return nil
}

func (p *bufferPoolImpl) Delete(key string) error {
	defer p.table.deallocate(key)
	return p.fs.RemoveFile(key)
}

func (p *bufferPoolImpl) BeforeDestroy(sig <-chan os.Signal, done chan struct{}) {
	defer close(done)
	<-sig
	if err := p.flushAll(); err != nil {
		panic(err)
	}
	logger.Info("data all flushed")
}

func (p *bufferPoolImpl) flushAll() error {
	for _, page := range p.table.toList() {
		if !page.isDirty() {
			continue
		}
		if _, err := p.fs.WriteFile(page.key, page.getData()); err != nil {
			return err
		}
		page.clearDirty()
	}
	return nil
}

func (p *bufferPoolImpl) lazyWrite(pg *page) {
	for i := 0; i < p.retry; i++ {
		if _, err := p.fs.WriteFile(pg.key, pg.getData()); err == nil {
			pg.clearDirty()
			return
		}
	}
	logger.Warnf("error on writing file %s", pg.key)
}
