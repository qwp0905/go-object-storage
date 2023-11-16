package bufferpool

import (
	"io"

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
	noCopy  nocopy.NoCopy
	fs      *filesystem.FileSystem
	maxSize int
	table   *pageTable
	retry   int
}

func NewBufferPool(maxSize int, fs *filesystem.FileSystem) *BufferPool {
	return &BufferPool{
		fs:      fs,
		maxSize: maxSize,
		table:   newPageTable(),
		retry:   10,
	}
}

func (p *BufferPool) Get(key string) (io.Reader, error) {
	page, ok := p.table.get(key)
	if ok {
		return page.getData(), nil
	}

	f, err := p.fs.ReadFile(key)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !p.isAvailable(int(info.Size())) {
		return f, nil
	}
	defer f.Close()

	if err := p.flush(int(info.Size())); err != nil {
		return nil, err
	}

	page = emptyPage(key)
	if err := page.putData(f); err != nil {
		return nil, err
	}

	p.table.allocate(page)
	return page.getData(), nil
}

func (p *BufferPool) Put(key string, size int, r io.Reader) error {
	if !p.isAvailable(size) {
		if _, err := p.fs.WriteFile(key, r); err != nil {
			return err
		}
		p.table.deAllocate(key)
		return nil
	}

	page, ok := p.table.get(key)
	if ok {
		size -= page.getSize()
	}

	if err := p.flush(size); err != nil {
		return err
	}

	page = emptyPage(key)
	page.setDirty()
	if err := page.putData(r); err != nil {
		return err
	}
	p.table.allocate(page)
	go p.lazyWrite(page)
	return nil
}

func (p *BufferPool) Delete(key string) error {
	defer p.table.deAllocate(key)
	return p.fs.RemoveFile(key)
}

func (p *BufferPool) FlushAll() error {
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

func (p *BufferPool) lazyWrite(pg *page) {
	for i := 0; i < p.retry; i++ {
		if _, err := p.fs.WriteFile(pg.key, pg.getData()); err == nil {
			pg.clearDirty()
			logger.Infof("%s written...", pg.key)
			return
		}
	}
	logger.Warnf("error on writing file %s", pg.key)
}
