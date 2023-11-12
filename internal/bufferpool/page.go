package bufferpool

import (
	"bytes"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

type page struct {
	noCopy     nocopy.NoCopy
	data       []byte
	key        string
	lastAccess *element
	dirty      bool
	locker     *sync.Mutex
}

func emptyPage(key string) *page {
	return &page{
		key:        key,
		locker:     new(sync.Mutex),
		dirty:      false,
		lastAccess: &element{value: key},
	}
}

func (bp *page) getData() *bytes.Reader {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	return bytes.NewReader(bp.data)
}

func (bp *page) getSize() int {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	return len(bp.data)
}

func (bp *page) isDirty() bool {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	return bp.dirty
}

func (bp *page) setDirty() {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	bp.dirty = true
}

func (bp *page) clearDirty() {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	bp.dirty = false
}

func (bp *page) clear() {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	bp.data = nil
	bp.lastAccess = nil
}

func (bp *page) putData(r io.Reader) error {
	bp.locker.Lock()
	defer bp.locker.Unlock()
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
