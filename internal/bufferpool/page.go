package bufferpool

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

type page struct {
	noCopy     nocopy.NoCopy
	data       []byte
	key        string
	lastAccess time.Time
	pinCount   int
	dirty      bool
	locker     *sync.Mutex
}

func emptyPage(key string) *page {
	return &page{
		key:        key,
		pinCount:   0,
		locker:     new(sync.Mutex),
		dirty:      false,
		lastAccess: time.Now(),
	}
}

func (bp *page) getData() *bytes.Reader {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	bp.lastAccess = time.Now()
	bp.pinCount++
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

func (bp *page) clear() {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	bp.dirty = false
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
	bp.lastAccess = time.Now()
	return nil
}
