package bufferpool

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

type buffer struct {
	noCopy     nocopy.NoCopy
	data       []byte
	key        string
	lastAccess time.Time
	pinCount   int
	dirty      bool
	locker     *sync.Mutex
}

func emptyBuffer(key string) *buffer {
	return &buffer{
		key:        key,
		pinCount:   0,
		locker:     new(sync.Mutex),
		dirty:      false,
		lastAccess: time.Now(),
	}
}

func (bp *buffer) getData() *bytes.Reader {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	bp.lastAccess = time.Now()
	bp.pinCount++
	return bytes.NewReader(bp.data)
}

func (bp *buffer) getSize() int {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	return len(bp.data)
}

func (bp *buffer) isDirty() bool {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	return bp.dirty
}

func (bp *buffer) setDirty() {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	bp.dirty = true
}

func (bp *buffer) clear() {
	bp.locker.Lock()
	defer bp.locker.Unlock()
	bp.dirty = false
}

func (bp *buffer) putData(r io.Reader) error {
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
