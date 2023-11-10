package bufferpool

import (
	"bytes"
	"time"

	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

type buffer struct {
	noCopy     nocopy.NoCopy
	data       []byte
	key        string
	lastAccess time.Time
	pinCount   int
	dirty      bool
}

func (b *buffer) getData() *bytes.Reader {
	b.lastAccess = time.Now()
	return bytes.NewReader(b.data)
}

func (b *buffer) getSize() int {
	return len(b.data)
}

func (b *buffer) isDirty() bool {
	return b.dirty
}
